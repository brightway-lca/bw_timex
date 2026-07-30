import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from heapq import heappop, heappush
from numbers import Number
from typing import Callable

import numpy as np
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.backends.schema import ExchangeDataset as ED
from bw_temporalis import TemporalDistribution, TemporalisLCA, loader_registry

from .utils import (
    get_reference_product_production_amount,
    linear_interpolation_weights,
    nearest_date_weight,
)

if not hasattr(np, "in1d"):
    np.in1d = np.isin

datetime_type = np.dtype("datetime64[s]")
timedelta_type = np.dtype("timedelta64[s]")


@dataclass
class Edge:
    """
    Class for storing a temporal edge with source and target.

    Leaf edges link to a source process which is a leaf in
    our graph traversal (either through cutoff or a filter
    function).

    """

    edge_type: str
    distribution: TemporalDistribution
    leaf: bool
    consumer: int
    producer: int
    td_producer: TemporalDistribution
    td_consumer: TemporalDistribution
    abs_td_producer: TemporalDistribution = None
    abs_td_consumer: TemporalDistribution = None
    temporal_evolution: dict = None
    temporal_evolution_reference: str = "producer"


def extract_temporal_evolution(exc_data: dict) -> dict | None:
    """Read ``temporal_evolution`` data from an exchange's data dict.

    Returns a ``{datetime: factor}`` dict, or ``None`` if the exchange carries
    no temporal evolution. ``temporal_evolution_amounts`` are normalized to
    factors using the exchange's base ``amount``. ``temporal_evolution_factors``
    and ``temporal_evolution_amounts`` are mutually exclusive.
    """
    has_amounts = exc_data.get("temporal_evolution_amounts") is not None
    has_factors = exc_data.get("temporal_evolution_factors") is not None

    if has_amounts and has_factors:
        raise ValueError(
            f"Exchange from {exc_data.get('input')} to "
            f"{exc_data.get('output')} has both "
            f"'temporal_evolution_amounts' and 'temporal_evolution_factors'. "
            f"These are mutually exclusive — use one or the other."
        )

    if has_amounts:
        base_amount = exc_data["amount"]
        if base_amount != 0:
            return {
                k: v / abs(base_amount)
                for k, v in exc_data["temporal_evolution_amounts"].items()
            }
        return None
    if has_factors:
        return exc_data["temporal_evolution_factors"]
    return None


class VariantBackgroundMixin:
    """Shared variant-aware (respective-variant) background-descent machinery.

    The base graph traversal (priority or BFS) runs on ``base_lca``, which only
    contains the *referenced* background variant. When descent continues INTO a
    background process reached at a date that routes to a NON-referenced variant,
    the respective variant's exchanges/amounts/TDs must be read from the bw2data
    activity proxy (``self.bw_node_proxies``) rather than from the
    (referenced-only) technosphere matrix or graph-traversal node objects.

    Both ``EdgeExtractorBFS`` and the priority ``EdgeExtractor`` mix this in.
    They differ in how they reach the first background crossing (matrix BFS vs.
    ``TemporalisLCA`` heap), but the variant split + the proxy-only descent
    through the resulting variant subtree are identical, so they live here.

    Mixers must provide:
    - ``self.bw_node_proxies``: ``{activity_id: bw2data Activity proxy}``.
    - ``self.database_dates_static``, ``self.interpolation_type``,
      ``self.interdatabase_activity_mapping`` (set by ``TimelineBuilder``).
    - ``self.static_activity_indices`` (set; empty under traverse_background).
    - ``self.variant_resolved_producers`` (set, collected during descent).
    - ``self.cutoff`` and a ``self.edge_ff`` edge-filter callable.
    """

    def _variant_shares_for_date(self, producer_date) -> dict:
        """Return ``{db_name: weight}`` interpolation shares for a cohort date.

        Maps the producer's absolute cohort date onto the available static
        background databases, using the same interpolation as the timeline
        builder so leaf and descended routing agree.
        """
        from datetime import datetime as _dt

        dates_static = getattr(self, "database_dates_static", None) or {}
        dates_to_db = {v: k for k, v in dates_static.items() if isinstance(v, _dt)}
        sorted_dates = tuple(sorted(dates_to_db))
        if not sorted_dates:
            return {}

        if isinstance(producer_date, np.datetime64):
            producer_date = producer_date.astype("datetime64[s]").astype(_dt)

        if getattr(self, "interpolation_type", "linear") == "nearest":
            weights = nearest_date_weight(producer_date, sorted_dates)
        else:
            weights = linear_interpolation_weights(producer_date, sorted_dates)
        return {dates_to_db[d]: w for d, w in (weights or {}).items()}

    def _resolve_in_variant(self, node_id: int, db_name: str) -> int:
        """Return the id of ``node_id``'s sibling in database ``db_name``.

        If ``node_id`` already lives in ``db_name`` it is returned unchanged;
        otherwise the sibling is looked up via the interdatabase mapping.
        """
        node = self.bw_node_proxies.get(node_id)
        if node is not None and node["database"] == db_name:
            return node_id
        return self.interdatabase_activity_mapping.find_match(node_id, db_name)

    def _proxy_production_amount(self, activity_id: int) -> float:
        """Production amount of a (possibly non-referenced) variant node, read
        from its bw2data proxy."""
        return get_reference_product_production_amount(
            self.bw_node_proxies[activity_id]
        )

    def _proxy_technosphere_inputs(self, activity_id: int) -> list[int]:
        """Technosphere input product ids of a variant node, read from its proxy.

        ``static_activity_indices`` contains MATRIX INDICES (as used by the
        priority TemporalisLCA engine), not activity ids. When
        ``traverse_background=True``, ``TimelineBuilder`` forces
        ``static_activity_indices = set()`` so this filter is always a no-op on
        the variant-descent path — filtering here by activity id would be
        incorrect whenever the set is non-empty.
        """
        inputs = []
        for exchange in self.bw_node_proxies[activity_id].technosphere():
            input_id = exchange.input.id
            # NOTE: static_activity_indices holds matrix indices, not activity ids;
            # the filter below is only correct when the set is empty (the invariant
            # enforced by EdgeExtractor.__init__ when traverse_background=True).
            if input_id in self.static_activity_indices:
                continue
            inputs.append(input_id)
        return inputs

    def _get_exchange_td_and_type_from_proxy(self, input_id: int, output_id: int):
        """Read the exchange between ``input_id`` and ``output_id`` directly from
        the consuming node's bw2data proxy (``self.bw_node_proxies[output_id]``)
        rather than from the technosphere matrix, because non-referenced variant
        nodes are absent from ``base_lca``'s matrix. Returns the same
        ``(td_or_amount, edge_type, temporal_evolution)`` tuple.
        """
        consumer = self.bw_node_proxies[output_id]
        exc = None
        for candidate in consumer.technosphere():
            if candidate.input.id == input_id:
                exc = candidate
                break
        if exc is None:
            for candidate in consumer.exchanges():
                if (
                    candidate.input.id == input_id
                    and candidate.get("type") != "production"
                ):
                    exc = candidate
                    break
        if exc is None:
            raise ValueError(
                f"No exchange from {input_id} to {output_id} found on proxy."
            )

        edge_type = exc.get("type", "technosphere")
        amount = exc["amount"]
        temporal_evolution = extract_temporal_evolution(exc.as_dict())

        td = exc.get("temporal_distribution")
        if isinstance(td, str) and "__loader__" in td:
            data = json.loads(td)
            td = loader_registry[data["__loader__"]](data)
        if isinstance(td, TemporalDistribution):
            return td * amount, edge_type, temporal_evolution
        return amount, edge_type, temporal_evolution

    def _normalized_production_edge_td_from_proxy(self, process_id: int):
        """Cohort production-edge TD of a variant node, normalized to unit
        weights, read from its proxy. ``None`` when there is no such TD."""
        productions = list(self.bw_node_proxies[process_id].production())
        if not productions:
            return None
        production = productions[0]
        td = production.get("temporal_distribution")
        if isinstance(td, str) and "__loader__" in td:
            data = json.loads(td)
            td = loader_registry[data["__loader__"]](data)
        if not isinstance(td, TemporalDistribution):
            return None
        return td / abs(production["amount"])

    @staticmethod
    def _fold_production_td(base_td, prod_td):
        """Convolve ``base_td`` with a producer's normalized production-edge TD.

        Unlike ``_join_datetime_and_timedelta_distributions`` (which tiles the
        producer amounts and drops the consumer-side amounts), this takes the
        outer product of amounts and the outer sum of dates, so the cohort
        weights carried in ``base_td`` are preserved. Ravel is ``base``-major so
        the result stays index-aligned with a sibling ``base_td`` folded the same
        way. Used to register a descended background producer at its
        production-TD-weighted cohorts.
        """
        date = (
            base_td.date.reshape(-1, 1) + prod_td.date.reshape(1, -1)
        ).ravel()
        amount = (
            base_td.amount.reshape(-1, 1) * prod_td.amount.reshape(1, -1)
        ).ravel()
        return TemporalDistribution(date=date, amount=amount)

    def _producer_process_in_variant(self, product_id: int, db_name: str):
        """Resolve the process producing ``product_id`` within variant
        ``db_name`` from the proxy.

        ``product_id`` was read from a variant proxy, so it already lives in
        ``db_name``. For chimaera nodes the product produces itself; returns
        ``None`` if the product has no producer (a pure leaf).
        """
        node = self.bw_node_proxies.get(product_id)
        if node is None:
            return None
        # Chimaera nodes have at least one production exchange; for a pure leaf
        # (no production edge at all) we should return None — but in practice
        # every node reachable here IS a chimaera and has a self-production edge,
        # so the loop always returns on the first iteration. The trailing
        # ``return product_id`` is therefore unreachable; it is kept as a
        # defensive fallback matching the docstring.
        for _ in node.production():
            return product_id
        return None

    def _is_static_background(self, node_id: int) -> bool:
        """True if ``node_id`` lives in one of the static background databases."""
        dates_static = getattr(self, "database_dates_static", None) or {}
        node = self.bw_node_proxies.get(node_id)
        return node is not None and node["database"] in dates_static

    def _emit_variant_split(
        self,
        *,
        node_id: int,
        producer_process: int,
        edge_type: str,
        temporal_evolution,
        td_producer: TemporalDistribution,
        distribution: TemporalDistribution,
        abs_td_producer: TemporalDistribution,
        abs_td: TemporalDistribution,
        td_parent,
        new_supply: float,
        total_demand: float,
    ) -> list:
        """Perform the variant-aware split at the first background crossing.

        For each producer cohort date, route the edge to its temporally
        appropriate variant database(s), emit one scaled ``Edge`` per variant,
        record the variant id in ``self.variant_resolved_producers``, and descend
        the resulting variant subtree via proxy reads. Returns the list of
        ``Edge`` instances (the split edge + every edge of every variant
        subtree).

        Shared verbatim by both the BFS and priority engines.

        A multi-date consumer (e.g. a foreground ``temporal_distribution``
        feeding a non-leaf background activity, so the consuming background
        process is reached at several cohort dates) is handled by splitting it
        into one single-consumer-date routing per consumer cohort. The join that
        builds ``abs_td_producer`` is consumer-major, so consumer cohort ``i`` is
        the contiguous block ``i*M : (i+1)*M``; each block then reduces to the
        single-consumer-date case, where the relative ``td_producer`` and the
        absolute ``abs_td_producer`` share one index axis.
        """
        n_consumer = len(abs_td)
        m_producer = len(abs_td_producer) // n_consumer
        # ``distribution`` is the (possibly simplify-merged) convolution; it stays
        # consumer-major and aligned to the N*M ``abs_td_producer`` axis unless two
        # convolved dates coincided and merged.
        aligned = len(distribution) == n_consumer * m_producer

        edges = []
        for i in range(n_consumer):
            block = slice(i * m_producer, (i + 1) * m_producer)
            abs_td_producer_i = TemporalDistribution(
                date=abs_td_producer.date[block],
                amount=abs_td_producer.amount[block],
            )
            if aligned:
                distribution_i = TemporalDistribution(
                    date=distribution.date[block],
                    amount=distribution.amount[block],
                )
            else:
                # Rebuild this cohort's absolute distribution from the join dates.
                # These amounts feed only cutoff / heap ordering, never emitted
                # amounts (those come from ``abs_td_producer``), so the rebuild is
                # safe for the inventory result.
                distribution_i = TemporalDistribution(
                    date=abs_td_producer_i.date,
                    amount=abs_td.amount[i] * td_producer.amount,
                )
            abs_td_i = TemporalDistribution(
                date=abs_td.date[i : i + 1],
                amount=abs_td.amount[i : i + 1],
            )
            edges.extend(
                self._emit_variant_split_for_consumer_date(
                    node_id=node_id,
                    producer_process=producer_process,
                    edge_type=edge_type,
                    temporal_evolution=temporal_evolution,
                    td_producer=td_producer,
                    distribution=distribution_i,
                    abs_td_producer=abs_td_producer_i,
                    abs_td=abs_td_i,
                    td_parent=td_parent,
                    new_supply=new_supply,
                    total_demand=total_demand,
                )
            )
        return edges

    def _emit_variant_split_for_consumer_date(
        self,
        *,
        node_id: int,
        producer_process: int,
        edge_type: str,
        temporal_evolution,
        td_producer: TemporalDistribution,
        distribution: TemporalDistribution,
        abs_td_producer: TemporalDistribution,
        abs_td: TemporalDistribution,
        td_parent,
        new_supply: float,
        total_demand: float,
    ) -> list:
        """Variant split for a single consumer cohort date (``len(abs_td) == 1``).

        With one consumer date the relative ``td_producer`` and the absolute
        ``abs_td_producer`` share one index axis, so all three arrays mask by the
        same kept indices and ``extract_edge_data`` can explode the
        consumer/producer dates and amounts consistently.
        """
        # Route each producer cohort date to its variant(s).
        variant_keep: dict[str, dict] = {}
        for date in abs_td_producer.date:
            for db_name, weight in self._variant_shares_for_date(date).items():
                variant_keep.setdefault(db_name, {})[date] = weight

        edges = []
        for db_name, keep in variant_keep.items():
            keep_idx = [
                i for i, d in enumerate(abs_td_producer.date) if keep.get(d)
            ]
            if not keep_idx:
                continue
            weights = np.array([keep[abs_td_producer.date[i]] for i in keep_idx])
            masked_abs_td_producer = TemporalDistribution(
                date=abs_td_producer.date[keep_idx],
                amount=abs_td_producer.amount[keep_idx] * weights,
            )
            masked_distribution = TemporalDistribution(
                date=distribution.date[keep_idx],
                amount=distribution.amount[keep_idx] * weights,
            )
            masked_td_producer = TemporalDistribution(
                date=td_producer.date[keep_idx],
                amount=td_producer.amount[keep_idx] * weights,
            )
            variant_id = self._resolve_in_variant(producer_process, db_name)
            self.variant_resolved_producers.add(variant_id)

            # If this background producer has a production-edge TD, spread it into
            # production-TD-weighted cohorts on the PRODUCER side too, so it is
            # registered at exactly the cohort-years it is later consumed at
            # (bands match -> no KeyError) with weights = exchange x production
            # (conserves). Fold identically into all three arrays to keep them
            # index-aligned for extract_edge_data.
            producer_production_td = self._normalized_production_edge_td_from_proxy(
                variant_id
            )
            if producer_production_td is not None:
                masked_td_producer = self._fold_production_td(
                    masked_td_producer, producer_production_td
                )
                masked_abs_td_producer = self._fold_production_td(
                    masked_abs_td_producer, producer_production_td
                )
                masked_distribution = self._fold_production_td(
                    masked_distribution, producer_production_td
                )

            edges.append(
                Edge(
                    edge_type=edge_type,
                    distribution=masked_distribution,
                    leaf=self.edge_ff(producer_process),
                    consumer=node_id,
                    producer=variant_id,
                    td_producer=masked_td_producer,
                    td_consumer=td_parent,
                    abs_td_producer=masked_abs_td_producer,
                    abs_td_consumer=abs_td,
                    temporal_evolution=temporal_evolution,
                )
            )

            child_td, child_abs_td = masked_distribution, masked_abs_td_producer

            # Cutoff-tracking estimate only — never feeds emitted amounts.
            # Emitted amounts come from the masked distributions above.
            variant_supply = new_supply * sum(keep.values()) / max(len(keep), 1)
            edges.extend(
                self._descend_variant_subtree(
                    node_id=variant_id,
                    td=child_td,
                    td_parent=masked_td_producer,
                    abs_td=child_abs_td,
                    supply=variant_supply,
                    variant_db=db_name,
                    total_demand=total_demand,
                )
            )
        return edges

    def _descend_variant_subtree(
        self,
        *,
        node_id: int,
        td: TemporalDistribution,
        td_parent,
        abs_td: TemporalDistribution,
        supply: float,
        variant_db: str,
        total_demand: float,
    ) -> list:
        """Proxy-only BFS descent through a locked background variant subtree.

        Once inside a variant descent the variant is LOCKED: every descendant
        stays in ``variant_db`` and is read from its bw2data proxy (those nodes
        are absent from ``base_lca``). No re-splitting happens. Variant-resolved
        background producers are recorded so the timeline builder temporalizes
        them (rather than re-interpolating them as temporal markets).

        Engine-agnostic: it does not touch the priority heap or the BFS matrix,
        so both extractors call it identically after their first-crossing split.

        Bounded by ``cutoff`` (per-edge supply threshold) and ``max_calc`` (total
        descended-node budget, shared via ``self._calc_count``); the latter
        caps how deep the background descent runs.
        """
        edges = []
        queue = deque()
        queue.append((node_id, td, td_parent, abs_td, supply))

        while queue:
            self._calc_count += 1
            if self._calc_count > self.max_calc:
                break
            cur_id, cur_td, cur_parent, cur_abs_td, cur_supply = queue.popleft()

            production_amount = self._proxy_production_amount(cur_id)
            input_ids = self._proxy_technosphere_inputs(cur_id)

            for input_id in input_ids:
                leaf = self.edge_ff(input_id)
                td_producer_raw, edge_type, temporal_evolution = (
                    self._get_exchange_td_and_type_from_proxy(input_id, cur_id)
                )

                td_producer = td_producer_raw / abs(production_amount)
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )

                # Zero-amount technosphere exchanges (ubiquitous in real
                # ecoinvent) carry no inventory. Convolving them away would yield
                # an empty TemporalDistribution (temporal_convolution drops zero
                # entries), so skip them entirely, mirroring the matrix path
                # where structural zeros are not edges.
                if not np.any(td_producer.amount):
                    continue

                distribution = (cur_td * td_producer).simplify()
                abs_td_producer = _join_datetime_and_timedelta_distributions(
                    td_producer, cur_abs_td
                )

                if isinstance(td_producer_raw, TemporalDistribution):
                    edge_supply = abs(td_producer_raw.amount.sum())
                else:
                    edge_supply = abs(td_producer_raw)
                new_supply = cur_supply * edge_supply / abs(production_amount)

                producer_process = self._producer_process_in_variant(
                    input_id, variant_db
                )
                will_descend = (
                    not leaf
                    and new_supply >= self.cutoff * total_demand
                    and producer_process is not None
                )

                # Already routed to its real variant database -> temporalize it.
                if self._is_static_background(input_id):
                    self.variant_resolved_producers.add(input_id)

                # Fold this producer's own production-edge TD into the producer
                # side so it is registered at the same production-TD-weighted
                # cohorts it is consumed at (bands match -> no KeyError; weights
                # = exchange x production -> conserves). Fold identically into
                # td_producer/abs_td_producer/distribution to keep them aligned.
                producer_production_td = None
                if producer_process is not None:
                    producer_production_td = (
                        self._normalized_production_edge_td_from_proxy(producer_process)
                    )
                if producer_production_td is not None:
                    td_producer = self._fold_production_td(
                        td_producer, producer_production_td
                    )
                    abs_td_producer = self._fold_production_td(
                        abs_td_producer, producer_production_td
                    )
                    distribution = self._fold_production_td(
                        distribution, producer_production_td
                    )

                edges.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=cur_id,
                        producer=input_id,
                        td_producer=td_producer,
                        td_consumer=cur_parent,
                        abs_td_producer=abs_td_producer,
                        abs_td_consumer=cur_abs_td,
                        temporal_evolution=temporal_evolution,
                    )
                )

                if not will_descend:
                    continue

                queue.append(
                    (producer_process, distribution, td_producer, abs_td_producer, new_supply)
                )
        return edges


class EdgeExtractor(VariantBackgroundMixin, TemporalisLCA):
    """
    Child class of TemporalisLCA that traverses the supply chain just as the parent class but can create a timeline of edges, in addition timeline of flows or nodes.

    The edge timeline is then used to match the timestamp of edges to that of background
    databases and to replace these edges with edges from these background databases
    using Brightway Datapackages.
    """

    def __init__(self, *args, edge_filter_function: Callable = None,
        traverse_background: bool = False, **kwargs) -> None:
        """
        Initialize the EdgeExtractor class and traverses the supply chain using
        functions of the parent class TemporalisLCA.

        Parameters
        ----------
        *args : Variable length argument list
        edge_filter_function : Callable, optional
            A callable that filters edges. If not provided, a function that always
            returns False is used.
        traverse_background : bool, optional
            Flag indicating whether to traverse background databases. Default is False.
        **kwargs : Arbitrary keyword arguments

        Returns
        -------
        None
            stores the output of the TemporalisLCA graph traversal (incl. relation of edges (edge_mapping) and nodes (node_mapping) in the instance of the class.

        """
        # ``cutoff``/``static_activity_indices`` are consumed by TemporalisLCA but
        # not stored on it; the shared variant-descent mixin needs both, so keep
        # local copies before delegating.
        self.cutoff = kwargs.get("cutoff", 5e-4)
        # max_calc bounds the background proxy descent too (the shared mixin's
        # _descend_variant_subtree). TemporalisLCA consumes max_calc for its own
        # heap traversal but does not store it, so keep a local copy + counter.
        self.max_calc = kwargs.get("max_calc", 2000)
        self._calc_count = 0
        self.static_activity_indices = kwargs.get("static_activity_indices") or set()
        # INVARIANT: static_activity_indices holds TemporalisLCA MATRIX INDICES,
        # not activity ids. _proxy_technosphere_inputs filters by activity id, so
        # the two id-spaces must not be mixed. TimelineBuilder enforces the empty-
        # set precondition when traverse_background=True; assert it here so any
        # future caller that bypasses TimelineBuilder fails loudly.
        assert not (self.static_activity_indices and traverse_background), (
            "static_activity_indices must be empty when traverse_background=True "
            "(index-space differs across engines)"
        )
        super().__init__(*args, **kwargs)  # use __init__ of TemporalisLCA
        self.traverse_background = traverse_background
        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False
        # Producer node ids variant-resolved during a variant-aware background
        # descent (shared with EdgeExtractorBFS via VariantBackgroundMixin). These
        # are temporalized by the timeline builder, not treated as temporal-market
        # leaves. ``bw_node_proxies`` (bw2data Activity proxies, keyed by activity
        # id) is set by the TimelineBuilder; the mixin's proxy reads use it because
        # the priority engine's own ``self.nodes`` are graph-traversal Node objects
        # keyed by ``unique_id`` and contain only the referenced variant.
        self.variant_resolved_producers: set[int] = set()
        self.bw_node_proxies: dict = {}

    def build_edge_timeline(self) -> list:
        """
        Creates a timeline of the edges from the output of the graph traversal.
        Starting from the edges of the functional unit node, it goes through
        each node using a heap, selecting the node with the highest impact first.
        It, then, propagates the TemporalDistributions of the edges from node to
        node through time using convolution-operators. It stops in case the current edge
        is known to have no temporal distribution (=leaf) (e.g. part of background database).

        Parameters
        ----------
        None

        Returns
        -------
        list
            A list of Edge instances with timestamps and amounts, and ids of its producing
            and consuming node.

        """

        heap = []
        timeline = []
        # Total demand drives the cutoff comparison in the shared variant-descent
        # mixin (same semantics as the BFS engine).
        total_demand = float(np.abs(self.lca_object.demand_array).sum())

        for edge in self.edge_mapping[
            self.unique_id
        ]:  # starting at the edges of the functional unit
            node = self.nodes[edge.producer_unique_id]
            td_producer = edge.amount
            initial_distribution = self.t0 * edge.amount
            abs_td_producer = self.t0
            abs_td_consumer = None

            row_id = self.lca_object.dicts.product.reversed[edge.product_index]
            col_id = node.activity_datapackage_id
            exchange = self.get_technosphere_exchange(input_id=row_id, output_id=col_id)

            # In the explicit process/product paradigm, the demanded product is produced
            # by an off-diagonal production edge. A TD on that edge distributes the
            # process invocation itself before the process inputs are traversed.
            if (
                row_id != col_id
                and hasattr(exchange, "data")
                and exchange.data.get("type") == "production"
            ):
                production_amount = abs(
                    get_reference_product_production_amount(
                        col_id, reference_product=row_id, lca=self.lca_object
                    )
                )
                td_producer = (
                    self._exchange_value(
                        exchange=exchange,
                        row_id=row_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / production_amount
                    * edge.amount
                )
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )
                initial_distribution = (self.t0 * td_producer).simplify()
                abs_td_producer = self.join_datetime_and_timedelta_distributions(
                    td_producer, self.t0
                )
                abs_td_consumer = self.t0

            heappush(
                heap,
                (
                    1 / node.cumulative_score,
                    initial_distribution,
                    self.t0,
                    abs_td_producer,
                    node,
                ),
            )

            timeline.append(
                Edge(
                    edge_type="production",  # FU exchange always type production (?)
                    distribution=initial_distribution,
                    leaf=False,
                    consumer=self.unique_id,
                    producer=node.activity_datapackage_id,
                    td_producer=td_producer,
                    td_consumer=self.t0,
                    abs_td_producer=abs_td_producer,
                    abs_td_consumer=abs_td_consumer,
                )
            )

        while heap:
            _, td, td_parent, abs_td, node = heappop(heap)

            for edge in self.edge_mapping[node.unique_id]:
                row_id = self.nodes[edge.producer_unique_id].activity_datapackage_id
                col_id = node.activity_datapackage_id
                product_id = self.lca_object.dicts.product.reversed[edge.product_index]
                exchange = self.get_technosphere_exchange(
                    input_id=product_id,
                    output_id=col_id,
                )
                if not hasattr(exchange, "data"):
                    exchange = self.get_technosphere_exchange(
                        input_id=row_id,
                        output_id=col_id,
                    )
                    product_id = row_id

                edge_type = exchange.data[
                    "type"
                ]  # can be technosphere, substitution, production or other string

                # Extract temporal evolution data from exchange
                temporal_evolution = extract_temporal_evolution(exchange.data)
                temporal_evolution_reference = exchange.data.get(
                    "temporal_evolution_reference", "producer"
                )

                td_producer = (  # td_producer is the TemporalDistribution of the edge
                    self._exchange_value(
                        exchange=exchange,
                        row_id=product_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / abs(
                        get_reference_product_production_amount(
                            node.activity_datapackage_id, lca=self.lca_object
                        )
                    )
                )
                producer = self.nodes[edge.producer_unique_id]
                leaf = self.edge_ff(row_id)

                # If an edge does not have a TD, give it a td with timedelta=0 and the amount= 'edge value'
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )

                if product_id != row_id:
                    production_exchange = self.get_technosphere_exchange(
                        input_id=product_id,
                        output_id=row_id,
                    )
                    if (
                        hasattr(production_exchange, "data")
                        and production_exchange.data.get("type") == "production"
                    ):
                        production_amount = abs(
                            get_reference_product_production_amount(
                                row_id,
                                reference_product=product_id,
                                lca=self.lca_object,
                            )
                        )
                        production_td = (
                            self._exchange_value(
                                exchange=production_exchange,
                                row_id=product_id,
                                col_id=row_id,
                                matrix_label="technosphere_matrix",
                            )
                            / production_amount
                        )
                        if isinstance(production_td, Number):
                            production_td = TemporalDistribution(
                                date=np.array([0], dtype="timedelta64[Y]"),
                                amount=np.array([production_td]),
                            )
                        td_producer = (td_producer * production_td).simplify()

                distribution = (
                    td * td_producer
                ).simplify()  # convolution-multiplication of TemporalDistribution of consuming node (td) and consumed edge (edge) gives TD of producing node

                abs_td_producer = self.join_datetime_and_timedelta_distributions(
                    td_producer, abs_td
                )

                # Variant-aware split at the FIRST crossing from the referenced
                # traversal into the background. Mirrors EdgeExtractorBFS exactly
                # (consumer is static background, producer is static background
                # WITH technosphere inputs); leaves / market frontier fall through
                # to the existing single-edge + heap path unchanged.
                consumer_id = node.activity_datapackage_id
                producer_id = producer.activity_datapackage_id
                variant_split = (
                    not leaf
                    and self.traverse_background
                    and getattr(self, "interdatabase_activity_mapping", None)
                    is not None
                    and self.bw_node_proxies
                    and self._is_static_background(consumer_id)
                    and self._is_static_background(producer_id)
                    and bool(self._proxy_technosphere_inputs(producer_id))
                )

                if variant_split:
                    if isinstance(td_producer, TemporalDistribution):
                        new_supply = abs(td_producer.amount.sum())
                    else:
                        new_supply = abs(td_producer)
                    # No per-edge cutoff term here: the priority heap loop has no
                    # per-edge cutoff at this layer. Cutoff is enforced inside
                    # ``_descend_variant_subtree`` where the supply is compared
                    # against ``cutoff * total_demand`` per child edge.
                    timeline.extend(
                        self._emit_variant_split(
                            node_id=consumer_id,
                            producer_process=producer_id,
                            edge_type=edge_type,
                            temporal_evolution=temporal_evolution,
                            td_producer=td_producer,
                            distribution=distribution,
                            abs_td_producer=abs_td_producer,
                            abs_td=abs_td,
                            td_parent=td_parent,
                            new_supply=new_supply,
                            total_demand=total_demand,
                        )
                    )
                    continue

                timeline.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=consumer_id,
                        producer=producer_id,
                        td_producer=td_producer,
                        td_consumer=td_parent,
                        abs_td_producer=abs_td_producer,
                        abs_td_consumer=abs_td,
                        temporal_evolution=temporal_evolution,
                        temporal_evolution_reference=temporal_evolution_reference,
                    )
                )
                if not leaf:
                    heappush(
                        heap,
                        (
                            1 / node.cumulative_score,
                            distribution,
                            td_producer,
                            abs_td_producer,
                            producer,
                        ),
                    )
        return timeline

    def join_datetime_and_timedelta_distributions(
        self, td_producer: TemporalDistribution, td_consumer: TemporalDistribution
    ) -> TemporalDistribution:
        """
        Joins a relative or absolute TemporalDistribution (td_producer) with an
        absolute TemporalDistribution (td_consumer) to create a new
        TemporalDistribution.

        If the producer does not have a TemporalDistribution, the consumer's
        TemporalDistribution is returned to continue the timeline.
        If both the producer and consumer have TemporalDistributions, they are joined together.

        Parameters
        ----------
        td_producer : TemporalDistribution
            TemporalDistribution of the producer. Expected to be a timedelta or
            datetime TemporalDistribution.
        td_consumer : TemporalDistribution
            TemporalDistribution of the consumer. Expected to be a datetime
            TemporalDistribution.

        Returns
        -------
        TemporalDistribution
            A new TemporalDistribution that is the result of joining the producer
            and consumer TemporalDistributions.

        Raises
        ------
        ValueError
            If the dtype of `td_consumer.date` is not `datetime64[s]` or the dtype
            of `td_producer.date` is neither `datetime64[s]` nor `timedelta64[s]`.

        """
        return _join_datetime_and_timedelta_distributions(td_producer, td_consumer)


class EdgeExtractorBFS(VariantBackgroundMixin):
    """
    Breadth-First-Search (BFS) graph traversal for extracting temporal edges from
    the supply chain.

    Unlike EdgeExtractor (which inherits from TemporalisLCA and uses priority-first
    traversal with per-subgraph LCA calculations), this class works directly with
    the technosphere matrix from a bw2calc LCA object and traverses using BFS.
    This avoids the overhead of computing individual subgraph LCAs for priority
    ordering.

    Returns the same list[Edge] format as EdgeExtractor, so all downstream code
    (TimelineBuilder, MatrixModifier, etc.) works unchanged.
    """

    def __init__(
        self,
        lca_object,
        starting_datetime: datetime | str = "now",
        edge_filter_function: Callable = None,
        cutoff: float = 1e-9,
        static_activity_indices: set[int] | None = None,
        nodes: dict | None = None,
        traverse_background: bool = False,
        max_calc: int = 1_000_000,
    ) -> None:
        self.lca_object = lca_object
        self.edge_ff = edge_filter_function if edge_filter_function else lambda x: False
        self.cutoff = cutoff
        self.static_activity_indices = static_activity_indices or set()
        # {node_id: bw2data Activity proxy} reused from TimexLCA, so production /
        # technosphere exchanges are read without re-fetching nodes.
        self.nodes = nodes or {}
        # The mixin reads bw2data Activity proxies through ``bw_node_proxies``.
        # For BFS these are the same node proxies the matrix traversal uses.
        self.bw_node_proxies = self.nodes
        self.traverse_background = traverse_background
        self.max_calc = max_calc
        self._calc_count = 0
        self._production_exchange_cache = {}
        # Producer node ids that were variant-resolved during a variant-aware
        # background descent. These are already routed to their respective
        # (real) variant database, so the timeline builder must temporalize them
        # rather than treat them as temporal-market leaves.
        self.variant_resolved_producers: set[int] = set()

        if self.traverse_background:
            # Background is no longer a hard stop; rely on cutoff / max_calc.
            # A user-supplied edge_filter_function is still respected.
            if edge_filter_function is None:
                self.edge_ff = lambda x: False

        if isinstance(starting_datetime, str):
            if starting_datetime == "now":
                starting_datetime = datetime.now()
            else:
                starting_datetime = datetime.fromisoformat(starting_datetime)

        self.t0 = TemporalDistribution(
            np.array([np.datetime64(starting_datetime)]),
            np.array([1]),
        )

        self.tech_matrix_csc = self.lca_object.technosphere_matrix.tocsc()
        self._ad_cache = {}

    def _get_activity_dataset(self, activity_id: int) -> AD:
        if activity_id not in self._ad_cache:
            self._ad_cache[activity_id] = AD.get(AD.id == activity_id)
        return self._ad_cache[activity_id]

    def _get_exchange(self, input_id: int, output_id: int):
        """Look up exchange between two activities. Returns ExchangeDataset or None."""
        inp = self._get_activity_dataset(input_id)
        outp = self._get_activity_dataset(output_id)
        exchanges = list(
            ED.select().where(
                ED.input_code == inp.code,
                ED.input_database == inp.database,
                ED.output_code == outp.code,
                ED.output_database == outp.database,
            )
        )
        if len(exchanges) == 1:
            return exchanges[0]
        elif len(exchanges) > 1:
            raise ValueError(
                f"Found {len(exchanges)} exchanges between {input_id} and {output_id}"
            )
        return None

    def _get_exchange_td_and_type(self, input_id: int, output_id: int):
        """
        Get temporal distribution, edge type and temporal evolution for an exchange.

        Returns (td_or_amount, edge_type, temporal_evolution) where td_or_amount
        is either a TemporalDistribution or a float (the signed matrix value),
        and temporal_evolution is a {datetime: factor} dict or None.
        """
        exchange = self._get_exchange(input_id, output_id)

        row_idx = self.lca_object.dicts.product[input_id]
        col_idx = self.lca_object.dicts.activity[output_id]
        matrix_value = self.tech_matrix_csc[row_idx, col_idx]

        if exchange is None:
            sign = 1 if input_id == output_id else -1
            return sign * matrix_value, "technosphere", None

        edge_type = exchange.data["type"]
        sign = -1 if edge_type in ("generic consumption", "technosphere") else 1
        amount = sign * matrix_value

        temporal_evolution = extract_temporal_evolution(exchange.data)

        td = exchange.data.get("temporal_distribution")
        if td is not None:
            if isinstance(td, str) and "__loader__" in td:
                data = json.loads(td)
                td = loader_registry[data["__loader__"]](data)
            if isinstance(td, TemporalDistribution):
                return td * amount, edge_type, temporal_evolution

        return amount, edge_type, temporal_evolution

    def _get_production_amount(self, activity_id: int) -> float:
        """Get the reference product production amount."""
        activity = self.lca_object.dicts.activity.reversed[
            self.lca_object.dicts.activity[activity_id]
        ]
        return get_reference_product_production_amount(activity)

    def _get_production_exchange(self, process_id: int):
        """Return the single ``production`` output exchange of ``process_id``,
        or ``None`` if it has none. Read from the reused node proxy (no extra
        node fetch) and memoized per process.

        Identifying the output via the exchange type (rather than the matrix
        sign) matters because a negative-amount technosphere input also produces
        a positive matrix entry, so sign alone cannot tell them apart.
        """
        if process_id in self._production_exchange_cache:
            return self._production_exchange_cache[process_id]

        productions = list(self.nodes[process_id].production())
        if len(productions) > 1:
            raise ValueError(
                f"Process {process_id} has multiple production outputs "
                f"(co-production); not supported with graph_traversal='bfs'."
            )
        exchange = productions[0] if productions else None
        self._production_exchange_cache[process_id] = exchange
        return exchange

    def _get_technosphere_inputs(self, activity_id: int) -> list[int]:
        """Get the input product IDs consumed by a process.

        Read straight from the node's technosphere exchanges, so the production
        output is naturally excluded and avoided-burden inputs (which the matrix
        stores with a flipped sign) are kept.
        """
        inputs = []
        for exchange in self.nodes[activity_id].technosphere():
            input_id = exchange.input.id
            if input_id in self.static_activity_indices:
                continue
            inputs.append(input_id)
        return inputs

    def _get_producer_process(self, product_id: int) -> int | None:
        """Return the process that produces ``product_id``.

        For a chimaera node this is the node itself; for an explicit ``product``
        it is the separate ``process`` whose ``production`` edge feeds the
        product's row. Returns ``None`` if the product has no producer (a leaf).
        """
        try:  # chimaera: the product is also its own producing activity
            self.lca_object.dicts.activity[product_id]
            return product_id
        except KeyError:
            pass

        product_idx = self.lca_object.dicts.product.get(product_id)
        if product_idx is None:
            return None
        # Candidate producer columns come from the product's matrix row (cheap);
        # the production one is confirmed against the node's production exchange.
        row = self.tech_matrix_csc.getrow(product_idx).tocoo()
        producers = []
        for col_idx in row.col:
            process_id = self.lca_object.dicts.activity.reversed[col_idx]
            production = self._get_production_exchange(process_id)
            if production is not None and production.input.id == product_id:
                producers.append(process_id)
        if not producers:
            return None
        if len(producers) > 1:
            raise ValueError(
                f"Product {product_id} has multiple producing processes "
                f"({producers}); ambiguous-producer resolution is not supported "
                f"with graph_traversal='bfs'."
            )
        return producers[0]

    def _get_normalized_production_edge_td(self, process_id: int):
        """Return the cohort ``TemporalDistribution`` on a process's production
        output edge, normalized to unit weights, or ``None`` if there is none.

        Chimaera self-production edges normally carry no TD, so this returns
        ``None`` and chimaera traversal is unaffected.
        """
        production = self._get_production_exchange(process_id)
        if production is None:
            return None
        td = production.get("temporal_distribution")
        if isinstance(td, str) and "__loader__" in td:
            data = json.loads(td)
            td = loader_registry[data["__loader__"]](data)
        if not isinstance(td, TemporalDistribution):
            return None
        # The TD weights normalize to 1; divide by the production amount (alpha)
        # so the alpha scaling is applied only once, on the input side.
        return td / abs(production["amount"])

    # Variant-aware (respective-variant) reads + proxy descent are inherited
    # from ``VariantBackgroundMixin`` and shared with the priority extractor.

    def build_edge_timeline(self) -> list:
        """
        Breadth-First-Search (BFS) traversal of the supply chain, extracting
        temporal edges.

        Returns a list of Edge instances compatible with the existing
        EdgeExtractor output format.
        """
        timeline = []
        queue = deque()

        demand_array = self.lca_object.demand_array
        fu_activity_ids = [
            self.lca_object.dicts.activity.reversed[idx]
            for idx in demand_array.nonzero()[0]
        ]

        total_demand = float(np.abs(demand_array).sum())

        for fu_id in fu_activity_ids:
            fu_amount = demand_array[self.lca_object.dicts.activity[fu_id]]
            td = self.t0 * fu_amount

            # Spread the functional unit over the producing process's cohort TD
            # (the explicit process -> product output edge). Chimaera nodes have
            # no such TD, so the original behaviour is preserved exactly.
            production_td = self._get_normalized_production_edge_td(fu_id)
            if production_td is None:
                timeline.append(
                    Edge(
                        edge_type="production",
                        distribution=td,
                        leaf=False,
                        consumer=-1,
                        producer=fu_id,
                        td_producer=fu_amount,
                        td_consumer=self.t0,
                        abs_td_producer=self.t0,
                    )
                )
                queue.append((fu_id, td, self.t0, self.t0, abs(fu_amount)))
            else:
                # Cohort-spread the FU so the producing process is registered at
                # every cohort time (each cohort gets its own time-mapped column).
                seed_td = (td * production_td).simplify()
                seed_abs_td = _join_datetime_and_timedelta_distributions(
                    production_td, self.t0
                )
                timeline.append(
                    Edge(
                        edge_type="production",
                        distribution=seed_td,
                        leaf=False,
                        consumer=-1,
                        producer=fu_id,
                        td_producer=seed_td,
                        td_consumer=self.t0,
                        abs_td_producer=seed_abs_td,
                        abs_td_consumer=self.t0,
                    )
                )
                queue.append(
                    (fu_id, seed_td, self.t0, seed_abs_td, abs(fu_amount))
                )

        while queue:
            node_id, td, td_parent, abs_td, supply = queue.popleft()

            self._calc_count += 1
            if self._calc_count > self.max_calc:
                break

            # Reaching a non-referenced background variant is handled entirely by
            # ``_emit_variant_split`` (shared with the priority engine), which owns
            # its own proxy descent. The matrix BFS queue therefore only ever holds
            # ``base_lca`` (referenced-variant) nodes, read from the matrix.
            production_amount = self._get_production_amount(node_id)
            input_ids = self._get_technosphere_inputs(node_id)

            for input_id in input_ids:
                leaf = self.edge_ff(input_id)

                td_producer_raw, edge_type, temporal_evolution = (
                    self._get_exchange_td_and_type(input_id, node_id)
                )

                td_producer = td_producer_raw / abs(production_amount)

                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )

                distribution = (td * td_producer).simplify()

                abs_td_producer = _join_datetime_and_timedelta_distributions(
                    td_producer, abs_td
                )

                if isinstance(td_producer_raw, TemporalDistribution):
                    edge_supply = abs(td_producer_raw.amount.sum())
                else:
                    edge_supply = abs(td_producer_raw)
                new_supply = supply * edge_supply / abs(production_amount)

                # Resolve the producing process for this input product.
                # For chimaera nodes the producer is the product itself.
                producer_process = self._get_producer_process(input_id)

                will_descend = (
                    not leaf
                    and new_supply >= self.cutoff * total_demand
                    and producer_process is not None
                )

                # Variant-aware split: when this input's producer is a static
                # background process that we will descend into, emit one edge per
                # temporally-appropriate variant (reading the RESPECTIVE variant's
                # exchanges on descent) so that each variant node is both a
                # producer and a consumer in the timeline and is rebuilt from its
                # own (real) database. Leaves and foreground producers fall
                # through to the original single-edge path unchanged.
                # The variant split happens only on the FIRST crossing from the
                # referenced matrix traversal into the background; from there the
                # shared ``_emit_variant_split`` runs the locked-variant subtree
                # via proxy reads, so the matrix queue never re-enters proxy mode.
                # Variant-split only matters when we will actually descend THROUGH
                # a background process into its own (technosphere) inputs — that is
                # where the respective variant's exchanges/amounts differ. A
                # background producer with no technosphere inputs is a true market
                # leaf (handled by the existing leaf machinery), so it is left
                # untouched to preserve the Task 4 behaviour exactly.
                variant_split = (
                    will_descend
                    and self.traverse_background
                    and getattr(self, "interdatabase_activity_mapping", None)
                    is not None
                    and self._is_static_background(node_id)
                    and self._is_static_background(producer_process)
                    and bool(self._proxy_technosphere_inputs(producer_process))
                )

                if not variant_split:
                    timeline.append(
                        Edge(
                            edge_type=edge_type,
                            distribution=distribution,
                            leaf=leaf,
                            consumer=node_id,
                            producer=input_id,
                            td_producer=td_producer,
                            td_consumer=td_parent,
                            abs_td_producer=abs_td_producer,
                            abs_td_consumer=abs_td,
                            temporal_evolution=temporal_evolution,
                        )
                    )

                    if not will_descend:
                        continue

                    child_td, child_abs_td = distribution, abs_td_producer
                    producer_production_td = self._get_normalized_production_edge_td(
                        producer_process
                    )
                    if producer_production_td is not None:
                        child_td = (distribution * producer_production_td).simplify()
                        child_abs_td = _join_datetime_and_timedelta_distributions(
                            producer_production_td, abs_td_producer
                        )

                    queue.append(
                        (
                            producer_process,
                            child_td,
                            td_producer,
                            child_abs_td,
                            new_supply,
                        )
                    )
                    continue

                # --- variant split path (shared with the priority engine) ---
                # Route each producer cohort date to its temporally appropriate
                # variant(s), emit the scaled edges, and descend each variant
                # subtree via proxy reads. ``_emit_variant_split`` owns the whole
                # variant subtree, so the matrix queue never re-enters proxy mode.
                timeline.extend(
                    self._emit_variant_split(
                        node_id=node_id,
                        producer_process=producer_process,
                        edge_type=edge_type,
                        temporal_evolution=temporal_evolution,
                        td_producer=td_producer,
                        distribution=distribution,
                        abs_td_producer=abs_td_producer,
                        abs_td=abs_td,
                        td_parent=td_parent,
                        new_supply=new_supply,
                        total_demand=total_demand,
                    )
                )

        return timeline


def _join_datetime_and_timedelta_distributions(
    td_producer: TemporalDistribution,
    td_consumer: TemporalDistribution,
) -> TemporalDistribution:
    """
    Join a relative or absolute TemporalDistribution (td_producer) with an
    absolute TemporalDistribution (td_consumer).

    If the producer does not have a TemporalDistribution, the consumer's
    TemporalDistribution is returned. If both have TDs, they are joined
    via broadcasting.
    """
    if isinstance(td_consumer, TemporalDistribution) and isinstance(
        td_producer, Number
    ):
        return td_consumer

    if isinstance(td_producer, TemporalDistribution) and isinstance(
        td_consumer, TemporalDistribution
    ):
        if not (td_consumer.date.dtype == datetime_type):
            raise ValueError(
                f"`td_consumer.date` must have dtype `datetime64[s]`, "
                f"but got `{td_consumer.date.dtype}`"
            )
        if td_producer.date.dtype == timedelta_type:
            date = (
                td_consumer.date.reshape((-1, 1))
                + td_producer.date.reshape((1, -1))
            ).ravel()
        elif td_producer.date.dtype == datetime_type:
            date = np.array(len(td_consumer) * [td_producer.date]).ravel()
        else:
            raise ValueError(
                f"`td_producer.date` must have dtype `datetime64[s]` "
                f"or `timedelta64[s]`, "
                f"but got `{td_producer.date.dtype}`"
            )
        amount = np.array(len(td_consumer) * [td_producer.amount]).ravel()
        return TemporalDistribution(date, amount)
    else:
        raise ValueError(
            f"Can't join TemporalDistribution and something else: "
            f"Trying with {type(td_consumer)} and {type(td_producer)}"
        )
