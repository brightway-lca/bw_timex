from bisect import bisect_left
from datetime import datetime, timedelta
from typing import Callable, KeysView

import bw2data as bd
import numpy as np
import pandas as pd
from bw2calc import LCA
from bw2data.configuration import labels
from loguru import logger

from .edge_extractor import Edge, EdgeExtractor, EdgeExtractorBFS
from .errors import UnmappedDatabaseError
from .utils import (
    convert_date_string_to_datetime,
    extract_date_as_integer,
    extract_date_as_string,
    linear_interpolation_weights,
    nearest_date_weight,
    round_datetime,
)

class TimelineBuilder:
    """
    Class for building a process timeline based on the temporal distributions of their exchanges.

    First, the `EdgeExtractor` does a priority-first graph traversal and extracts a timeline of
    exchanges (edge_timeline) with temporal information. Identical edges within temporal grouping
    (e.g. year, month, day, hour) are then grouped and the amount of exchanges is summed up.
    """

    def __init__(
        self,
        base_lca: LCA,
        starting_datetime: datetime,
        edge_filter_function: Callable,
        database_dates: dict,
        database_dates_static: dict,
        activity_time_mapping: dict,
        node_collections: dict,
        nodes: dict,
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
        cutoff: float = 1e-9,
        max_calc: int = 2000,
        graph_traversal: str = "priority",
        traverse_background: bool = False,
        interdatabase_activity_mapping=None,
        *args,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        base_lca: LCA
            A static LCA object.
        starting_datetime: datetime | str, optional
            Point in time when the demand occurs.
        edge_filter_function: Callable
            A callable that filters edges. If not provided, a function that always returns False is used.
        database_dates: dict
            A dictionary mapping databases to dates.
        database_dates_static: dict
            same as database_dates, but excluding the "dynamic" foreground databases.
        activity_time_mapping: dict
            A dictionary to map processes to specific times.
        node_collections: dict
            A dictionary collecting useful subsets of node ids.
        nodes: dict
            A dictionary {node_id: 'bw2data.backends.proxies.Activity'} for all nodes.
        temporal_grouping: str, optional
            The temporal grouping to be used. Default is "year".
        interpolation_type: str, optional
            The type of interpolation to be used to select the background databases. Default is "linear".
        cutoff:
            The cutoff value for the graph traversal. Default is 1e-9.
        max_calc:
            The maximum number of calculations to be performed by the graph traversal. Default is 2000.
        args:   Variable length argument list
            Keyword arguments passed to the EdgeExtractor which inherits from TemporalisLCA. Here, things like the further settings for graph traversal can be set. For details, see bw_temporalis.TemporalisLCA.
        kwargs: Arbitrary keyword arguments
            Keyword arguments passed to the EdgeExtractor which inherits from TemporalisLCA.
        """
        self.base_lca = base_lca
        self.starting_datetime = starting_datetime
        self.edge_filter_function = edge_filter_function
        self.database_dates = database_dates
        self.database_dates_static = database_dates_static
        self.activity_time_mapping = activity_time_mapping
        self.node_collections = node_collections
        self.nodes = nodes
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type
        self.cutoff = cutoff
        self.max_calc = max_calc
        self.traverse_background = traverse_background
        self.interdatabase_activity_mapping = interdatabase_activity_mapping
        self._logged_reference_date_below_range = False
        self._logged_reference_date_above_range = False

        # Finding indices of activities from the connected background databases that are known to be static, i.e. have no temporal distributions connecting to them.
        # These will be be skipped in the graph traversal.
        if self.traverse_background:
            static_background_activity_ids = set()
        else:
            static_background_activity_ids = {
                node_id
                for node_id in self.node_collections["background"]
                if node_id not in self.node_collections["first_level_background_static"]
            }

        logger.info("Traversing supply chain graph...")
        if graph_traversal == "bfs":
            self.edge_extractor = EdgeExtractorBFS(
                lca_object=base_lca,
                starting_datetime=self.starting_datetime,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                max_calc=self.max_calc,
                static_activity_indices=set(static_background_activity_ids),
                nodes=self.nodes,
                traverse_background=self.traverse_background,
            )
            self.edge_extractor.database_dates_static = self.database_dates_static
            self.edge_extractor.interdatabase_activity_mapping = (
                self.interdatabase_activity_mapping
            )
            self.edge_extractor.interpolation_type = self.interpolation_type
        elif graph_traversal == "priority":
            self.edge_extractor = EdgeExtractor(
                base_lca,
                starting_datetime=self.starting_datetime,
                *args,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                max_calc=self.max_calc,
                static_activity_indices=set(static_background_activity_ids),
                traverse_background=self.traverse_background,
                **kwargs,
            )
            # Same variant-aware descent inputs as the BFS extractor. The priority
            # engine's own ``self.nodes`` are graph-traversal Node objects, so the
            # shared mixin reads bw2data Activity proxies through ``bw_node_proxies``.
            self.edge_extractor.bw_node_proxies = self.nodes
            self.edge_extractor.database_dates_static = self.database_dates_static
            self.edge_extractor.interdatabase_activity_mapping = (
                self.interdatabase_activity_mapping
            )
            self.edge_extractor.interpolation_type = self.interpolation_type
        else:
            raise ValueError(
                f"Unknown graph_traversal '{graph_traversal}'. Use 'priority' or 'bfs'."
            )
        self.edge_timeline = self.edge_extractor.build_edge_timeline()

    def build_timeline(self) -> pd.DataFrame:
        """
        Create a DataFrame with grouped, time-explicit edges and, for each grouped edge,
        interpolate to the database with the closest time of representativeness.

        It uses the edge_timeline, an output from the graph traversal in EdgeExtractor.
        Edges from same producer to same consumer that occur at different times within
        the same time window (temporal_grouping) are grouped together.
        Possible temporal groupings are "year", "month", "day" and "hour".

        For edges between foreground and background system, the column "temporal_market_shares"
        assigns the ratio [0-1] of the edge's amount to be taken from the database with the closest
        time of representativeness. If a process is in the foreground system only, the interpolation weight is set to None.

        Available interpolation types are:

        - "linear": linear interpolation between the two closest databases, based on temporal distance.

        - "closest": closest database is assigned 1

            (all are already passed during instantiation)

        Returns
        -------
        pd.DataFrame
            A timeline with grouped, time-explicit edges and temporal_market_shares to background databases.
        """
        logger.info("Building timeline...")

        # check if database names match with databases in BW project
        self.check_database_names()

        # Check if temporal_grouping is a valid value
        valid_temporal_groupings = ["year", "month", "day", "hour"]
        if self.temporal_grouping not in valid_temporal_groupings:
            raise ValueError(
                f"Invalid value for 'temporal_grouping'. Allowed values are {valid_temporal_groupings}."
            )

        # Extract edge data into a list of dictionaries
        edges_data = [self.extract_edge_data(edge) for edge in self.edge_timeline]

        # Convert list of dictionaries to dataframe
        edges_df = pd.DataFrame(edges_data)

        # adjust the sign for substitution exchanges:
        sign = edges_df["edge_type"].apply(
            lambda x: self.adjust_sign_of_amount_based_on_edge_type(x)
        )
        edges_df["amount"] = edges_df["amount"] * sign
        edges_df["cumulative_amount"] = edges_df["cumulative_amount"] * sign

        # Explode datetime and amount columns: each row with multiple dates and amounts is exploded into multiple rows with one date and one amount
        edges_df = edges_df.explode(
            ["consumer_date", "producer_date", "amount", "cumulative_amount"]
        )

        # Create a hashable key from temporal_evolution dicts for dedup and groupby
        # (dicts are unhashable, so we need a hashable proxy column)
        edges_df["_te_key"] = edges_df["temporal_evolution"].apply(
            lambda d: tuple(sorted(d.items())) if isinstance(d, dict) else None
        )

        # `cumulative_amount` is deliberately excluded from the dedup key: rows
        # that are otherwise identical (same producer/consumer/dates/local
        # amount/edge type) can arise from the same time-collapsed node being
        # reached via multiple distinct ancestor paths (e.g. absolute temporal
        # distributions that look up the same producer dates regardless of
        # which upstream cohort reached them). Those are genuine duplicates
        # from the local-amount/dedup point of view (the existing behavior:
        # keep one, don't sum), but their cumulative amounts must be summed
        # across all such paths before the duplicates are dropped, since each
        # path represents an additive upstream contribution.
        dedup_cols = [
            c
            for c in edges_df.columns
            if c not in ("temporal_evolution", "cumulative_amount")
        ]
        edges_df["cumulative_amount"] = edges_df.groupby(dedup_cols, dropna=False)[
            "cumulative_amount"
        ].transform("sum")
        edges_df.drop_duplicates(subset=dedup_cols, inplace=True)
        edges_df = edges_df[edges_df["amount"] != 0]

        # For the Functional Unit: set consumer date = producer date as it occurs at the same time
        edges_df.loc[edges_df["consumer"] == -1, "consumer_date"] = edges_df.loc[
            edges_df["consumer"] == -1, "producer_date"
        ]

        edges_df["rounded_consumer_date"] = edges_df["consumer_date"].apply(
            lambda x: round_datetime(x, self.temporal_grouping)
        )
        edges_df["rounded_producer_date"] = edges_df["producer_date"].apply(
            lambda x: round_datetime(x, self.temporal_grouping)
        )

        # extract grouping time of consumer and producer: processes occuring at different times within in the same time window of grouping get the same grouping time
        edges_df["consumer_grouping_time"] = edges_df["rounded_consumer_date"].apply(
            lambda x: extract_date_as_string(x, self.temporal_grouping)
        )
        edges_df["producer_grouping_time"] = edges_df["rounded_producer_date"].apply(
            lambda x: extract_date_as_string(x, self.temporal_grouping)
        )

        # group unique pair of consumer and producer with the same grouping times
        # _te_key ensures exchanges with different temporal_evolution dicts stay separate
        grouped_edges = (
            edges_df.groupby(
                [
                    "producer_grouping_time",
                    "consumer_grouping_time",
                    "producer",
                    "consumer",
                    "_te_key",
                    "temporal_evolution_reference",
                ],
                dropna=False,
            )
            .agg({"amount": "sum", "cumulative_amount": "sum"})
            .reset_index()
        )
        # Reconstruct temporal_evolution dicts from the hashable _te_key
        grouped_edges["temporal_evolution"] = grouped_edges["_te_key"].apply(
            lambda k: dict(k) if isinstance(k, tuple) else None
        )
        grouped_edges.drop(columns=["_te_key"], inplace=True)

        # Convert grouping times to datetime with a unique-value cache to avoid repeated parsing
        unique_grouping_strings = set(grouped_edges["producer_grouping_time"]).union(
            set(grouped_edges["consumer_grouping_time"])
        )
        datetime_cache = {
            value: convert_date_string_to_datetime(self.temporal_grouping, value)
            for value in unique_grouping_strings
        }

        grouped_edges["date_producer"] = grouped_edges["producer_grouping_time"].map(
            datetime_cache
        )
        grouped_edges["date_consumer"] = grouped_edges["consumer_grouping_time"].map(
            datetime_cache
        )

        # add dates as integers as hashes to the DataFrame
        hash_cache = {
            dt: extract_date_as_integer(dt, time_res=self.temporal_grouping)
            for dt in datetime_cache.values()
        }
        grouped_edges["hash_producer"] = grouped_edges["date_producer"].map(hash_cache)
        grouped_edges["hash_consumer"] = grouped_edges["date_consumer"].map(hash_cache)

        grouped_edges = self._drop_edges_of_unsupplied_consumers(grouped_edges)

        self._check_traversed_databases_are_mapped(grouped_edges)

        # add new processes to activity_time_mapping
        static_dbs = set(self.database_dates_static.keys()) if self.traverse_background else set()
        for row in grouped_edges.itertuples():
            producer_node = self.nodes[row.producer]
            if self.traverse_background and producer_node["database"] in static_dbs:
                # Traversed background node: store with actual db so the downstream
                # biosphere exchange lookup can identify it unambiguously.
                db_key = producer_node["database"]
            else:
                db_key = "temporalized"
            self.activity_time_mapping.add(
                (
                    (db_key, producer_node["code"]),
                    row.hash_producer,
                )
            )

        # store the ids from the time_mapping in DataFrame
        grouped_edges["time_mapped_producer"] = [
            self.get_time_mapping_key(producer, hash_producer)
            for producer, hash_producer in zip(
                grouped_edges["producer"], grouped_edges["hash_producer"]
            )
        ]

        grouped_edges["time_mapped_consumer"] = [
            self.get_time_mapping_key(consumer, hash_consumer)
            if consumer != -1
            else -1
            for consumer, hash_consumer in zip(
                grouped_edges["consumer"], grouped_edges["hash_consumer"]
            )
        ]

        # Add temporal_market_shares to background databases to the DataFrame
        grouped_edges = self.add_column_temporal_market_shares_to_timeline(
            grouped_edges,
            interpolation_type=self.interpolation_type,
        )

        # Retrieve producer and consumer names
        grouped_edges["producer_name"] = [
            self.nodes[producer]["name"] for producer in grouped_edges["producer"]
        ]
        grouped_edges["consumer_name"] = [
            self.get_consumer_name(consumer) for consumer in grouped_edges["consumer"]
        ]

        # Reorder columns
        grouped_edges = grouped_edges[
            [
                "hash_producer",
                "time_mapped_producer",
                "date_producer",
                "producer",
                "producer_name",
                "hash_consumer",
                "time_mapped_consumer",
                "date_consumer",
                "consumer",
                "consumer_name",
                "amount",
                "cumulative_amount",
                "temporal_market_shares",
                "temporal_evolution",
                "temporal_evolution_reference",
            ]
        ]

        return grouped_edges

    ###################################################
    # underlying functions called by build_timeline() #
    ###################################################

    def _check_traversed_databases_are_mapped(self, grouped_edges: pd.DataFrame) -> None:
        """
        Check that every traversed process lives in a database that is mapped.

        `bw_timex` places a process in time via the database it lives in, so
        every database the traversal reaches must either represent a point in
        time or be marked as `"dynamic"`. Only the databases holding the
        functional unit are treated as dynamic automatically - a foreground
        split across several databases has to mark the other ones itself. A
        database that is mapped nowhere has no node metadata loaded for it,
        which would otherwise surface as a bare `KeyError` on a node id.

        Parameters
        ----------
        grouped_edges : pd.DataFrame
            The timeline edges, with `producer` and `consumer` node ids.

        Returns
        -------
        None

        Raises
        ------
        UnmappedDatabaseError
            If any traversed process is in a database that is not mapped.
        """
        node_ids = set(grouped_edges["producer"]).union(grouped_edges["consumer"])
        unmapped = sorted(
            node_id
            for node_id in node_ids
            if node_id != -1 and node_id not in self.nodes
        )
        if not unmapped:
            return

        examples = {}
        for node_id in unmapped:
            node = bd.get_node(id=node_id)
            examples.setdefault(node["database"], []).append(node["name"])

        databases = ", ".join(
            f"'{database}' (e.g. '{names[0]}'"
            + (f", and {len(names) - 1} more" if len(names) > 1 else "")
            + ")"
            for database, names in examples.items()
        )
        first = next(iter(examples))
        raise UnmappedDatabaseError(
            f"The graph traversal reached processes in database(s) that are not "
            f"mapped to a point in time: {databases}. `bw_timex` places every "
            f"traversed process in time via its database, and only the "
            f"database(s) holding the functional unit are treated as 'dynamic' "
            f"automatically, so a foreground split across several databases has "
            f"to mark the other ones itself.\n"
            f"If '{first}' is part of your foreground, mark it as dynamic:\n"
            f"    bw_timex.set_database_metadata('{first}', representative_time='dynamic')\n"
            f"If it represents a point in time, give it that date instead:\n"
            f"    bw_timex.set_database_metadata('{first}', "
            f"representative_time=datetime(2030, 1, 1))\n"
            f"Databases mapped for this calculation: "
            f"{sorted(self.database_dates)}. When passing `database_dates` "
            f"explicitly, it must list every database the traversal reaches."
        )

    def check_database_names(self) -> None:
        """
        Check that the strings of the databases exist in the databases of the Brightway project.

        """
        for db in self.database_dates_static.keys():
            assert (
                db in bd.databases
            ), f"{db} is not in your Brightway project databases."

    def extract_edge_data(self, edge: Edge) -> dict:
        """
        Stores the attributes of an Edge instance in a dictionary.

        Parameters
        ----------
        edge: Edge
            Edge instance

        Returns
        -------
        dict
            Dictionary with the attributes of the edge instance.
        """
        try:
            consumer_date = edge.abs_td_consumer.date
            consumer_date = np.array(
                [consumer_date for i in range(len(edge.td_producer))]
            ).T.flatten()
        except AttributeError:
            consumer_date = None

        return {
            "producer": edge.producer,
            "consumer": edge.consumer,
            "consumer_date": consumer_date,
            "producer_date": edge.abs_td_producer.date,
            "amount": edge.abs_td_producer.amount,
            # Same dates/shape as "amount", but holding the true cumulative
            # (supply-chain-scaled) throughput rather than just the local,
            # per-unit-of-immediate-consumer exchange amount. Used to build the
            # dynamic inventory directly from the timeline
            # (`TimexLCA.lci(expand_technosphere=False)`), where there is no
            # matrix solve to derive this scale from otherwise.
            "cumulative_amount": edge.cumulative_amount_producer.amount,
            "edge_type": edge.edge_type,
            "temporal_evolution": edge.temporal_evolution,
            "temporal_evolution_reference": edge.temporal_evolution_reference,
        }

    def adjust_sign_of_amount_based_on_edge_type(self, edge_type):
        """
        It checks if the an exchange is of type substitution or a technosphere exchange,
        based on bw2data labelling convention, and adjusts the amount accordingly.
        Flips the sign of the amount value in the timeline for substitution (positive technosphere) exchanges.

        Parameters
        ----------
        edge_type: str
            Type of the edge, as defined in the exchange data.

        Returns
        -------
        int
            Multiplier for the amount value, 1 for technosphere exchanges, -1 for substitution exchanges.

        """

        if edge_type in labels.technosphere_negative_edge_types:
            return 1  # Variants of technosphere labels

        if edge_type in labels.technosphere_positive_edge_types:
            return -1 if edge_type in labels.substitution_edge_types else 1

        raise TypeError(f"Unrecognized type in this edge: {edge_type}")

    @staticmethod
    def _drop_edges_of_unsupplied_consumers(grouped_edges: pd.DataFrame) -> pd.DataFrame:
        """Drop edges whose consumer cohort is never produced.

        Edges with ``amount == 0`` are dropped before grouping, so a node cohort
        that is only reached via zero-amount edges (e.g. a background temporal
        distribution with a zero weight at one of its dates) has no producing
        row left. The edges *out* of that cohort do survive, because their own
        exchange amounts are non-zero - but they carry no throughput, since
        nothing supplies their consumer. Keeping them would ask the activity
        time mapping for a consumer that was never registered (only producers
        are), so drop them, and iteratively whatever hangs off them.
        """
        n_before = len(grouped_edges)
        producer_instance = (
            grouped_edges["producer"].astype(str)
            + "|"
            + grouped_edges["producer_grouping_time"]
        )
        consumer_instance = (
            grouped_edges["consumer"].astype(str)
            + "|"
            + grouped_edges["consumer_grouping_time"]
        )
        is_functional_unit = grouped_edges["consumer"] == -1
        while True:
            # Dropping edges can orphan their producer, so iterate to a fixpoint.
            keep = is_functional_unit | consumer_instance.isin(set(producer_instance))
            if keep.all():
                break
            grouped_edges = grouped_edges[keep]
            producer_instance = producer_instance[keep]
            consumer_instance = consumer_instance[keep]
            is_functional_unit = is_functional_unit[keep]

        if len(grouped_edges) < n_before:
            logger.info(
                f"Dropped {n_before - len(grouped_edges)} edge(s) from the timeline "
                f"whose consuming process received no supply."
            )
        return grouped_edges.reset_index(drop=True)

    def get_time_mapping_key(self, node_id: int, node_hash: int) -> int:
        """
        Returns the time_mapping_id (key) from the activity_time_mapping for a given node.

        Parameters
        ----------
        node_id: int
            database id of the node.
        node_hash: int
            datetime_as_integer of the node.

        Returns
        -------
        int
            time_mapping_id (key) of the corresponding time-mapped activity.

        """
        try:
            return self.activity_time_mapping[
                (("temporalized", self.nodes[node_id]["code"]), node_hash)
            ]
        except KeyError:
            return self.activity_time_mapping[((self.nodes[node_id].key), node_hash)]

    def _leaf_background_producers(self, edges_df: pd.DataFrame) -> set:
        """Producers that are leaves (never traversed into) and live in a static
        background db. These are the temporal-market frontier."""
        consumers = set(edges_df["consumer"].unique())
        static_dbs = set(self.database_dates_static.keys())
        leaves = set()
        for producer in edges_df["producer"].unique():
            if producer in consumers:
                continue  # traversed into -> temporalized, not a market
            # Producers the descent resolved to their variant database but never
            # descended into (cut off by `cutoff` or the `max_calc` budget) are
            # leaves like any other: nothing upstream of them is in the timeline,
            # so they need their full background LCI. Their already-resolved
            # variant is preserved by pinning their market to their own database
            # in `add_column_temporal_market_shares_to_timeline`, so the routing
            # made during the descent is not interpolated a second time.
            node = self.nodes.get(producer)
            if node is not None and node["database"] in static_dbs:
                leaves.add(producer)
        return leaves

    def candidate_databases_for_producers(self, producers: set) -> dict:
        """Map each producer to the static databases that hold a match for it.

        Returns ``{producer_id: {date: database_name}}``. A candidate is a static
        background database containing a node with the same ``(name, reference
        product, location)`` as the producer. Several databases may share a date;
        if two of them hold a match for the same producer, the model is ambiguous
        and this raises.

        As a side effect, ``self.market_producer_matches`` is filled with
        ``{producer_id: {database_name: node_id}}``, which is exactly what
        ``TimexLCA.add_interdatabase_activity_mapping_from_timeline`` needs.
        """
        triplets = {}
        for producer in producers:
            node = self.nodes[producer]
            key = (node["name"], node.get("reference product"), node["location"])
            triplets.setdefault(key, []).append(producer)

        candidates = {producer: {} for producer in producers}
        matches = {producer: {} for producer in producers}
        for node in self.nodes.values():
            date = self.database_dates_static.get(node["database"])
            if date is None:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            for producer in triplets.get(key, ()):
                already = candidates[producer].get(date)
                if already is not None and already != node["database"]:
                    raise ValueError(
                        f"Producer '{node['name']}' was found in more than one database "
                        f"at {date:%Y-%m-%d}: '{already}' and '{node['database']}'. "
                        "bw_timex cannot tell which one its temporal market should use. "
                        "Give the copy a distinct name, reference product or location, "
                        "or remove one of the two databases from `database_dates` or "
                        "its `representative_time` metadata."
                    )
                candidates[producer][date] = node["database"]
                matches[producer][node["database"]] = node.id

        # A producer missing from some vintages is not logged: it is normal (a
        # hand-built family covering fewer years than the background, a process
        # premise only introduces later), and warning per producer buried
        # everything else. The resulting temporal market shares say the same
        # thing and are inspectable on the timeline.

        self.market_producer_matches = matches
        return candidates

    def add_column_temporal_market_shares_to_timeline(
        self,
        tl_df: pd.DataFrame,
        interpolation_type: str = "linear",
    ) -> pd.DataFrame:
        """
        Add a column to a timeline with the weights for an interpolation between
        the two nearest dates, from the list of dates of the available databases.

        Parameters
        ----------
        tl_df: pd.DataFrame
            Timeline as a DataFrame.
        interpolation_type: str, optional
            Type of interpolation between the nearest lower and higher dates.
            Available options: "linear" and "nearest", defaulting to "linear".

        Returns
        -------
        pd.DataFrame
            Timeline as a DataFrame with a column 'temporal_market_shares' added,
            this column looks like {database_name: weight, database_name: weight}.
        """
        if not self.database_dates_static:
            tl_df["temporal_market_shares"] = None
            logger.info(
                "No time-explicit databases are provided. Mapping to time-explicit databases is not possible.",
            )
            return tl_df

        if "date_producer" not in list(tl_df.columns):
            raise ValueError("The timeline does not contain dates.")

        if interpolation_type not in ("linear", "nearest"):
            raise ValueError(
                f"Sorry, but {interpolation_type} interpolation is not available yet."
            )

        if self.traverse_background:
            market_producers = self._leaf_background_producers(tl_df)
        else:
            market_producers = self.node_collections["first_level_background_static"]

        producers_in_timeline = {
            producer
            for producer in tl_df["producer"].unique()
            if producer in market_producers
        }
        candidate_databases = self.candidate_databases_for_producers(
            producers_in_timeline
        )

        # Producers the background descent already routed to a specific variant
        # database. When such a producer ends up a market (descent stopped there),
        # it keeps that database instead of being interpolated a second time.
        variant_resolved = getattr(
            self.edge_extractor, "variant_resolved_producers", set()
        )

        weight_cache = {}
        shares = []
        for producer, producer_date in zip(tl_df["producer"], tl_df["date_producer"]):
            if producer not in producers_in_timeline:
                shares.append(None)
                continue
            if producer in variant_resolved:
                shares.append({self.nodes[producer]["database"]: 1})
                continue
            candidates = candidate_databases[producer]
            sorted_dates = tuple(sorted(candidates))
            # The cache key must include which database each date maps to, not
            # just the set of dates: two producers can share the same vintage
            # dates while drawing from different database families (e.g. an
            # untouched background producer vs. a foreground-modified copy
            # kept in its own database), and only the mapping distinguishes them.
            cache_key = (tuple(sorted(candidates.items())), producer_date)
            if cache_key not in weight_cache:
                if interpolation_type == "nearest":
                    weights = self.find_closest_date(producer_date, sorted_dates)
                else:
                    weights = self.get_weights_for_interpolation_between_nearest_years(
                        producer_date, sorted_dates, interpolation_type
                    )
                weight_cache[cache_key] = {
                    candidates[date]: share for date, share in weights.items()
                }
            shares.append(weight_cache[cache_key])

        tl_df["temporal_market_shares"] = shares

        return tl_df

    def find_closest_date(self, target: datetime, dates: tuple[datetime, ...]) -> dict:
        """
        Find the closest date to the target in the dates list.

        Parameters
        ----------
        target : datetime.datetime
            Target datetime object.
        dates : KeysView[datetime]
            List of datetime.datetime objects.

        Returns
        -------
        dict
            Dictionary with the key as the closest datetime.datetime object from the list and a value of 1.
        """

        return nearest_date_weight(target, dates)

    def get_weights_for_interpolation_between_nearest_years(
        self,
        reference_date: datetime,
        dates_list: tuple[datetime, ...],
        interpolation_type: str | None = None,
    ) -> dict:
        """
        Find the nearest dates (lower and higher) for a given date from a list of dates
        and calculate the interpolation weights based on temporal proximity.

        Parameters
        ----------
        reference_date : datetime
            Target date.
        dates_list : KeysView[datetime]
            List of datetime objects representing the temporal representativeness of the available databases.
        interpolation_type : str, optional
            Type of interpolation between the nearest lower and higher dates. For now, only "linear" is available.

        Returns
        -------
        dict
            Dictionary with datetimes of the available closest databases as keys and the weights for interpolation as values.
        """
        interpolation_type = interpolation_type or self.interpolation_type
        if interpolation_type != "linear":
            raise ValueError(
                f"Sorry, but {interpolation_type} interpolation is not available yet."
            )

        position = bisect_left(dates_list, reference_date)
        if position == 0 and not (
            position < len(dates_list) and dates_list[position] == reference_date
        ):
            if not getattr(self, "_logged_reference_date_below_range", False):
                logger.info(
                    "Reference date {} is lower than all provided dates. Data will be taken from the closest higher year.",
                    reference_date,
                )
                self._logged_reference_date_below_range = True
        elif position == len(dates_list):
            if not getattr(self, "_logged_reference_date_above_range", False):
                logger.info(
                    "Reference date {} is higher than all provided dates. Data will be taken from the closest lower year.",
                    reference_date,
                )
                self._logged_reference_date_above_range = True

        return linear_interpolation_weights(reference_date, dates_list)

    def get_consumer_name(self, idx: int) -> str:
        """
        Returns the name of consumer node.
        If consuming node is the functional unit, returns -1.

        Parameters
        ----------
        idx : int
            Id of node.

        Returns
        -------
        str
            Name of the node or -1
        """
        try:
            return self.nodes[idx]["name"]
        except KeyError:
            return "-1"  # functional unit
