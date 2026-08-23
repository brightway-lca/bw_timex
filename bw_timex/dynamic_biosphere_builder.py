import bw2data as bd
import numpy as np
import pandas as pd
from bw2calc import LCA
from bw_temporalis import TemporalDistribution
from scipy import sparse as sp

from .helper_classes import SetList
from .utils import (
    convert_date_string_to_datetime,
    get_reference_product_production_amount,
    get_temporal_evolution_factor,
)


class DynamicBiosphereBuilder:
    """
    Class for building a dynamic biosphere matrix with dimensions (biosphere flow at a specific point in time) x (processes)
    """

    def __init__(
        self,
        lca_obj: LCA,
        activity_time_mapping: dict,
        biosphere_time_mapping: dict,
        demand_timing: dict,
        node_collections: dict,
        temporal_grouping: str,
        database_dates: dict,
        database_dates_static: dict,
        timeline: pd.DataFrame,
        interdatabase_activity_mapping: SetList,
        expand_technosphere: bool = True,
        background_solver=None,
        nodes: dict | None = None,
        keep_activity_dimension: bool = True,
        group_background_by_time: bool = False,
    ) -> None:
        """
        Initializes the DynamicBiosphereBuilder object.

        Parameters
        ----------
        lca_obj : LCA object
            instance of the bw2calc LCA class, e.g. TimexLCA.lca
        activity_time_mapping : dict
            A dictionary mapping activity to their respective timing in the format
            (('database', 'code'), datetime_as_integer): time_mapping_id)
        biosphere_time_mapping : dict
            A dictionary mapping biosphere flows to their respective timing in the format
            (('database', 'code'), datetime_as_integer): time_mapping_id), empty at this point.
        demand_timing : dict
            A dictionary mapping of the demand to demand time
        node_collections : dict
            A dictionary containing lists of node ids for different node subsets
        temporal_grouping : str
            A string indicating the temporal grouping of the processes, e.g. 'year', 'month',
            'day', 'hour'
        database_dates : dict
            A dictionary mapping database names to their respective date
        database_dates_static : dict
            A dictionary mapping database names to their respective date, but only containing
            static databases, which are the background databases.
        timeline: pd.DataFrame
            The edge timeline, created from TimexLCA.build_timeline()
        interdatabase_activity_mapping : SetList
            A list of sets, where each set contains the activity ids of the same activity in
            different databases
        expand_technosphere : bool, optional
            A boolean indicating if the dynamic biosphere matrix is built via expanded matrices or directly from the timeline.
            Default is True.
        background_solver : BackgroundSolver, optional
            Solver supplying the unit background LCIs the temporal markets are
            made of. Required whenever the timeline contains temporal markets;
            `TimexLCA` builds one per `lci()` call and hands the same instance
            to every builder, so its supply/aggregate caches are shared.
        nodes : dict, optional
            A dictionary mapping node ids to their bw2data node proxies, as collected by
            `TimexLCA`. Used to resolve producers by id instead of by code, which is only
            unique within a database.

        Returns
        -------
        None

        """

        self._time_res_mapping = {
            "year": "datetime64[Y]",
            "month": "datetime64[M]",
            "day": "datetime64[D]",
            "hour": "datetime64[h]",
        }

        self.lca_obj = lca_obj

        self._expand_technosphere = bool(expand_technosphere)
        # With the activity dimension dropped, every emission goes into a single
        # column, already scaled by its activity's supply. That is all a score -
        # static or dynamic - needs, and it keeps the entry count proportional to
        # the number of (flow, time) pairs instead of (flow, time, activity).
        self.keep_activity_dimension = bool(keep_activity_dimension)
        # Sum the background demands of every temporal-market row landing at
        # the same point in time, and solve those sums instead of one unit LCI
        # per background process. Only lossless when the rows in question end
        # up in the same column anyway, which is exactly what dropping the
        # activity dimension does - and only wired for the timeline build,
        # where a row *is* a column. `TimexLCA` decides whether it is also
        # cheaper; see `_plan_background_solves`.
        self.group_background_by_time = bool(
            group_background_by_time
            and not keep_activity_dimension
            and not expand_technosphere
        )

        if expand_technosphere:
            self.technosphere_matrix = (
                lca_obj.technosphere_matrix.tocsc()
            )  # convert to csc as this is only used for column slicing
            self.dynamic_supply_array = lca_obj.supply_array
            self.activity_dict = lca_obj.dicts.activity
            # Only used when building from the timeline, where each row is its
            # own column; with expanded matrices the columns already collapse.
            self.collapsed_market_rows = set()
        else:
            # `timeline.amount` is the LOCAL, per-edge exchange amount (per unit
            # of the immediate consumer); it does not carry the upstream
            # supply-chain scaling that a real linear solve provides "for free"
            # in the `expand_technosphere=True` branch above. `cumulative_amount`
            # is its recursively-scaled counterpart (see `edge_extractor.py`'s
            # `Edge.cumulative_amount_producer` / `_join_cumulative_amount`) and
            # is the correct quantity to scale each timeline row's biosphere/
            # market contribution by.
            self.dynamic_supply_array, self.collapsed_market_rows = (
                self._supply_array_from_timeline(timeline, node_collections)
            )

        self.activity_time_mapping = activity_time_mapping
        self.biosphere_time_mapping = biosphere_time_mapping
        self.demand_timing = demand_timing
        self.node_collections = node_collections
        self.time_res = self._time_res_mapping[temporal_grouping]
        self.temporal_grouping = temporal_grouping
        self.database_dates = database_dates
        self.database_dates_static = database_dates_static
        self.timeline = timeline
        self.interdatabase_activity_mapping = interdatabase_activity_mapping
        self.nodes = nodes if nodes is not None else {}
        self._matrix_entries = {}  # (row, col) -> amount
        # Biosphere exchanges of foreground/background producers are read
        # from the bw2data SQL store; share results across TimexLCA objects.
        from ._lci_cache import BIOSPHERE_EXCHANGES_CACHE
        self._activity_biosphere_exchange_cache = BIOSPHERE_EXCHANGES_CACHE
        self.background_solver = background_solver
        if background_solver is not None:
            # A `BackgroundSolver` has no notion of a time mapping, so it
            # cannot tell a stable background-process identity from a
            # time-mapped or temporalized one - the builder can, and that
            # split is what keeps unstable keys out of the module-level cache.
            background_solver.cache_key = self.get_background_lci_cache_key
        # Per temporal market: which background activities it demands, in what
        # amount per unit of market output, and the market's own supply. Two
        # small dicts of floats instead of one `B @ diag(x)` matrix per market
        # - see `TimexLCA.temporal_market_lcis`, which materializes those
        # matrices from these recipes only when something asks for them.
        self.temporal_market_recipes = {}
        self.temporal_market_scales = {}
        self.temporal_market_cols = []  # To keep track of temporal market columns
        # Time step -> {background activity: demand, supply already folded in}.
        # Only filled when `group_background_by_time` is on.
        self._grouped_background_demands: dict = {}

    @staticmethod
    def _supply_array_from_timeline(
        timeline: pd.DataFrame, node_collections: dict
    ) -> tuple[np.ndarray, set]:
        """Per-timeline-row supply, used instead of a solved supply array when
        the dynamic inventory is built directly from the timeline.

        `timeline.cumulative_amount` is the supply-chain-scaled amount of the
        producer's PRODUCT (the local, per-consumer `timeline.amount` scaled by
        all upstream edges). That is what the temporal markets need, since their
        background unit LCIs are also per unit of product. A temporalized
        process, however, is scaled by its own production amount when its
        biosphere exchanges are read (those are per production amount, not per
        unit of product), which the expanded technosphere gets for free from the
        solve. So convert those rows to process units here.

        Rows that share a time-mapped temporal market are collapsed onto the
        first of them: they all carry the same background LCI per unit of market
        output, so the inventory only depends on their summed supply. Returns
        the supply array and the positions of the collapsed-away rows, whose
        columns are left empty.
        """
        supply = timeline.cumulative_amount.values.astype(float)
        temporalized = node_collections["temporalized_processes"]
        markets = node_collections["temporal_markets"]
        production_amounts = {}
        market_positions = {}
        for position, row in enumerate(timeline.itertuples()):
            if row.time_mapped_producer in temporalized:
                if row.producer not in production_amounts:
                    production_amounts[row.producer] = (
                        get_reference_product_production_amount(row.producer)
                    )
                supply[position] /= production_amounts[row.producer]
            elif row.time_mapped_producer in markets:
                market_positions.setdefault(row.time_mapped_producer, []).append(
                    position
                )

        collapsed_market_rows = set()
        for positions in market_positions.values():
            if len(positions) == 1:
                continue
            keep, rest = positions[0], positions[1:]
            supply[keep] += supply[rest].sum()
            supply[rest] = 0.0
            collapsed_market_rows.update(rest)
        return supply, collapsed_market_rows

    def build_dynamic_biosphere_matrix(
        self,
        expand_technosphere: bool = True,
    ):
        """
        This function creates a separate biosphere matrix, with the dimensions
        (bio_flows at a specific time step) x (processes).

        Every temporally resolved biosphere flow has its own row in the matrix, making it highly
        sparse. The timing of the emitting process and potential additional temporal information of
        the biosphere flow (e.g. delay of emission compared to the timing of the process) are considered.

        Absolute Temporal Distributions for biosphere exchanges are dealt with as a look up
        function: If an activity happens at timestamp X and the biosphere exchange has an
        absolute temporal distribution (ATD), it looks up the amount from the ATD corresponding
        to timestamp X. E.g.: X = 2024, TD=(data=[2020,2021,2022,2023,2024,.....,2120],
        amount=[3,4,4,5,6,......,3]), it will look up the value 6 corresponding 2024. If timestamp X
        does not exist, it finds the nearest timestamp available (if two timestamps are equally close,
        it will take the first in order of appearance (see numpy.argmin() for this behavior).

        Parameters
        ----------
        expand_technosphere : bool, optional
            A boolean indicating if the dynamic biosphere matrix is built via expanded matrices
            or directly from the timeline. Default is via expanded matrices.

        Returns
        -------
        dynamic_biosphere_matrix : scipy.sparse.csr_matrix
            A sparse matrix with the dimensions (bio_flows at a specific time step) x (processes).
            The temporal markets' background recipes are left on
            `temporal_market_recipes` / `temporal_market_scales`.
        """

        for row in self.timeline.itertuples():
            idx = row.time_mapped_producer
            # Deduplicates repeated (flow, time) entries within one activity,
            # which the per-activity columns do implicitly.
            seen_rows = set()

            if expand_technosphere:
                process_col_index = self.activity_dict[
                    idx
                ]  # get the matrix column index
            else:  # from timeline
                process_col_index = row.Index  # start a new matrix

            (
                (original_db, original_code),
                time,
            ) = self.activity_time_mapping.reversed[idx]

            if idx in self.node_collections["temporalized_processes"]:

                time_in_datetime = convert_date_string_to_datetime(
                    self.temporal_grouping, str(time)
                )  # now time is a datetime

                td_producer = TemporalDistribution(
                    date=np.array([time_in_datetime], dtype=self.time_res),
                    amount=np.array([1]),
                ).date
                date = td_producer[0]

                # Get temporal evolution factor for this timestamp
                temporal_evolution_factor = 1.0
                if hasattr(row, "temporal_evolution") and row.temporal_evolution is not None:
                    reference = getattr(row, "temporal_evolution_reference", "producer")
                    reference_time = (
                        row.date_consumer if reference == "consumer" else time_in_datetime
                    )
                    temporal_evolution_factor = get_temporal_evolution_factor(
                        row.temporal_evolution, reference_time
                    )

                for input_id, exc_amount, temporal_distribution in (
                    self.get_biosphere_exchanges(
                        original_db, original_code, producer_id=row.producer
                    )
                ):
                    if temporal_distribution:
                        td_dates = temporal_distribution.date
                        td_values = temporal_distribution.amount
                        # If the biosphere flows have an absolute TD, this means we have to look up
                        # the biosphere flow for the activity time (td_producer)
                        if isinstance(td_dates[0], np.datetime64):
                            dates = td_producer  # datetime array, same time as producer
                            values = [
                                exc_amount
                                * temporal_evolution_factor
                                * td_values[
                                    np.argmin(
                                        np.abs(
                                            td_dates.astype(self.time_res)
                                            - td_producer.astype(self.time_res)
                                        )
                                    )
                                ]
                            ]  # look up the value correponding to the absolute producer time
                        else:
                            # we can add a datetime of len(1) to a timedelta of len(N) easily
                            dates = td_producer + td_dates
                            values = exc_amount * temporal_evolution_factor * td_values

                    else:  # exchange has no TD
                        dates = td_producer  # datetime array, same time as producer
                        values = [exc_amount * temporal_evolution_factor]

                    # Add entries to dynamic bio matrix
                    for date, amount in zip(dates, values):

                        # first create a row index for the tuple (bioflow_id, date)
                        time_mapped_matrix_idx = self.biosphere_time_mapping.add(
                            (input_id, date)
                        )

                        # populate lists with which sparse matrix is constructed
                        self._add_entry(
                            row=time_mapped_matrix_idx,
                            col=process_col_index,
                            amount=amount,
                            seen_rows=seen_rows,
                        )

            elif idx in self.node_collections["temporal_markets"]:
                if expand_technosphere and idx in self.temporal_market_recipes:
                    # Several timeline rows (one per consumer) can share a
                    # time-mapped market, but with expanded matrices they all
                    # map to the same column, whose supply already sums them up.
                    continue
                if row.Index in self.collapsed_market_rows:
                    # Built from the timeline: this row's market is served by
                    # another row's column, which carries their summed supply.
                    continue
                self.temporal_market_cols.append(process_col_index)
                (
                    (original_db, original_code),
                    time,
                ) = self.activity_time_mapping.reversed[idx]

                if expand_technosphere:
                    demand = self.demand_from_technosphere(idx, process_col_index)
                else:
                    demand = self.demand_from_timeline(row)

                if demand and self.group_background_by_time:
                    # Defer to the grouped pass after the loop: fold this
                    # row's supply into its demand now (there is no column
                    # left to scale by afterwards) and sum it into the time
                    # step the emissions land at.
                    scale = float(self.dynamic_supply_array[process_col_index])
                    target = self._grouped_background_demands.setdefault(time, {})
                    for act, amount in demand.items():
                        target[act] = target.get(act, 0.0) + amount * scale
                    continue

                if demand:
                    # Emissions of all background activities of the temporal
                    # market, per unit of market output, already summed over
                    # the background processes that caused them. Only this
                    # aggregate feeds the dynamic biosphere matrix; the
                    # per-process breakdown is kept as a recipe below.
                    aggregated_inventory = None
                    for act, amount in demand.items():
                        contribution = self.get_background_unit_aggregate(act) * amount
                        aggregated_inventory = (
                            contribution
                            if aggregated_inventory is None
                            else aggregated_inventory + contribution
                        )

                    if expand_technosphere:
                        # Recorded only for `disaggregate_background_lci()`,
                        # which needs the expanded technosphere. A recipe is a
                        # handful of floats; the matrices it stands for are
                        # megabytes each, and real background systems have
                        # hundreds of thousands of market rows.
                        recipe = self.temporal_market_recipes.setdefault(idx, {})
                        for act, amount in demand.items():
                            recipe[act] = recipe.get(act, 0.0) + amount
                        self.temporal_market_scales[idx] = float(
                            self.dynamic_supply_array[process_col_index]
                        )

                    time_in_datetime = convert_date_string_to_datetime(
                        self.temporal_grouping, str(time)
                    )  # now time is a datetime

                    date = TemporalDistribution(
                        date=np.array([str(time_in_datetime)], dtype=self.time_res),
                        amount=np.array([1]),
                    ).date[0]

                    # A background LCI touches a few hundred of the thousands of
                    # biosphere flows; the rest would only add explicit zeros.
                    for row_idx in np.flatnonzero(aggregated_inventory):
                        bioflow = self.lca_obj.dicts.biosphere.reversed[row_idx]

                        time_mapped_matrix_idx = self.biosphere_time_mapping.add(
                            (bioflow, date)
                        )

                        self._add_entry(
                            row=time_mapped_matrix_idx,
                            col=process_col_index,
                            amount=aggregated_inventory[row_idx],
                            seen_rows=seen_rows,
                        )

        if self.group_background_by_time:
            self._add_grouped_background_entries()

        # now build the dynamic biosphere matrix
        if not self.keep_activity_dimension:
            ncols = 1
        elif expand_technosphere:
            ncols = len(self.activity_time_mapping)
        else:
            ncols = len(self.timeline)

        if not self._matrix_entries:
            return sp.csr_matrix((0, ncols))

        # Filled element-wise into pre-sized arrays rather than via Python
        # lists: real background systems reach tens of millions of entries,
        # where boxed ints cost several GB more than the arrays themselves.
        n_entries = len(self._matrix_entries)
        rows = np.empty(n_entries, dtype=np.int64)
        cols = np.empty(n_entries, dtype=np.int64)
        values = np.empty(n_entries, dtype=float)
        for position, ((row, col), amount) in enumerate(self._matrix_entries.items()):
            rows[position] = row
            cols[position] = col
            values[position] = amount

        shape = (rows.max() + 1, ncols)
        dynamic_biosphere_matrix = sp.coo_matrix(
            (values, (rows, cols)), shape
        )
        dynamic_biosphere_matrix = dynamic_biosphere_matrix.tocsr()

        return dynamic_biosphere_matrix

    def _add_grouped_background_entries(self):
        """Emit one solved background aggregate per time step.

        `sum_r B A^-1 d_r` and `B A^-1 sum_r d_r` are the same number, and with
        no activity dimension every `r` at a given time step writes into the
        same column - so the sum can be taken before the solve. Supply was
        already folded into each `d_r` when it was collected, which is why
        these entries bypass `_add_entry`'s scaling.

        Emitted after the timeline loop, so `biosphere_time_mapping` hands out
        its row ids in a different order than an ungrouped build would. The
        rows carry the same `(flow, time)` pairs with the same amounts - only
        their position in the matrix differs, and everything user-facing
        (`dynamic_inventory_df`, the scores) goes through the mapping.
        """
        for time, demand in self._grouped_background_demands.items():
            aggregate = self.background_solver.aggregate_for_demand(demand)

            time_in_datetime = convert_date_string_to_datetime(
                self.temporal_grouping, str(time)
            )
            date = TemporalDistribution(
                date=np.array([str(time_in_datetime)], dtype=self.time_res),
                amount=np.array([1]),
            ).date[0]

            for row_idx in np.flatnonzero(aggregate):
                bioflow = self.lca_obj.dicts.biosphere.reversed[row_idx]
                key = (self.biosphere_time_mapping.add((bioflow, date)), 0)
                self._matrix_entries[key] = (
                    self._matrix_entries.get(key, 0.0) + aggregate[row_idx]
                )

    def collect_background_demands_by_time(self):
        """Plan the grouped background solves: time step -> summed demand.

        The counterpart of `collect_background_demands`, which groups the same
        walk by temporal market instead. Supply is folded in here exactly as
        the build does it, so the number of distinct `(time, block)` pairs in
        the result is the number of solves grouping would actually cost.
        """
        return self.collect_background_demand_plan()[1]

    def collect_background_demand_plan(self):
        """Both groupings of the background demands, from a single walk.

        `TimexLCA` compares the two solve strategies before building, which
        needs the demands grouped per temporal market *and* per time step.
        Collecting them separately walks the timeline twice and re-derives
        every row's demand twice - on a premise-sized model that costs more
        than the grouping it is trying to choose.

        Returns
        -------
        tuple of dict
            `(by_market, by_time)`. `by_market` matches
            `collect_background_demands`; `by_time` carries each row's supply
            folded into its amounts, as the grouped build applies it.
        """
        by_market, by_time = {}, {}
        for row in self.timeline.itertuples():
            idx = row.time_mapped_producer
            if idx not in self.node_collections["temporal_markets"]:
                continue
            if row.Index in self.collapsed_market_rows:
                continue
            demand = self.demand_from_timeline(row)
            if not demand:
                continue

            market = by_market.setdefault(idx, {})
            _, time = self.activity_time_mapping.reversed[idx]
            scale = float(self.dynamic_supply_array[row.Index])
            grouped = by_time.setdefault(time, {})
            for act, amount in demand.items():
                market[act] = market.get(act, 0.0) + amount
                grouped[act] = grouped.get(act, 0.0) + amount * scale
        return by_market, by_time

    def demand_from_timeline(self, row):
        """
        Returns a demand dict directly from the timeline row
        and its temporal_market_shares.

        Parameters:
        -----------
        row: pd.Series
            A row of the timeline DataFrame

        Returns
        -------
        demand: dict
            A demand-dictionary with as keys the ids of the time-mapped activities
            and as values the share.


        """
        demand = {}
        for db, amount in row.temporal_market_shares.items():
            timed_act_id = self.interdatabase_activity_mapping.find_match(
                row.producer, db
            )
            demand[timed_act_id] = amount
        return demand

    def demand_from_technosphere(self, idx, process_col_index):
        """
        Returns a demand dict of background processes based on the technosphere column.
        Foreground exchanges are skipped as these are added separately.

        Parameters:
        -----------
        idx: int
            The time-mapped-activity id of the producer
        process_col_index: int
            The technosphere matrix id of the producer

        Returns
        -------
        demand: dict
            A demand-dictionary with as keys the brightway ids of the consumed background
            activities and as values their consumed amount.
        """
        col = self.technosphere_matrix[:, process_col_index]  # Sparse column
        activity_row = self.activity_dict[idx]  # Producer's row index
        foreground_nodes = self.node_collections["foreground"]

        demand = {
            self.activity_dict.reversed[row_idx]: -amount
            for row_idx, amount in zip(col.nonzero()[0], col.data)
            if row_idx != activity_row  # Skip production exchange
            and self.activity_dict.reversed[row_idx]
            not in foreground_nodes  # Only background
        }

        return demand

    def _add_entry(self, row, col, amount, seen_rows=None):
        """Add one dynamic biosphere entry, honouring `keep_activity_dimension`.

        With the activity dimension dropped, entries of different activities land
        in the same column, so they are summed rather than deduplicated - and the
        activity's supply is applied here, since there is no per-activity column
        left to scale afterwards. `seen_rows` keeps the deduplication *within* an
        activity that `add_matrix_entry_for_biosphere_flows` does.
        """
        if self.keep_activity_dimension:
            self.add_matrix_entry_for_biosphere_flows(row=row, col=col, amount=amount)
            return
        if row in seen_rows:
            return
        seen_rows.add(row)
        key = (row, 0)
        self._matrix_entries[key] = (
            self._matrix_entries.get(key, 0.0)
            + amount * self.dynamic_supply_array[col]
        )

    def add_matrix_entry_for_biosphere_flows(self, row, col, amount):
        """
        Adds an entry to the internal matrix-entry mapping, which is then used to construct
        the dynamic biosphere matrix. Only unique entries are added, i.e. if the same row and
        col index already exists, the value is not added again.

        Parameters
        ----------
        row : int
            A row index of a new element to the dynamic biosphere matrix
        col: int
            A column index of a new element to the dynamic biosphere matrix
        amount: float
            The amount of the new element to the dynamic biosphere matrix

        Returns
        -------
        None
            the internal matrix-entry mapping is updated

        """

        key = (row, col)
        if key not in self._matrix_entries:
            self._matrix_entries[key] = amount

    def get_biosphere_exchanges(self, original_db, original_code, producer_id=None):
        """Return cached biosphere exchanges for a producer.

        Keyed by the source database's `modified` token so foreground or
        background edits invalidate stale entries automatically.

        Temporalized producers are stored in the activity time mapping under
        the pseudo-database "temporalized", which does not identify a node:
        codes are only unique *within* a database, so the same code can exist
        in several databases of a project (e.g. a benchmark copy of a
        foreground, or a background process copied into every vintage). The
        timeline's `producer` id is unambiguous, so resolve the real node from
        it and key the cache on its actual database.
        """
        act = None
        if original_db == "temporalized" and producer_id is not None:
            act = self.nodes.get(producer_id) or bd.get_node(id=producer_id)
            original_db = act["database"]

        modified = (
            bd.databases[original_db].get("modified")
            if original_db in bd.databases
            else None
        )
        cache_key = (bd.projects.current, original_db, original_code, modified)
        if cache_key not in self._activity_biosphere_exchange_cache:
            if act is None:
                if original_db == "temporalized":
                    act = bd.get_node(code=original_code)
                else:
                    act = bd.get_node(database=original_db, code=original_code)
            self._activity_biosphere_exchange_cache[cache_key] = [
                (exc.input.id, exc["amount"], exc.get("temporal_distribution"))
                for exc in act.biosphere()
            ]
        return self._activity_biosphere_exchange_cache[cache_key]

    def get_background_unit_aggregate(self, act):
        """Unit background LCI of an activity, aggregated over its processes.

        Parameters
        ----------
        act : int
            Node id of the background activity.

        Returns
        -------
        numpy.ndarray
            Emissions per unit of `act`, dense over the biosphere rows of
            `lca_obj`, summed over the background processes that emit them.

        Notes
        -----
        Only the aggregate reaches the dynamic biosphere matrix: a temporal
        market contributes one column, so the per-process breakdown would be
        summed away immediately. `BackgroundSolver` caches the aggregate (and
        the supply column behind it) per background process identity, so
        repeated occurrences of the same process cost nothing.
        """
        if self.background_solver is None:
            raise ValueError(
                "Temporal markets need a BackgroundSolver; none was passed to "
                "DynamicBiosphereBuilder. TimexLCA.lci() builds one."
            )
        return self.background_solver.unit_aggregate(act)

    def collect_background_demands(self):
        """Plan the background unit LCIs the matrix build will ask for.

        Walks the temporal-markets branch of `build_dynamic_biosphere_matrix`
        with the same row guards, but without solving anything. `TimexLCA`
        uses the activity ids to pre-factorize (via `BackgroundSolver.prepare`)
        those blocks that several pending solves would otherwise factorize -
        or `spsolve` - one at a time.

        Returns
        -------
        dict
            Time-mapped temporal market id -> `{background activity id:
            coefficient}`. When building from the timeline, several rows can
            share a market and each get their own column; their coefficients
            are summed here, which is all planning needs - only the activity
            ids matter, and they are the same either way.
        """
        demands = {}
        for row in self.timeline.itertuples():
            idx = row.time_mapped_producer
            if idx not in self.node_collections["temporal_markets"]:
                continue
            if self._expand_technosphere:
                if idx in demands:
                    # All rows of one market share a column, so they share a
                    # demand; the build looks at the first row only.
                    continue
                demand = self.demand_from_technosphere(idx, self.activity_dict[idx])
            else:
                if row.Index in self.collapsed_market_rows:
                    continue
                demand = self.demand_from_timeline(row)
            if not demand:
                continue
            target = demands.setdefault(idx, {})
            for act, amount in demand.items():
                target[act] = target.get(act, 0.0) + amount
        return demands

    def get_background_lci_cache_key(self, act):
        """Build a stable cache key for background unit LCI reuse."""
        mapping = self.activity_time_mapping.reversed.get(act)
        if mapping is None:
            return ("activity_id", act)

        process_key, _ = mapping
        if isinstance(process_key, tuple):
            db, code = process_key
            if db == "temporalized":
                return ("temporalized", code)
            if (
                act in self.node_collections["temporalized_processes"]
                or act in self.node_collections["temporal_markets"]
            ):
                # A time-explicit copy of a background process, or a market
                # standing in for one, carries the *original* process key - so
                # a `("db_code", ...)` key would name the original node, whose
                # unit LCI is a different column of a different matrix. Those
                # copies are per-run, so they stay instance-local.
                return ("activity_id", act)
            # Include the background database's `modified` token so edits to
            # that database invalidate stale globally-cached unit LCIs.
            modified = bd.databases[db].get("modified") if db in bd.databases else None
            # Include the current bw2data project — activity / bioflow ids
            # are project-scoped, so a triplet cached under one project
            # must not be reused under another.
            return ("db_code", bd.projects.current, db, code, modified)

        return ("activity_id", act)
