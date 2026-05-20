import bw2data as bd
import numpy as np
import pandas as pd
from bw2calc import LCA
from bw_temporalis import TemporalDistribution
from scipy import sparse as sp

from .helper_classes import SetList
from .utils import convert_date_string_to_datetime, get_temporal_evolution_factor


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
        background_unit_lci_cache: dict | None = None,
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

        # Cached background unit LCIs are stored as structure-independent
        # triplets (bioflow_id, bg_activity_id, amount); when consumed the
        # matrix is rebuilt to *this* lca_obj's biosphere/technosphere
        # index space (see `_rebuild_unit_lci`).
        self._expand_technosphere = bool(expand_technosphere)

        if expand_technosphere:
            self.technosphere_matrix = (
                lca_obj.technosphere_matrix.tocsc()
            )  # convert to csc as this is only used for column slicing
            self.dynamic_supply_array = lca_obj.supply_array
            self.activity_dict = lca_obj.dicts.activity
        else:
            self.dynamic_supply_array = timeline.amount.values.astype(
                float
            )  # get the supply vector directly from the timeline

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
        self._matrix_entries = {}  # (row, col) -> amount
        # Biosphere exchanges of foreground/background producers are read
        # from the bw2data SQL store; share results across TimexLCA objects.
        from ._lci_cache import BIOSPHERE_EXCHANGES_CACHE
        self._activity_biosphere_exchange_cache = BIOSPHERE_EXCHANGES_CACHE
        # Shared/global cache: only stable ("db_code", ...) keys go here so it
        # can safely persist across TimexLCA objects. Stored as
        # structure-independent triplets (bioflow_id, bg_activity_id, amount)
        # so the same entry can be reused across lca_objs with different
        # column/row spaces (different timelines, expand modes, etc.).
        self._background_unit_lci_cache = (
            background_unit_lci_cache if background_unit_lci_cache is not None else {}
        )
        # Per-object cache for keys that are NOT stable across TimexLCA objects
        # (time-mapped activity ids and the per-run "temporalized" database).
        self._instance_unit_lci_cache = {}
        # Within-build cache of rebuilt unit-LCI matrices (sized to *this*
        # lca_obj). Avoids re-materializing the same CSR for repeated calls
        # within one build_dynamic_biosphere_matrix run.
        self._rebuilt_unit_lci_cache = {}
        self.temporal_market_cols = []  # To keep track of temporal market columns

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
        temporal_market_lcis : dict
            A dictionary containing the disaggregated LCI's of the temporal markets,
            with the time-mapped-activity id as key.
        """

        temporal_market_lcis = {}

        for row in self.timeline.itertuples():
            idx = row.time_mapped_producer

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
                    temporal_evolution_factor = get_temporal_evolution_factor(
                        row.temporal_evolution, time_in_datetime
                    )

                for input_id, exc_amount, temporal_distribution in (
                    self.get_biosphere_exchanges(original_db, original_code)
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
                        self.add_matrix_entry_for_biosphere_flows(
                            row=time_mapped_matrix_idx,
                            col=process_col_index,
                            amount=amount,
                        )

            elif idx in self.node_collections["temporal_markets"]:
                self.temporal_market_cols.append(process_col_index)
                (
                    (original_db, original_code),
                    time,
                ) = self.activity_time_mapping.reversed[idx]

                if expand_technosphere:
                    demand = self.demand_from_technosphere(idx, process_col_index)
                else:
                    demand = self.demand_from_timeline(row)

                if demand:
                    for act, amount in demand.items():
                        unit_lci = self.get_background_unit_lci(act)
                        # add lci of both background activities of the temporal market and save total lci
                        if idx not in temporal_market_lcis.keys():
                            temporal_market_lcis[idx] = unit_lci * amount
                        else:
                            temporal_market_lcis[idx] += unit_lci * amount

                    aggregated_inventory = temporal_market_lcis[idx].sum(axis=1)

                    # multiply LCI with supply of temporal market
                    temporal_market_lcis[idx] *= self.dynamic_supply_array[
                        process_col_index
                    ]

                    for row_idx, amount in enumerate(aggregated_inventory.A1):
                        bioflow = self.lca_obj.dicts.biosphere.reversed[row_idx]
                        ((_, _), time) = self.activity_time_mapping.reversed[idx]

                        time_in_datetime = convert_date_string_to_datetime(
                            self.temporal_grouping, str(time)
                        )  # now time is a datetime

                        td_producer = TemporalDistribution(
                            date=np.array([str(time_in_datetime)], dtype=self.time_res),
                            amount=np.array([1]),
                        ).date
                        date = td_producer[0]

                        time_mapped_matrix_idx = self.biosphere_time_mapping.add(
                            (bioflow, date)
                        )

                        self.add_matrix_entry_for_biosphere_flows(
                            row=time_mapped_matrix_idx,
                            col=process_col_index,
                            amount=amount,
                        )

        # now build the dynamic biosphere matrix
        if expand_technosphere:
            ncols = len(self.activity_time_mapping)
        else:
            ncols = len(self.timeline)

        if not self._matrix_entries:
            return sp.csr_matrix((0, ncols)), temporal_market_lcis

        rows = []
        cols = []
        values = []
        for (row, col), amount in self._matrix_entries.items():
            rows.append(row)
            cols.append(col)
            values.append(amount)

        shape = (max(rows) + 1, ncols)
        dynamic_biosphere_matrix = sp.coo_matrix(
            (values, (rows, cols)), shape
        )
        dynamic_biosphere_matrix = dynamic_biosphere_matrix.tocsr()

        return dynamic_biosphere_matrix, temporal_market_lcis

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

    def get_biosphere_exchanges(self, original_db, original_code):
        """Return cached biosphere exchanges for a producer.

        Keyed by the source database's `modified` token so foreground or
        background edits invalidate stale entries automatically.
        """
        modified = (
            bd.databases[original_db].get("modified")
            if original_db in bd.databases
            else None
        )
        cache_key = (bd.projects.current, original_db, original_code, modified)
        if cache_key not in self._activity_biosphere_exchange_cache:
            if original_db == "temporalized":
                act = bd.get_node(code=original_code)
            else:
                act = bd.get_node(database=original_db, code=original_code)
            self._activity_biosphere_exchange_cache[cache_key] = [
                (exc.input.id, exc["amount"], exc.get("temporal_distribution"))
                for exc in act.biosphere()
            ]
        return self._activity_biosphere_exchange_cache[cache_key]

    def get_background_unit_lci(self, act):
        """
        Return unit background LCI matrix for an activity, cached by process identity.

        Background activities can occur repeatedly with different exchange amounts.
        Reusing the unit LCI avoids repeated `redo_lci` solves for equivalent processes.
        """
        cache_key = self.get_background_lci_cache_key(act)
        # Within this build the rebuilt matrix is stable; reuse it.
        if cache_key in self._rebuilt_unit_lci_cache:
            return self._rebuilt_unit_lci_cache[cache_key]

        # Only stable background-process identities may be reused across
        # TimexLCA objects; everything else stays in the per-object cache.
        cache = (
            self._background_unit_lci_cache
            if cache_key[0] == "db_code"
            else self._instance_unit_lci_cache
        )
        if cache_key not in cache:
            self.lca_obj.redo_lci({act: 1})
            cache[cache_key] = self._inventory_to_triplets(self.lca_obj.inventory)
        matrix = self._rebuild_unit_lci(cache[cache_key])
        self._rebuilt_unit_lci_cache[cache_key] = matrix
        return matrix

    def _inventory_to_triplets(self, inv):
        """Convert a CSR inventory matrix to structure-independent triplets.

        Translates row/col indices into stable (bioflow_id, activity_id)
        pairs via the producing lca_obj's dicts, so the cache entry can be
        reused by lca_objs with different index spaces.
        """
        coo = inv.tocoo()
        bio_rev = self.lca_obj.dicts.biosphere.reversed
        act_rev = self.lca_obj.dicts.activity.reversed
        return [
            (bio_rev[r], act_rev[c], v)
            for r, c, v in zip(coo.row, coo.col, coo.data)
        ]

    def _rebuild_unit_lci(self, triplets):
        """Rebuild a unit-LCI CSR sized to *this* lca_obj from cached triplets.

        Entries referring to bioflows or activities not present in the
        current lca_obj are silently skipped. For consumers using the same
        set of databases this never drops anything; for legitimately
        narrower scenarios it correctly excludes out-of-scope entries.
        """
        # On a pure cache hit (no preceding redo_lci on this lca_obj) the
        # technosphere/biosphere matrices and dicts may not have been built
        # yet. Materialize them now.
        if not hasattr(self.lca_obj, "technosphere_matrix"):
            self.lca_obj.load_lci_data()
        bio_dict = self.lca_obj.dicts.biosphere
        act_dict = self.lca_obj.dicts.activity
        rows, cols, vals = [], [], []
        for bid, aid, v in triplets:
            row = bio_dict.get(bid)
            col = act_dict.get(aid)
            if row is None or col is None:
                continue
            rows.append(row)
            cols.append(col)
            vals.append(v)
        n_bio = self.lca_obj.biosphere_matrix.shape[0]
        n_act = self.lca_obj.technosphere_matrix.shape[0]
        return sp.csr_matrix((vals, (rows, cols)), shape=(n_bio, n_act))

    def count_pending_background_solves(self):
        """Count uncached background unit-LCI solves that the matrix build will need.

        Walks the temporal-markets branch of `build_dynamic_biosphere_matrix`
        without performing any `redo_lci` solves — just consults the existing
        cache. TimexLCA uses this to decide whether LU-factorizing the
        technosphere upfront is worth it; factorization only pays off once
        the number of pending solves exceeds the break-even point.
        """
        if not self._expand_technosphere:
            # Non-expand path: demands come from the timeline rows and we
            # cannot cheaply enumerate without effectively running the build.
            # Be conservative and report unknown-many so callers can fall
            # back to the default factorize policy if they care.
            return len(self.node_collections.get("temporal_markets", ()))

        pending_keys = set()
        for row in self.timeline.itertuples():
            idx = row.time_mapped_producer
            if idx not in self.node_collections["temporal_markets"]:
                continue
            process_col_index = self.activity_dict[idx]
            demand = self.demand_from_technosphere(idx, process_col_index)
            if not demand:
                continue
            for act in demand:
                key = self.get_background_lci_cache_key(act)
                cache = (
                    self._background_unit_lci_cache
                    if key[0] == "db_code"
                    else self._instance_unit_lci_cache
                )
                if key in cache or key in pending_keys:
                    continue
                pending_keys.add(key)
        return len(pending_keys)

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
            # Include the background database's `modified` token so edits to
            # that database invalidate stale globally-cached unit LCIs.
            modified = bd.databases[db].get("modified") if db in bd.databases else None
            # Include the current bw2data project — activity / bioflow ids
            # are project-scoped, so a triplet cached under one project
            # must not be reused under another.
            return ("db_code", bd.projects.current, db, code, modified)

        return ("activity_id", act)
