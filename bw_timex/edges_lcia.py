"""
Characterisation of a bw_timex time-explicit inventory with the `edges` package.

`edges` (https://edges.readthedocs.io) characterises *exchanges* rather than flows, and its
characterisation factors can be symbolic expressions evaluated per scenario year. bw_timex knows
when each process runs and when each emission occurs, so the two combine into characterisation
factors evaluated at each exchange's own year.
"""

from __future__ import annotations

import bw2data as bd
import numpy as np
import pandas as pd
from loguru import logger

from .utils import round_datetime_series_to_year, year_from_time_mapped_timestamp

EDGES_IMPORT_ERROR = (
    "The `edges` package is required for TimexLCA.edges_lcia(). Install it with "
    "`pip install bw_timex[edges]`. Note that edges supports Python >=3.10,<3.13, while "
    "bw_timex itself also runs on 3.13 - on 3.13 the extra installs nothing."
)

try:
    from edges import EdgeLCIA
    from edges.matrix_builders import build_technosphere_edges_matrix
    from edges.utils import get_flow_matrix_positions
except ImportError as exc:
    raise ImportError(EDGES_IMPORT_ERROR) from exc

CF_TABLE_COLUMNS = [
    "supplier",
    "supplier categories",
    "consumer",
    "consumer location",
    "activity",
    "direction",
    "date",
    "year",
    "amount",
    "CF",
    "impact",
]


class TimexEdgeLCIA(EdgeLCIA):
    """
    `edges.EdgeLCIA` subclass that understands bw_timex's time-explicit matrices.

    Two things differ from plain `edges`:

    1. The columns of the expanded technosphere matrix are time-mapped ids (from
       `TimexLCA.activity_time_mapping`) rather than Brightway ids, so flow metadata is resolved
       through that mapping instead of through `bw2data`.
    2. Characterisation factors are evaluated per year, using each exchange's own year.

    Parameters
    ----------
    timex_lca : TimexLCA
        A TimexLCA whose `lci()` has already been calculated with `expand_technosphere=True`.
    method : tuple, str, pathlib.Path or dict
        The edges method, passed through to `edges.EdgeLCIA`.
    **edge_kwargs
        Further keyword arguments for `edges.EdgeLCIA` (`parameters`, `scenario`, `weight`,
        `filepath`, `allowed_functions`).
    """

    def __init__(self, timex_lca, method, **edge_kwargs):
        self.timex_lca = timex_lca
        self.position_to_timestamp = {}
        self.position_to_biosphere_flows_lookup = {}
        super().__init__(
            demand=timex_lca.fu,
            method=method,
            lca=timex_lca.lca,
            **edge_kwargs,
        )

    def lci(self) -> None:
        """
        Collects the exchanges and flow metadata of the time-explicit inventory.

        Replaces `edges.EdgeLCIA.lci`, which would re-solve the already-solved inventory and
        would try to resolve bw_timex's time-mapped ids through `bw2data`.
        """
        lca = self.lca
        if not hasattr(lca, "inventory"):
            raise AttributeError(
                "LCI not yet calculated. Call TimexLCA.lci() before TimexLCA.edges_lcia()."
            )

        self.biosphere_edges = set()
        self.technosphere_edges = set()
        self.biosphere_flows = None
        self.technosphere_flow_matrix = None

        if self._uses_technosphere_supplier_matrix():
            self.technosphere_flow_matrix = build_technosphere_edges_matrix(
                lca.technosphere_matrix, lca.supply_array
            )
            self.technosphere_edges = set(zip(*self.technosphere_flow_matrix.nonzero()))

        if self._uses_biosphere_supplier_matrix():
            self.biosphere_edges = self._biosphere_edge_positions()

        biosphere_dict = lca.dicts.biosphere
        activity_dict = lca.dicts.activity

        unique_biosphere_flows = {edge[0] for edge in self.biosphere_edges}
        if unique_biosphere_flows:
            self.biosphere_flows = get_flow_matrix_positions(
                {
                    key: position
                    for key, position in biosphere_dict.items()
                    if position in unique_biosphere_flows
                }
            )

        self.technosphere_flows = self._translate_time_mapped_activities(activity_dict)

        self.reversed_activity = {v: k for k, v in activity_dict.items()}
        self.reversed_biosphere = {v: k for k, v in biosphere_dict.items()}

        self.position_to_technosphere_flows_lookup = {
            flow["position"]: {k: v for k, v in flow.items() if k != "position"}
            for flow in self.technosphere_flows
        }
        self.position_to_biosphere_flows_lookup = {
            flow["position"]: {k: v for k, v in flow.items() if k != "position"}
            for flow in (self.biosphere_flows or [])
        }

        self._base_supplier_lookup_bio = None
        self._base_supplier_lookup_tech = None
        self._base_consumer_lookup = None
        self._flows_version = (
            len(self.biosphere_flows) if self.biosphere_flows else 0,
            len(self.technosphere_flows),
        )

    def _biosphere_edge_positions(self) -> set:
        """
        Collects the `(biosphere position, technosphere position)` pairs that CFs are matched for.

        The two inventories of a TimexLCA do not agree on which column an emission belongs to:
        the static inventory keeps the emissions of a temporal market on the original background
        process, while `DynamicBiosphereBuilder` aggregates them onto the time-mapped market
        column. Since `characterize_time_explicit` reads the dynamic inventory, its pairs have to
        be matched as well, so the union of both patterns is used. Pairs that never carry an
        amount simply end up as unused cells of the characterisation matrix.

        Returns
        -------
        set
            Matrix positions of the biosphere exchanges to be matched.
        """
        edges = set(zip(*self.lca.inventory.nonzero()))

        timex = self.timex_lca
        if not hasattr(timex, "dynamic_inventory"):
            return edges

        coo = timex.dynamic_inventory.tocoo()
        if coo.nnz == 0:
            return edges

        reversed_time_mapping = timex.biosphere_time_mapping.reversed
        biosphere_positions = self.lca.dicts.biosphere
        row_to_position = {
            row: biosphere_positions[reversed_time_mapping[row][0]] for row in np.unique(coo.row)
        }
        edges.update((row_to_position[row], column) for row, column in zip(coo.row, coo.col))
        return edges

    def _translate_time_mapped_activities(self, activity_dict) -> list:
        """
        Builds edges' flow-metadata dicts for the columns of the expanded technosphere matrix.

        Each column is a (process, time) combination, so several columns can share one underlying
        Brightway node. edges keys its lookups by matrix position, so repeated metadata is fine.

        Parameters
        ----------
        activity_dict : dict
            `lca.dicts.activity`, mapping time-mapped ids to matrix positions.

        Returns
        -------
        list
            One flow-metadata dict per matrix position, in the shape `edges` expects from
            `get_flow_matrix_positions`.
        """
        reversed_mapping = self.timex_lca.activity_time_mapping.reversed
        node_cache = {}
        flows = []

        for time_mapped_id, position in activity_dict.items():
            key, timestamp = reversed_mapping[time_mapped_id]
            if key not in node_cache:
                database, code = key
                # bw_timex gives every activity it time-resolves a copy under the synthetic
                # "temporalized" database, which does not exist in bw2data - only the original
                # activity, under its real database, does. Its `code` is unchanged, so look it up
                # by code alone, as bw_timex itself does elsewhere (e.g.
                # `DynamicBiosphereBuilder.get_biosphere_exchanges`).
                if database == "temporalized":
                    node_cache[key] = bd.get_node(code=code)
                else:
                    node_cache[key] = bd.get_node(database=database, code=code)
            node = node_cache[key]

            flows.append(
                {
                    "name": node.get("name"),
                    "reference product": node.get("reference product"),
                    "categories": node.get("categories"),
                    "unit": node.get("unit"),
                    "location": node.get("location"),
                    "classifications": node.get("classifications"),
                    "type": node.get("type"),
                    "position": position,
                }
            )
            self.position_to_timestamp[position] = timestamp

        return flows

    def run_mapping(self, regionalized: bool = True) -> None:
        """
        Matches the exchanges of the time-explicit inventory to the method's characterisation
        factors. Matching does not depend on the year, so it runs once for the whole timeline.

        Parameters
        ----------
        regionalized : bool
            If True, runs edges' location-mapping cascade after the direct matching, which fills
            aggregate, dynamic ("RoW"), contained and global regions. Default is True.

        Returns
        -------
        None
        """
        self.map_exchanges()

        if regionalized:
            self.map_aggregate_locations()
            self.map_dynamic_locations()
            self.map_contained_locations()
            self.map_remaining_locations_to_global()

    def characterize_time_explicit(self, use_disaggregated_lci: bool = False) -> pd.DataFrame:
        """
        Characterises the time-explicit inventory, evaluating each exchange's characterisation
        factor at that exchange's own year.

        Biosphere exchanges are taken from the dynamic inventory, so emissions spread by a
        temporal distribution are characterised at the year they actually occur. Technosphere
        exchanges are characterised at the vintage of the consuming process, since bw_timex has no
        dynamic technosphere inventory.

        Parameters
        ----------
        use_disaggregated_lci : bool
            If True, uses the disaggregated dynamic inventory of the background. Default is False.

        Returns
        -------
        pandas.DataFrame
            One row per characterised exchange, with the date and year used for its CF. Columns
            are `supplier`, `supplier categories`, `consumer`, `consumer location`, `activity`,
            `direction`, `date`, `year`, `amount`, `CF` and `impact`.
        """
        slices = {}
        biosphere_entries = self._biosphere_entries(use_disaggregated_lci)
        if biosphere_entries is not None:
            slices["biosphere"] = biosphere_entries
        technosphere_entries = self._technosphere_entries()
        if technosphere_entries is not None:
            slices["technosphere"] = technosphere_entries

        years = sorted({year for entries in slices.values() for year in entries["year"].unique()})

        tables = []
        for year in years:
            self.evaluate_cfs(scenario_idx=str(year))
            for matrix_type, entries in slices.items():
                cf_matrix = self.characterization_matrices.get(matrix_type)
                if cf_matrix is None:
                    continue
                in_year = entries[entries["year"] == year]
                if in_year.empty:
                    continue
                tables.append(self._characterize_entries(in_year, cf_matrix.tocsr(), matrix_type))

        if not tables:
            logger.warning("No exchanges were characterized. The edges score is 0.")
            self.score = 0.0
            return pd.DataFrame(columns=CF_TABLE_COLUMNS)

        table = pd.concat(tables, ignore_index=True).sort_values(
            by=["date", "impact"], ascending=[True, False]
        )
        table = table[table["CF"] != 0].reset_index(drop=True)
        self.score = float(table["impact"].sum())
        return table

    def _characterize_entries(
        self, entries: pd.DataFrame, cf_matrix, matrix_type: str
    ) -> pd.DataFrame:
        """
        Looks up the CF of each entry in one year's characterisation matrix.

        Parameters
        ----------
        entries : pandas.DataFrame
            Inventory entries of a single year, as returned by `_biosphere_entries` or
            `_technosphere_entries`.
        cf_matrix : scipy.sparse.csr_matrix
            The characterisation matrix `edges` evaluated for that year.
        matrix_type : str
            Either "biosphere" or "technosphere".

        Returns
        -------
        pandas.DataFrame
            One row per entry, with the CF and the resulting impact.
        """
        rows = entries["row"].to_numpy()
        columns = entries["column"].to_numpy()
        cfs = np.asarray(cf_matrix[rows, columns]).ravel()

        supplier_lookup = (
            self.position_to_biosphere_flows_lookup
            if matrix_type == "biosphere"
            else self.position_to_technosphere_flows_lookup
        )
        direction = (
            "biosphere-technosphere" if matrix_type == "biosphere" else "technosphere-technosphere"
        )
        amounts = entries["amount"].to_numpy()

        return pd.DataFrame(
            {
                "supplier": [supplier_lookup[row].get("name") for row in rows],
                "supplier categories": [supplier_lookup[row].get("categories") for row in rows],
                "consumer": [
                    self.position_to_technosphere_flows_lookup[column].get("name")
                    for column in columns
                ],
                "consumer location": [
                    self.position_to_technosphere_flows_lookup[column].get("location")
                    for column in columns
                ],
                "activity": [self.reversed_activity[column] for column in columns],
                "direction": direction,
                "date": entries["date"].to_numpy(),
                "year": entries["year"].to_numpy(),
                "amount": amounts,
                "CF": cfs,
                "impact": amounts * cfs,
            }
        )

    def _biosphere_entries(self, use_disaggregated_lci: bool):
        """
        Returns the non-zero entries of the dynamic inventory as a DataFrame with the matrix
        positions and the year that edges should evaluate the CF at.

        The dynamic inventory is used rather than `lca.inventory`, because only it keeps the
        emission dates produced by temporal distributions on biosphere exchanges: its rows are
        `(flow, date)` pairs, while the static inventory has exactly one cell per
        `(flow, process)` and could therefore only carry a single year per process.

        Parameters
        ----------
        use_disaggregated_lci : bool
            If True, uses the disaggregated dynamic inventory of the background.

        Returns
        -------
        pandas.DataFrame or None
            Columns `row`, `column`, `amount`, `date` and `year`, or None if there is nothing to
            characterise.
        """
        if not self._uses_biosphere_supplier_matrix():
            return None

        timex = self.timex_lca
        if not hasattr(timex, "dynamic_inventory"):
            raise AttributeError(
                "Dynamic inventory not yet calculated. Call TimexLCA.lci() with "
                "build_dynamic_biosphere=True before characterizing with edges."
            )
        if use_disaggregated_lci and not hasattr(timex, "dynamic_inventory_disaggregated"):
            timex.disaggregate_background_lci()

        inventory = (
            timex.dynamic_inventory_disaggregated
            if use_disaggregated_lci
            else timex.dynamic_inventory
        )
        coo = inventory.tocoo()
        if coo.nnz == 0:
            return None

        reversed_time_mapping = timex.biosphere_time_mapping.reversed
        biosphere_positions = self.lca.dicts.biosphere

        resolved = {row: reversed_time_mapping[row] for row in np.unique(coo.row)}
        row_to_position = {row: biosphere_positions[flow] for row, (flow, _) in resolved.items()}
        row_to_date = {row: date for row, (_, date) in resolved.items()}

        dates = pd.Series(np.array([row_to_date[row] for row in coo.row], dtype="datetime64[s]"))

        return pd.DataFrame(
            {
                "row": [row_to_position[row] for row in coo.row],
                "column": coo.col,
                "amount": coo.data,
                "date": dates,
                "year": round_datetime_series_to_year(dates).dt.year,
            }
        )

    def _technosphere_entries(self):
        """
        Returns the non-zero entries of the technosphere flow matrix, dated by the vintage of the
        consuming process. There is no dynamic technosphere inventory, so this is the finest
        resolution available for technosphere characterisation factors.

        Returns
        -------
        pandas.DataFrame or None
            Columns `row`, `column`, `amount`, `date` and `year`, or None if there is nothing to
            characterise.
        """
        if self.technosphere_flow_matrix is None:
            return None

        coo = self.technosphere_flow_matrix.tocoo()
        if coo.nnz == 0:
            return None

        temporal_grouping = self.timex_lca.temporal_grouping
        year_by_column = {
            column: year_from_time_mapped_timestamp(
                self.position_to_timestamp[column], temporal_grouping
            )
            for column in np.unique(coo.col)
        }

        resolved = np.array([year_by_column[column] is not None for column in coo.col], dtype=bool)
        unresolved_count = int((~resolved).sum())
        if unresolved_count:
            logger.warning(
                f"{unresolved_count} technosphere exchanges have no resolved process time "
                f"(timestamp 'dynamic') and are not characterized."
            )
        if not resolved.any():
            return None

        years = np.array([year_by_column[column] for column in coo.col[resolved]], dtype=int)
        dates = pd.Series(np.array([f"{year}-01-01" for year in years], dtype="datetime64[s]"))

        return pd.DataFrame(
            {
                "row": coo.row[resolved],
                "column": coo.col[resolved],
                "amount": coo.data[resolved],
                "date": dates,
                "year": dates.dt.year,
            }
        )
