"""
Characterisation of a bw_timex time-explicit inventory with the `edges` package.

`edges` (https://edges.readthedocs.io) characterises *exchanges* rather than flows, and its
characterisation factors can be symbolic expressions evaluated per scenario year. bw_timex knows
when each process runs and when each emission occurs, so the two combine into characterisation
factors evaluated at each exchange's own year.
"""

from __future__ import annotations

import bw2data as bd

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
            self.biosphere_edges = set(zip(*lca.inventory.nonzero()))

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
