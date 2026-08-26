"""Temporal-market recipes and the disaggregation they feed.

The builder no longer keeps a `B @ diag(x)` matrix per background activity;
it keeps a *recipe* - `{background activity: coefficient}` plus the market's
supply as a scale - and `disaggregate_background_lci()` materializes the
per-market inventories from it on demand. These tests pin the numbers that
materialization has to reproduce, and that the recipes really are numbers.
"""

from datetime import datetime

import bw2data as bd
import numpy as np
import pytest
import scipy.sparse as sp
from loguru import logger

import bw_timex
from bw_timex import TimexLCA
from bw_timex.helper_classes import TimeMappingDict

from .test_background_traversal import _strip_background_tds

# Captured from the pre-change implementation (`vehicle_db`, EV demand,
# starting 2024-01-02). Disaggregation replaces a market's aggregated
# emissions with the background processes that caused them, so the totals
# and the per-(bioflow, time) row sums must come out unchanged.
BASELINE_TOTAL = 14358.003937894107
BASELINE_ROW_SUMS = [
    5060.68789215088,
    3256.8479339599608,
    5922.000110149384,
    118.46800163388252,
]
BASELINE_SHAPE = (4, 29)
BASELINE_NNZ = 16
BASELINE_MARKET_SUMS = [
    6.467999964952469,
    112.00000166893005,
    514.9759826660156,
    801.2159729003906,
    821.9679718017578,
    1282.8479461669922,
    1919.9039794921873,
    2976.6239730834964,
    5922.000110149384,
]


def _build_tlca():
    electric_vehicle = bd.get_node(database="foreground", code="EV")
    tlca = TimexLCA(
        demand={electric_vehicle.key: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "db_2040": datetime.strptime("2040", "%Y"),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(starting_datetime=datetime(2024, 1, 2))
    tlca.lci()
    return tlca


@pytest.mark.usefixtures("vehicle_db")
class TestSupplyRecipes:

    @pytest.fixture(autouse=True)
    def _clear(self):
        # The fixture rebuilds its databases per test; start from an empty
        # module-level cache so nothing here can hit another test's entries.
        bw_timex.clear_background_lci_cache()
        yield
        bw_timex.clear_background_lci_cache()

    def test_disaggregated_inventory_matches_baseline(self):
        tlca = _build_tlca()
        tlca.static_lcia()
        tlca.disaggregate_background_lci()

        disaggregated = tlca.dynamic_inventory_disaggregated
        assert disaggregated.shape == BASELINE_SHAPE
        assert disaggregated.nnz == BASELINE_NNZ
        assert disaggregated.sum() == pytest.approx(BASELINE_TOTAL, rel=1e-12)
        assert tlca.static_score == pytest.approx(BASELINE_TOTAL, rel=1e-12)
        row_sums = np.asarray(disaggregated.sum(axis=1)).ravel()
        assert row_sums == pytest.approx(BASELINE_ROW_SUMS, rel=1e-12)

    def test_disaggregation_preserves_row_sums_of_the_aggregated_inventory(self):
        # A row is one (biosphere flow, time) pair, so equal row sums mean any
        # dynamic characterization scores the two inventories identically -
        # disaggregation only re-attributes emissions across columns.
        tlca = _build_tlca()
        aggregated_row_sums = np.asarray(tlca.dynamic_inventory.sum(axis=1)).ravel()
        tlca.disaggregate_background_lci()
        disaggregated_row_sums = np.asarray(
            tlca.dynamic_inventory_disaggregated.sum(axis=1)
        ).ravel()
        assert disaggregated_row_sums == pytest.approx(
            aggregated_row_sums, rel=1e-12
        )
        # The DataFrame the dynamic LCIA actually consumes carries the same
        # emissions.
        assert tlca.dynamic_inventory_disaggregated_df.amount.sum() == pytest.approx(
            tlca.dynamic_inventory_df.amount.sum(), rel=1e-12
        )

    def test_recipes_hold_no_matrices(self):
        tlca = _build_tlca()
        builder = tlca.dynamic_biosphere_builder

        assert builder.temporal_market_recipes
        for market_id, recipe in builder.temporal_market_recipes.items():
            assert isinstance(market_id, int)
            assert recipe
            for activity_id, coefficient in recipe.items():
                assert isinstance(activity_id, int)
                assert isinstance(coefficient, float)
        for scale in builder.temporal_market_scales.values():
            assert isinstance(scale, float)

        # Nothing anywhere in the build holds an inventory matrix per
        # background activity any more - that is the whole point.
        assert not any(
            sp.issparse(value)
            for cache in (
                tlca._background_supply_cache,
                tlca._background_aggregate_cache,
            )
            for payload in cache.values()
            for value in payload
        )

    def test_temporal_market_lcis_still_available_after_disaggregation(self):
        tlca = _build_tlca()
        tlca.disaggregate_background_lci()

        market_lcis = tlca.temporal_market_lcis
        assert len(market_lcis) == len(BASELINE_MARKET_SUMS)
        for market_id, lci in market_lcis.items():
            assert sp.issparse(lci)
            assert lci.shape[1] == BASELINE_SHAPE[1]
            assert market_id in tlca.dynamic_biosphere_builder.temporal_market_recipes
        assert sorted(float(lci.sum()) for lci in market_lcis.values()) == pytest.approx(
            BASELINE_MARKET_SUMS, rel=1e-12
        )

    def test_temporal_market_lcis_are_materialized_lazily(self):
        tlca = _build_tlca()
        # `lci()` leaves recipes behind, not matrices; the matrices only appear
        # when something asks for them.
        assert tlca._temporal_market_lcis is None
        assert tlca.temporal_market_lcis
        assert tlca._temporal_market_lcis is not None

    def test_collected_demands_cover_every_market_recipe(self):
        tlca = _build_tlca()
        builder = tlca.dynamic_biosphere_builder
        demands = builder.collect_background_demands()
        assert set(demands) == set(builder.temporal_market_recipes)
        for market_id, demand in demands.items():
            assert demand == pytest.approx(
                builder.temporal_market_recipes[market_id], rel=1e-12
            )

    def test_recipe_entries_in_one_block_are_summed_into_one_supply(self):
        # A market's vintages sit in different blocks, but two activities of
        # the *same* block have to end up sharing one supply column.
        tlca = _build_tlca()
        solver = tlca._background_solver
        builder = tlca.dynamic_biosphere_builder
        market_id, recipe = next(iter(builder.temporal_market_recipes.items()))
        activity_id = next(iter(recipe))
        block_index = solver.block_index_for(activity_id)
        block_node_ids = [
            solver.activity_dict.reversed[column]
            for column in solver.structure.blocks[block_index].columns
        ]
        siblings = [
            node_id for node_id in block_node_ids if node_id in solver.product_dict
        ]
        assert len(siblings) >= 2

        builder.temporal_market_recipes[market_id] = {
            siblings[0]: 2.0,
            siblings[1]: 3.0,
        }
        builder.temporal_market_scales[market_id] = 1.0
        tlca._temporal_market_lcis = None

        expected_supply = 2.0 * solver.unit_supply(siblings[0]).values + (
            3.0 * solver.unit_supply(siblings[1]).values
        )
        columns = solver.structure.blocks[block_index].columns
        expected = solver.biosphere_matrix[:, columns].multiply(
            expected_supply[columns]
        )
        difference = tlca.temporal_market_lcis[market_id][:, columns] - expected
        assert abs(difference).max() == pytest.approx(0.0, abs=1e-12)

    def test_unlabelable_nodes_fall_back_to_a_single_block(self):
        # A technosphere we cannot fully attribute to databases is one we must
        # not split: `detect` has to see a single label and go degenerate.
        tlca = _build_tlca()
        tlca.nodes = {}
        tlca.activity_time_mapping = TimeMappingDict()
        column_labels, row_labels = tlca._technosphere_database_labels()
        assert len(set(column_labels.tolist())) == 1
        assert len(set(row_labels.tolist())) == 1
        assert tlca._build_background_solver().structure.is_degenerate

    def test_one_unlabelable_node_collapses_the_structure_and_warns(self):
        # The fallback is all-or-nothing on purpose, but it costs per-vintage
        # solving, so it has to say so - otherwise a large run is just
        # inexplicably slow.
        tlca = _build_tlca()
        # Give exactly one node a non-tuple process key (the case
        # `get_background_lci_cache_key`'s "activity_id" branch exists for)
        # and hide it from `self.nodes`, so every other node still resolves.
        mapping = TimeMappingDict()
        victim = None
        for (process_key, time), node_id in tlca.activity_time_mapping.items():
            if victim is None:
                victim = node_id
                mapping[("unmappable-process-key", time)] = node_id
            else:
                mapping[(process_key, time)] = node_id
        tlca.activity_time_mapping = mapping
        tlca.nodes = {k: v for k, v in tlca.nodes.items() if k != victim}

        messages = []
        sink_id = logger.add(messages.append, level="WARNING")
        try:
            column_labels, row_labels = tlca._technosphere_database_labels()
        finally:
            logger.remove(sink_id)

        assert len(set(column_labels.tolist())) == 1
        assert len(set(row_labels.tolist())) == 1
        assert tlca._build_background_solver().structure.is_degenerate
        assert len(messages) == 1
        # Exactly one node was unlabelable - this is the *partial* case, not
        # the fully-empty one.
        assert "of 1 technosphere node(s)" in messages[0]
        assert f"node id {victim}" in messages[0]
        assert "single block" in messages[0]

    def test_market_lcis_raise_without_a_dynamic_biosphere(self):
        electric_vehicle = bd.get_node(database="foreground", code="EV")
        tlca = TimexLCA(
            demand={electric_vehicle.key: 1},
            method=("GWP", "example"),
            database_dates={
                "db_2020": datetime.strptime("2020", "%Y"),
                "foreground": "dynamic",
            },
        )
        tlca.build_timeline(starting_datetime=datetime(2024, 1, 2))
        tlca.lci(build_dynamic_biosphere=False)
        with pytest.raises(AttributeError, match="Dynamic biosphere not yet built"):
            tlca.temporal_market_lcis


TRAVERSAL_DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _traversal_tlca(traverse_background):
    """A full run of the background-traversal fixture, disaggregation included."""
    tlca = TimexLCA(
        {("foreground", "fu"): 1}, ("GWP", "example"), TRAVERSAL_DATABASE_DATES
    )
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=traverse_background,
    )
    tlca.lci()
    tlca.static_lcia()
    tlca.disaggregate_background_lci()
    return tlca


def _traversal_results(tlca):
    return {
        "supply": tlca.lca.supply_array.copy(),
        "static_score": tlca.static_score,
        "market_sums": [
            float(matrix.sum())
            for _, matrix in sorted(tlca.temporal_market_lcis.items())
        ],
        # Per-activity attribution: what the disaggregation says each column
        # emitted, which is what a background contribution analysis reads.
        "attribution": np.asarray(
            tlca.dynamic_inventory_disaggregated.sum(axis=0)
        ).ravel(),
    }


@pytest.mark.usefixtures("background_td_db")
def test_time_explicit_copies_do_not_inherit_the_originals_unit_column():
    """Two runs in one session must not share a copy's unit supply column.

    `traverse_background=True` makes time-explicit copies of background
    processes, and a temporal market stands in for one. Both carry the
    *original's* process key, so a `("db_code", ...)` cache key would name the
    original node, whose unit supply is a different column of a different
    matrix - and the cache holding it is shared across `TimexLCA` objects in a
    session. Those copies have to be keyed instance-locally instead, and two
    runs in one session have to agree with the same run cold.
    """
    _strip_background_tds()
    bw_timex.clear_background_lci_cache()
    try:
        # Cold: the traversing run on its own, nothing cached by anything else.
        cold_tlca = _traversal_tlca(True)
        cold = _traversal_results(cold_tlca)
        assert cold["market_sums"], "no temporal market to disaggregate"
        assert cold["static_score"] > 0
        bw_timex.clear_background_lci_cache()

        # Warm: the non-traversing run first - the one that used to leave
        # columns in the cache under keys the traversing run then matched.
        _traversal_tlca(False)
        warm_tlca = _traversal_tlca(True)
        warm = _traversal_results(warm_tlca)
    finally:
        bw_timex.clear_background_lci_cache()

    np.testing.assert_allclose(warm["supply"], cold["supply"], rtol=1e-9)
    assert warm["static_score"] == pytest.approx(cold["static_score"], rel=1e-9)
    np.testing.assert_allclose(warm["market_sums"], cold["market_sums"], rtol=1e-9)
    np.testing.assert_allclose(warm["attribution"], cold["attribution"], rtol=1e-9)

    # The copies themselves: whatever asks the solver for their unit supply
    # must get a key nothing outside this run can match.
    builder = warm_tlca.dynamic_biosphere_builder
    copies = (
        warm_tlca.node_collections["temporal_markets"]
        | warm_tlca.node_collections["temporalized_processes"]
    )
    assert copies, "no time-explicit copies in this run"
    for act in copies:
        assert builder.get_background_lci_cache_key(act)[0] in {
            "activity_id",
            "temporalized",
        }, act
