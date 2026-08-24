"""Per-time-step background solving.

When the dynamic inventory keeps no activity dimension, every temporal market
row landing at the same point in time is summed into the same column anyway.
The background demands of those rows can therefore be summed *before* the
solve rather than after - `sum_r B A^-1 d_r` becomes `B A^-1 sum_r d_r` - which
trades one solve per background process for one solve per (time, block) pair.

Which of the two is cheaper depends on the model, so `lci()` counts both and
takes the smaller.
"""

from datetime import datetime

import bw2data as bd
import numpy as np
import pytest
from loguru import logger as loguru_logger

import bw_timex
from bw_timex import TimexLCA

DATABASE_DATES = {
    "db_2020": datetime.strptime("2020", "%Y"),
    "db_2030": datetime.strptime("2030", "%Y"),
    "db_2040": datetime.strptime("2040", "%Y"),
    "foreground": "dynamic",
}


def _tlca():
    bw_timex.clear_background_lci_cache()
    tlca = TimexLCA(
        demand={bd.get_node(database="foreground", code="EV").key: 1},
        method=("GWP", "example"),
        database_dates=DATABASE_DATES,
    )
    tlca.build_timeline(starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"))
    return tlca


def _block_solve_counts(solver, builder):
    """How many grouped solves each block receives."""
    counts = {}
    for demand in builder.collect_background_demands_by_time().values():
        for block in {solver.block_index_for(act) for act in demand}:
            counts[block] = counts.get(block, 0) + 1
    return counts


def _rows_by_flow_and_date(tlca):
    """Dynamic inventory keyed by what a row *means*, not where it sits."""
    return {
        (flow, np.datetime64(date)): float(tlca.dynamic_inventory[index].sum())
        for (flow, date), index in tlca.biosphere_time_mapping.items()
    }


@pytest.mark.usefixtures("vehicle_db")
class TestAggregateForDemand:
    """`BackgroundSolver.aggregate_for_demand` solves a combined demand.

    A temporal market interpolates between vintages that live in *different*
    background databases, i.e. different diagonal blocks, so a combined demand
    has to be split per block and the block aggregates summed.
    """

    def test_matches_the_sum_of_scaled_unit_aggregates(self):
        tlca = _tlca()
        tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        solver = tlca._background_solver
        builder = tlca.dynamic_biosphere_builder

        demands = builder.collect_background_demands()
        demand = next(d for d in demands.values() if len(d) > 1)

        expected = sum(
            solver.unit_aggregate(act) * amount for act, amount in demand.items()
        )
        assert np.allclose(solver.aggregate_for_demand(demand), expected)

    def test_spans_several_blocks(self):
        # Guard the reason this method exists: if every demand sat in one
        # block, a plain `unit_aggregate` would do.
        tlca = _tlca()
        tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        solver = tlca._background_solver
        builder = tlca.dynamic_biosphere_builder

        demands = builder.collect_background_demands()
        spanning = [
            d
            for d in demands.values()
            if len({solver.block_index_for(act) for act in d}) > 1
        ]
        assert spanning, "fixture no longer has a market interpolating across vintages"

        for demand in spanning:
            expected = sum(
                solver.unit_aggregate(act) * amount for act, amount in demand.items()
            )
            assert np.allclose(solver.aggregate_for_demand(demand), expected)

    def test_one_solve_per_block_touched(self):
        tlca = _tlca()
        tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        solver = tlca._background_solver
        builder = tlca.dynamic_biosphere_builder

        demand = next(
            d
            for d in builder.collect_background_demands().values()
            if len({solver.block_index_for(act) for act in d}) > 1
        )
        blocks = {solver.block_index_for(act) for act in demand}

        before = solver.n_solves
        solver.aggregate_for_demand(demand)

        assert solver.n_solves - before == len(blocks)


@pytest.mark.usefixtures("vehicle_db")
class TestGroupedBuildMatchesUngrouped:

    def test_dynamic_inventory_is_unchanged(self):
        ungrouped = _tlca()
        ungrouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=False,
        )

        grouped = _tlca()
        grouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )

        assert grouped.dynamic_biosphere_builder.group_background_by_time is True
        assert ungrouped.dynamic_biosphere_builder.group_background_by_time is False
        assert grouped.dynamic_inventory.shape == ungrouped.dynamic_inventory.shape

        # Compared by (flow, date) identity, not by row position: the grouped
        # build emits its background entries after the timeline loop, so
        # `biosphere_time_mapping` hands out row ids in a different order. The
        # rows themselves - which flow at which time, carrying what - are the
        # invariant, and the raw array is not.
        left = _rows_by_flow_and_date(ungrouped)
        right = _rows_by_flow_and_date(grouped)
        assert set(left) == set(right)
        for key in left:
            assert right[key] == pytest.approx(left[key], rel=1e-9, abs=1e-9)

    def test_score_is_unchanged(self):
        ungrouped = _tlca()
        ungrouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=False,
        )
        ungrouped.static_lcia()

        grouped = _tlca()
        grouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )
        grouped.static_lcia()

        assert grouped.static_score == pytest.approx(ungrouped.static_score, rel=1e-12)

    def test_emission_timing_is_unchanged(self):
        ungrouped = _tlca()
        ungrouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=False,
        )
        grouped = _tlca()
        grouped.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )

        left = ungrouped.dynamic_inventory_df.groupby(["flow", "date"])["amount"].sum()
        right = grouped.dynamic_inventory_df.groupby(["flow", "date"])["amount"].sum()
        assert set(left.index) == set(right.index)
        for key in left.index:
            assert right[key] == pytest.approx(left[key], rel=1e-12, abs=1e-12)


@pytest.mark.usefixtures("vehicle_db")
class TestGroupingIsGated:

    def test_not_used_when_the_activity_dimension_is_kept(self):
        # With one column per emitting activity, summing the rows that share a
        # time step would destroy the attribution those columns exist for.
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=True,
            group_background_by_time=True,
        )

        assert tlca.dynamic_biosphere_builder.group_background_by_time is False

    def test_not_used_with_an_expanded_technosphere(self):
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=True,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )

        assert tlca.dynamic_biosphere_builder.group_background_by_time is False

    def test_not_chosen_once_the_cache_is_warm(self):
        # Grouped right-hand sides are sums specific to one run, so grouping
        # caches nothing. Once a per-process run has filled the unit-LCI cache
        # there are zero pending solves, and nothing can beat that - so the
        # adaptive choice must drop grouping.
        first = _tlca()
        first.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=False,
        )

        warm = TimexLCA(
            demand={bd.get_node(database="foreground", code="EV").key: 1},
            method=("GWP", "example"),
            database_dates=DATABASE_DATES,
        )
        warm.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        warm.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
        )

        assert warm.dynamic_biosphere_builder.group_background_by_time is False
        assert warm._background_solver.n_solves == 0

    def test_grouping_does_not_warm_the_unit_lci_cache(self):
        # A grouped right-hand side is a sum over timeline rows, with no stable
        # identity to key a cache on. Worth pinning: it means a grouped run
        # does not make the next run cheaper, which is the trade grouping makes
        # for needing fewer solves in the first place.
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )

        assert tlca._background_solver.n_solves > 0
        assert tlca._background_solver.shared_cache == {}

    def test_grouped_solves_reuse_a_factorization(self):
        # Every time step solves the same handful of blocks again, so the LU
        # has to be bought once and reused - otherwise each grouped solve is a
        # fresh spsolve on a full background block, which is far more
        # expensive than the per-process solves grouping is meant to replace.
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
            group_background_by_time=True,
        )

        solver = tlca._background_solver
        builder = tlca.dynamic_biosphere_builder
        repeated = {
            block
            for block, count in _block_solve_counts(solver, builder).items()
            if count > 1
        }
        assert repeated, "fixture no longer solves any block more than once"
        assert repeated <= solver.factorized_blocks

    @pytest.mark.parametrize(
        "expand_technosphere, keep_activity_dimension",
        [(True, False), (False, True)],
    )
    def test_an_explicit_request_that_cannot_be_honoured_warns(
        self, expand_technosphere, keep_activity_dimension
    ):
        # Silently ignoring an explicit `group_background_by_time=True` would
        # leave the caller thinking they got it.
        messages = []
        handler = loguru_logger.add(lambda m: messages.append(str(m)), level="WARNING")
        try:
            tlca = _tlca()
            tlca.lci(
                expand_technosphere=expand_technosphere,
                build_dynamic_biosphere=True,
                keep_activity_dimension=keep_activity_dimension,
                group_background_by_time=True,
            )
        finally:
            loguru_logger.remove(handler)

        assert tlca.dynamic_biosphere_builder.group_background_by_time is False
        assert any("group_background_by_time" in message for message in messages)

    def test_an_honoured_request_does_not_warn(self):
        messages = []
        handler = loguru_logger.add(lambda m: messages.append(str(m)), level="WARNING")
        try:
            tlca = _tlca()
            tlca.lci(
                expand_technosphere=False,
                build_dynamic_biosphere=True,
                keep_activity_dimension=False,
                group_background_by_time=True,
            )
        finally:
            loguru_logger.remove(handler)

        assert tlca.dynamic_biosphere_builder.group_background_by_time is True
        assert not any("group_background_by_time" in m for m in messages)

    def test_auto_is_the_default(self):
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
        )
        builder = tlca.dynamic_biosphere_builder
        solver = tlca._background_solver

        by_market, by_time = builder.collect_background_demand_plan()
        per_process = len({solver.cache_key(a) for d in by_market.values() for a in d})
        grouped = len(
            {
                (time, solver.block_index_for(act))
                for time, demand in by_time.items()
                for act in demand
            }
        )
        assert builder.group_background_by_time == (grouped < per_process)

    def test_planning_walks_the_timeline_once(self):
        # Comparing the two strategies needs both groupings of the same walk.
        # Collecting them separately walks the timeline - and re-derives every
        # row's demand - twice, which on a premise-sized model costs more than
        # the grouping saves.
        tlca = _tlca()
        tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        builder = tlca.dynamic_biosphere_builder

        by_market, by_time = builder.collect_background_demand_plan()

        assert by_market == builder.collect_background_demands()
        assert by_time == builder.collect_background_demands_by_time()

    def test_the_cheaper_strategy_is_the_one_chosen(self):
        # The rule itself, rather than an outcome that depends on the fixture:
        # group exactly when it needs strictly fewer solves.
        tlca = _tlca()
        tlca.lci(
            expand_technosphere=False,
            build_dynamic_biosphere=True,
            keep_activity_dimension=False,
        )
        builder = tlca.dynamic_biosphere_builder
        solver = tlca._background_solver

        per_process = len(
            {
                solver.cache_key(act)
                for demand in builder.collect_background_demands().values()
                for act in demand
            }
        )
        grouped = len(
            {
                (time, solver.block_index_for(act))
                for time, demand in builder.collect_background_demands_by_time().items()
                for act in demand
            }
        )

        assert builder.group_background_by_time == (grouped < per_process)
