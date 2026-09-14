"""Batched, multiple-RHS background solving."""

import bw2data as bd
import numpy as np
import pytest
import scipy.sparse as sp

from .test_background_solver import (  # noqa: F401
    _setup,
    chained_background_activities_db,
    two_background_activities_db,
)


@pytest.mark.usefixtures("two_background_activities_db")
class TestCascadingSolveWithMatrixRHS:

    def test_matrix_cascade_matches_column_by_column(self):
        lca, structure, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        block_index = solver.block_index_for(c1.id)
        block = structure.blocks[block_index]

        seed = np.zeros((len(block.rows), 2))
        seed[solver._local_row(block, c1.id), 0] = 1.0
        seed[solver._local_row(block, c2.id), 1] = 1.0

        batched, touched = solver._cascading_solve({block_index: seed})

        assert batched.shape == (lca.technosphere_matrix.shape[1], 2)
        for j, node in enumerate((c1, c2)):
            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[node.id]] = 1
            expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            assert np.allclose(batched[:, j], expected)

    def test_one_dimensional_cascade_still_returns_a_vector(self):
        lca, structure, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        block_index = solver.block_index_for(c1.id)
        block = structure.blocks[block_index]
        seed = np.zeros(len(block.rows))
        seed[solver._local_row(block, c1.id)] = 1.0

        values, _ = solver._cascading_solve({block_index: seed})

        assert values.ndim == 1


@pytest.mark.usefixtures("two_background_activities_db")
class TestPrepareSolvesTheBatch:

    def test_prepare_fills_the_supply_cache(self):
        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        solver.prepare([c1.id, c2.id])

        for node in (c1, c2):
            assert solver.cache_key(node.id) in solver._instance_supply_cache

    def test_prepare_fills_the_aggregate_cache(self):
        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")

        solver.prepare([c1.id])

        assert solver.cache_key(c1.id) in solver._instance_aggregate_cache

    def test_unit_supply_after_prepare_adds_no_solves(self):
        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        solver.prepare([c1.id, c2.id])
        after_prepare = solver.n_solves
        solver.unit_supply(c1.id)
        solver.unit_supply(c2.id)

        assert solver.n_solves == after_prepare

    def test_prepared_results_match_a_direct_solve(self):
        lca, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        solver.prepare([c1.id, c2.id])

        for node in (c1, c2):
            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[node.id]] = 1
            expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            assert np.allclose(solver.unit_supply(node.id).values, expected)

    def test_chunking_does_not_change_results(self):
        _, _, unchunked = _setup()
        _, _, chunked = _setup()
        chunked.max_batch_bytes = 1  # forces chunk_size() == 1
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        unchunked.prepare([c1.id, c2.id])
        chunked.prepare([c1.id, c2.id])

        assert chunked.chunk_size() == 1
        for node in (c1, c2):
            assert np.allclose(
                unchunked.unit_supply(node.id).values,
                chunked.unit_supply(node.id).values,
            )

    def test_chunk_size_respects_the_memory_budget(self):
        _, _, solver = _setup()
        n_columns = solver.technosphere_matrix.shape[1]
        n_biosphere_rows = solver.biosphere_matrix.shape[0]
        per_column = (n_columns + n_biosphere_rows) * 8
        solver.max_batch_bytes = per_column * 4

        assert solver.chunk_size() == 4

    def test_prepare_solves_each_block_once_per_chunk(self):
        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        solver.prepare([c1.id, c2.id])

        # Two activities, one shared block, one chunk -> one solve call
        # carrying two right-hand side columns.
        assert solver.n_solves == 1
        assert solver.n_rhs_solved == 2

    def test_solve_calls_do_not_scale_with_activity_count(self):
        _, _, two = _setup()
        _, _, repeated = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        two.prepare([c1.id, c2.id])
        repeated.prepare([c1.id, c2.id] * 10)

        assert repeated.n_solves == two.n_solves


@pytest.mark.usefixtures("chained_background_activities_db")
class TestPrepareBatchLivenessAcrossBlocks:
    """`_cascading_solve`'s `live` mask picks, per block, which columns of a
    batch actually have a nonzero right-hand side there (`np.any(rhs != 0,
    axis=0)`). The `two_background_activities_db` fixture used above always
    produces a 2x2 seed in a single block, where reducing over the wrong axis
    happens to read back the same answer - so it cannot catch a transposed
    axis.

    `chained_background_activities_db` can: `glider` (`db_parts`) has no
    biosphere flow of its own and cascades entirely into `steel`
    (`db_materials`), a second, separate block. A batch of
    `[glider, steel]` puts two columns through that second block with
    genuinely different right-hand sides - column 0 arrives only via the
    cascade (amount 2, from `glider`'s 2x steel exchange), column 1 is
    steel's own direct unit demand (amount 1) - so a wrong axis silently
    drops or conflates a column instead of merely handing back a coincidentally
    correct 2x2 result.
    """

    def test_prepared_supply_matches_independent_solves(self):
        lca, _, solver = _setup()
        glider = bd.get_node(database="db_parts", code="glider")
        steel = bd.get_node(database="db_materials", code="steel")

        solver.prepare([glider.id, steel.id])

        for node in (glider, steel):
            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[node.id]] = 1
            expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            assert np.allclose(solver.unit_supply(node.id).values, expected)

    def test_prepared_aggregate_matches_independent_solves(self):
        # `glider`'s own block emits nothing - its whole footprint comes from
        # `_solve_and_cache_chunk`'s `for block_index in touched_blocks`
        # aggregate sum pulling in the downstream `steel` block. Nothing
        # else in this suite calls `unit_aggregate` after a `prepare()` that
        # spans both blocks, so a regression in that summing loop would slip
        # through unnoticed without this test.
        lca, _, solver = _setup()
        glider = bd.get_node(database="db_parts", code="glider")
        steel = bd.get_node(database="db_materials", code="steel")

        solver.prepare([glider.id, steel.id])

        for node, expected_co2 in ((glider, 6.0), (steel, 3.0)):
            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[node.id]] = 1
            expected_supply = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            expected_aggregate = np.asarray(
                lca.biosphere_matrix @ expected_supply
            ).ravel()

            aggregate = solver.unit_aggregate(node.id)

            assert np.allclose(aggregate, expected_aggregate)
            co2 = bd.get_node(database="bio", code="CO2")
            assert aggregate[lca.dicts.biosphere[co2.id]] == pytest.approx(
                expected_co2
            )
