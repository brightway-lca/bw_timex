"""Tests for per-block background solving and supply-column caching."""

import bw2data as bd
import numpy as np
import pytest
import scipy.sparse as sp
from bw2calc import LCA
from bw2data.tests import bw2test

from bw_timex.background_solver import BackgroundSolver
from bw_timex.block_structure import BlockStructure


@pytest.fixture
@bw2test
def two_background_activities_db():
    """A self-contained project with *two* background activities in the same
    block, both consumed by one foreground process.

    Kept local to this test module rather than added to
    `dynamic_biomatrix_db_fixture.py`: other tests (`test_lci_cache.py`,
    `test_dynamic_biomatrix_construction.py`) assert on that fixture's exact
    contents. `prepare()`'s `count > 1` factorization branch needs at least
    two pending solves landing in the same block, which the single-activity
    `db_2020` of the shared fixture can never provide.
    """
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2020").write(
        {
            ("db_2020", "C1"): {
                "name": "node c1",
                "location": "somewhere",
                "reference product": "C1",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "C1"),
                    },
                    {
                        "amount": 1.5,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
            ("db_2020", "C2"): {
                "name": "node c2",
                "location": "somewhere",
                "reference product": "C2",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "C2"),
                    },
                    {
                        "amount": 2.5,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "node a",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("db_2020", "C1"),
                    },
                    {
                        "amount": 3,
                        "type": "technosphere",
                        "input": ("db_2020", "C2"),
                    },
                ],
            },
        }
    )

    for db in bd.databases:
        bd.Database(db).register()
        bd.Database(db).process()


def _setup():
    """A plain LCA over the fixture project, split into per-database blocks."""
    node_a = bd.get_node(database="foreground", code="A")
    lca = LCA({node_a: 1})
    lca.load_lci_data()
    lca.build_demand_array()

    n_columns = lca.technosphere_matrix.shape[1]
    column_labels = np.array(
        [bd.get_node(id=lca.dicts.activity.reversed[i])["database"] for i in range(n_columns)]
    )
    n_rows = lca.technosphere_matrix.shape[0]
    row_labels = np.array(
        [bd.get_node(id=lca.dicts.product.reversed[i])["database"] for i in range(n_rows)]
    )
    structure = BlockStructure.detect(
        lca.technosphere_matrix, column_labels, row_labels
    )
    solver = BackgroundSolver(
        technosphere_matrix=lca.technosphere_matrix,
        biosphere_matrix=lca.biosphere_matrix,
        activity_dict=lca.dicts.activity,
        product_dict=lca.dicts.product,
        biosphere_dict=lca.dicts.biosphere,
        structure=structure,
    )
    return lca, structure, solver


def _full_supply(structure, supply):
    block = structure.blocks[supply.block_index]
    full = np.zeros(structure.n_columns)
    full[block.columns] = supply.values
    return full


@pytest.mark.usefixtures("dynamic_biosphere_matrix_db")
class TestBackgroundSolver:

    def test_unit_supply_matches_a_direct_solve(self):
        lca, structure, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        supply = solver.unit_supply(background.id)

        demand = np.zeros(lca.technosphere_matrix.shape[0])
        demand[lca.dicts.product[background.id]] = 1
        expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
        assert np.allclose(_full_supply(structure, supply), expected)

    def test_unit_supply_lands_in_the_background_block(self):
        _, structure, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        supply = solver.unit_supply(background.id)

        assert structure.blocks[supply.block_index].labels == frozenset({"db_2020"})

    def test_unit_aggregate_matches_biosphere_times_supply(self):
        lca, structure, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        aggregate = solver.unit_aggregate(background.id)

        expected = lca.biosphere_matrix @ _full_supply(
            structure, solver.unit_supply(background.id)
        )
        assert np.allclose(aggregate, expected)

    def test_repeated_calls_do_not_solve_again(self):
        _, _, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        solver.unit_supply(background.id)
        after_first = solver.n_solves
        solver.unit_supply(background.id)
        solver.unit_aggregate(background.id)

        assert after_first == 1
        assert solver.n_solves == 1

    def test_shared_cache_is_reused_by_a_second_solver(self):
        shared = {}
        _, structure, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")
        solver.shared_cache = shared
        solver.cache_key = lambda act: ("db_code", act)
        first = solver.unit_supply(background.id)

        _, second_structure, second_solver = _setup()
        second_solver.shared_cache = shared
        second_solver.cache_key = lambda act: ("db_code", act)
        second = second_solver.unit_supply(background.id)

        assert second_solver.n_solves == 0
        assert np.allclose(
            _full_supply(structure, first), _full_supply(second_structure, second)
        )

    def test_cached_payloads_are_one_dimensional(self):
        # Regression guard: caching `B @ diag(x)` is what made `lci()` run out
        # of memory on premise-sized systems.
        shared = {}
        _, _, solver = _setup()
        solver.shared_cache = shared
        solver.cache_key = lambda act: ("db_code", act)
        solver.unit_supply(bd.get_node(database="db_2020", code="C").id)

        assert shared
        for ids, values in shared.values():
            assert ids.ndim == 1
            assert values.ndim == 1

    def test_solve_block_matches_a_direct_solve(self):
        lca, structure, solver = _setup()
        block_index = solver.block_index_for(
            bd.get_node(database="db_2020", code="C").id
        )
        block = structure.blocks[block_index]
        rhs = np.arange(1, len(block.rows) + 1, dtype=float)

        result = solver.solve_block(block_index, rhs)

        expected = sp.linalg.spsolve(
            lca.technosphere_matrix[block.rows][:, block.columns].tocsc(), rhs
        )
        assert np.allclose(result, expected)

    def test_prepare_factorizes_only_blocks_with_several_pending_solves(self):
        _, _, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        solver.prepare([background.id])

        assert solver.factorized_blocks == set()

    def test_prepare_counts_repeated_ids_as_one_pending_solve(self):
        # Every temporal market of the same process demands the same
        # background vintages, so callers hand `prepare` the same id many
        # times. One distinct activity is one solve, and an LU costs roughly
        # a hundred of those - it must not be bought here.
        _, _, solver = _setup()
        background = bd.get_node(database="db_2020", code="C")

        solver.prepare([background.id, background.id, background.id])

        assert solver.factorized_blocks == set()
        assert solver.n_solves == 0


@pytest.mark.usefixtures("two_background_activities_db")
class TestPrepareWithSeveralPendingSolvesInOneBlock:
    """Covers `prepare()`'s `count > 1` branch end-to-end: two background
    activities sharing a block get that block LU-factorized, and the
    subsequent `unit_supply` calls actually go through `solve_block`'s
    cached-LU branch (`solve = self._block_solvers.get(block_index)`) rather
    than the ad-hoc `spsolve` path.
    """

    def test_prepare_factorizes_the_shared_block_without_solving(self):
        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        block_index = solver.block_index_for(c1.id)
        # Sanity: both activities must land in the same block, or this test
        # would not exercise the `count > 1` branch at all.
        assert solver.block_index_for(c2.id) == block_index

        solver.prepare([c1.id, c2.id])

        assert solver.factorized_blocks == {block_index}
        # Factorizing an LU is not the same as solving with it.
        assert solver.n_solves == 0

    def test_supply_after_prepare_matches_a_direct_solve(self):
        lca, structure, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")

        solver.prepare([c1.id, c2.id])
        assert solver.factorized_blocks  # sanity: factorization happened

        for background in (c1, c2):
            supply = solver.unit_supply(background.id)

            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[background.id]] = 1
            expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            assert np.allclose(_full_supply(structure, supply), expected)

        # Both solves went through the cached-LU branch of `solve_block`,
        # not a fresh ad-hoc `spsolve` - still one real solve each.
        assert solver.n_solves == 2
