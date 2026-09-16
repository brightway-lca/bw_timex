"""Batched, multiple-RHS background solving."""

import bw2data as bd
import numpy as np
import pytest
import scipy.sparse as sp

from bw_timex.solvers import iterative_solver_enabled, select_backend

from .test_background_solver import (  # noqa: F401
    _setup,
    chained_background_activities_db,
    chained_background_two_step_materials_db,
    two_background_activities_db,
)


def _pardiso_active():
    return select_backend() == "pardiso"


def _phase_spy(monkeypatch):
    """Record the Pardiso phase of every `_call_pardiso`, returning the list.

    `pypardiso.spsolve` runs `solver.factorize(A)` - phase 12, one
    `_call_pardiso` - whenever the matrix it is handed differs from the one
    currently in MKL's single global slot, then solves with phase 33. So a
    phase-12 (or 13, when factorization is skipped) entry is one full
    analyse-and-factorise, and counting them counts thrash directly.

    Also forgets whatever an *earlier* test last left factorized in that
    global slot. `pypardiso`'s cache is keyed on matrix content, not test
    boundaries, and plenty of tests solve a trivial single-activity `[[1.0]]`
    block - so without this, a phase count here could silently come out one
    short simply because some previous test happened to leave the exact same
    content behind, not because this test's code failed to re-factorize.
    """
    import pypardiso
    from pypardiso.pardiso_wrapper import PyPardisoSolver

    pypardiso.ps.remove_stored_factorization()

    phases = []
    original = PyPardisoSolver._call_pardiso

    def spy(self, A, b):
        phases.append(self.phase)
        return original(self, A, b)

    monkeypatch.setattr(PyPardisoSolver, "_call_pardiso", spy)
    return phases


def _numeric_factorizations(phases):
    return [phase for phase in phases if phase in (12, 13)]


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
        """The invariant the docstring claims: the two persistent dense
        buffers of one chunk fit inside `max_batch_bytes`.

        Measured, not recomputed. Restating `chunk_size`'s own arithmetic and
        asserting the two agree validates neither the formula nor the budget -
        both sides move together under any change. `per_column` here is the
        real size of one column of the buffers `_solve_and_cache_chunk`
        actually allocates: a `(n_columns, k)` supply and a
        `(n_biosphere_rows, k)` aggregate.
        """
        _, _, solver = _setup()
        per_column = (
            np.zeros((solver.technosphere_matrix.shape[1], 1)).nbytes
            + np.zeros((solver.biosphere_matrix.shape[0], 1)).nbytes
        )

        for budget in (
            per_column,
            per_column * 2,
            per_column * 3 + 7,
            per_column * 40,
            per_column * 40 - 1,
        ):
            solver.max_batch_bytes = budget
            chunk = solver.chunk_size()

            assert chunk >= 1
            assert chunk * per_column <= budget
            # ... and it is the largest chunk that fits, so the budget is
            # actually spent rather than merely respected.
            assert (chunk + 1) * per_column > budget

        # A budget too small for even one column still yields a runnable
        # batch - a chunk of zero could never make progress.
        solver.max_batch_bytes = 1
        assert solver.chunk_size() == 1

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


@pytest.fixture
def lu_block_solver(monkeypatch):
    """Pin a test to the LU path.

    The tests below count MKL factorization phases. Blocks are solved by
    Neumann series by default, which factorizes nothing, so those counts are
    all zero unless the series is switched off - and a test whose subject
    never runs passes for the wrong reason.
    """
    monkeypatch.setenv("BW_TIMEX_NO_ITERATIVE_SOLVER", "1")
    assert not iterative_solver_enabled()


@pytest.mark.skipif(not _pardiso_active(), reason="pardiso backend not active")
@pytest.mark.usefixtures("two_background_activities_db")
class TestPardisoIsNotEnteredOnTheDefaultPath:
    """What pardiso does when the series is available: nothing.

    The counts below are the point of the iterative solver on the backend
    where a factorization is cheapest to reach - if MKL is still entered on
    an ordinary run, the series is not doing the work.
    """

    def test_a_default_run_makes_no_mkl_calls(self, monkeypatch):
        phases = _phase_spy(monkeypatch)

        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        solver.prepare([c1.id, c2.id])

        assert solver.n_solves == 1
        assert phases == []
        assert {s.name for s in solver._block_solvers.values()} == {"iterative"}

    def test_the_answer_is_the_one_pardiso_gives(self, monkeypatch):
        c1 = bd.get_node(database="db_2020", code="C1")

        _, structure, iterative = _setup()
        from_series = iterative.unit_supply(c1.id).values

        monkeypatch.setenv("BW_TIMEX_NO_ITERATIVE_SOLVER", "1")
        _, _, direct = _setup()
        assert direct.backend_name == "pardiso"
        from_pardiso = direct.unit_supply(c1.id).values

        assert np.allclose(from_series, from_pardiso, rtol=1e-10, atol=0)


@pytest.mark.skipif(not _pardiso_active(), reason="pardiso backend not active")
@pytest.mark.usefixtures("two_background_activities_db", "lu_block_solver")
class TestPardisoDoesNotRefactorize:
    """MKL Pardiso keeps exactly one factorization alive, so alternating
    blocks re-analyses and re-factorizes on every hop. `_call_pardiso` runs
    with phase 12 or 13 when a numeric factorization happens and phase 33
    when an existing one is reused, which makes thrash directly countable.

    Necessary but NOT sufficient on their own: `two_background_activities_db`
    puts C1 and C2 in the same `db_2020` block, so there is only one block
    here and alternation is impossible by construction - these counts hold
    however badly a multi-block path thrashes. `TestPardisoAcrossTwoBlocks`
    below is where that is actually tested.
    """

    def test_batched_prepare_factorizes_once_per_block(self, monkeypatch):
        phases = _phase_spy(monkeypatch)

        _, _, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        solver.prepare([c1.id, c2.id])

        factorizations = [p for p in phases if p in (12, 13)]
        # One block touched, one chunk -> exactly one numeric factorization,
        # however many activities the batch carries.
        assert len(factorizations) == 1

    def test_factorizations_do_not_scale_with_activity_count(self, monkeypatch):
        from pypardiso.pardiso_wrapper import PyPardisoSolver

        counts = []
        original = PyPardisoSolver._call_pardiso

        def run(ids):
            phases = []

            def spy(self, A, b):
                phases.append(self.phase)
                return original(self, A, b)

            monkeypatch.setattr(PyPardisoSolver, "_call_pardiso", spy)
            _, _, solver = _setup()
            solver.prepare(ids)
            return len([p for p in phases if p in (12, 13)])

        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        counts.append(run([c1.id]))
        counts.append(run([c1.id, c2.id] * 10))

        assert counts[0] == counts[1]


@pytest.mark.skipif(not _pardiso_active(), reason="pardiso backend not active")
@pytest.mark.usefixtures("chained_background_two_step_materials_db", "lu_block_solver")
class TestPardisoAcrossTwoBlocks:
    """The same phase counting, on a fixture that actually has two blocks.

    `glider` lives in `db_parts` and cascades into `steel` in `db_materials`,
    so a cascade genuinely hops between two different matrices and MKL's
    single global factorization slot can be made to thrash.

    This deliberately does *not* reuse `chained_background_activities_db`:
    that fixture gives both `db_parts` and `db_materials` the exact same
    trivial `[[1.0]]` submatrix (one activity, one unit production
    exchange), and pypardiso's factorization cache
    (`PyPardisoSolver._is_already_factorized`) compares matrices by content
    (`indptr`/`indices`/`data`), not by which block `bw_timex` thinks they
    are. Two content-identical blocks never actually force a re-factorization
    when alternated between, so a count taken there could not tell correct
    behaviour from thrash. `chained_background_two_step_materials_db` gives
    `db_materials` a second activity (`coke`, consumed by `steel`), making it
    a 2x2 block - structurally different from `db_parts`'s 1x1 block, not
    merely different by a float - so the two blocks below are genuinely
    distinguishable to pypardiso's cache.

    The batch order is `[steel, glider]` on purpose. With `[glider, steel]`
    the cascade happens to leave the steel block factorized at the end of the
    first chunk, so the second chunk reuses it and chunking looks free -
    which would make the assertion below unable to distinguish a batched run
    from a thrashing one.
    """

    def _nodes(self):
        return (
            bd.get_node(database="db_materials", code="steel"),
            bd.get_node(database="db_parts", code="glider"),
        )

    def test_one_chunk_factorizes_each_block_exactly_once(self, monkeypatch):
        phases = _phase_spy(monkeypatch)

        _, _, solver = _setup()
        steel, glider = self._nodes()
        assert solver.block_index_for(steel.id) != solver.block_index_for(glider.id)

        solver.prepare([steel.id, glider.id])

        # Two blocks - the 1x1 `db_parts` block and the 2x2 `db_materials`
        # block, genuinely different content - one chunk carrying both
        # columns: the cascade enters each block once, so each is analysed
        # and factorized exactly once.
        assert len(_numeric_factorizations(phases)) == 2

    def test_chunking_costs_a_refactorization_per_revisit(self, monkeypatch):
        phases = _phase_spy(monkeypatch)

        _, _, solver = _setup()
        solver.max_batch_bytes = 1  # one column per chunk
        assert solver.chunk_size() == 1
        steel, glider = self._nodes()

        solver.prepare([steel.id, glider.id])

        # Chunk 1 (`steel`) factorizes the steel block. Chunk 2 (`glider`)
        # factorizes the glider block, then cascades back into steel - whose
        # factorization the glider solve has just evicted from MKL's single
        # slot - so steel is factorized a second time. Three, not two: the
        # cost of revisiting a block with a separate right-hand side, which
        # is exactly what one chunk carrying every column avoids.
        assert len(_numeric_factorizations(phases)) == 3


@pytest.mark.usefixtures("chained_background_activities_db")
class TestGroupedSolvesReuseOneFactorizationPerBlock:
    """`aggregate_for_demand` is called once per time step by the grouped
    background path, each call a fresh 1-D cascade across every block the
    demand touches. That is the access pattern a single global factorization
    slot is worst at, so `prepare_blocks` must hand those blocks a solver
    that owns its own factorization - on every backend, pardiso included.
    """

    def _demand(self, amount=1.0):
        steel = bd.get_node(database="db_materials", code="steel")
        glider = bd.get_node(database="db_parts", code="glider")
        return {steel.id: amount, glider.id: amount}

    def _prepared_solver(self, n_time_steps):
        _, _, solver = _setup()
        demand = self._demand()
        blocks = [solver.block_index_for(activity_id) for activity_id in demand]
        assert len(set(blocks)) == 2, "fixture must span two blocks"
        # What `TimexLCA._prepare_grouped_blocks` does: every time step names
        # the blocks its demand touches, so a revisited block is counted more
        # than once and gets pre-factorized.
        solver.prepare_blocks(blocks * n_time_steps)
        return solver

    def test_factorize_block_builds_a_solver_that_owns_its_factorization(self):
        # Backend-agnostic: the object `prepare_blocks` leaves behind must not
        # be one whose "factorization" lives in a shared global slot.
        solver = self._prepared_solver(3)

        assert len(solver.factorized_blocks) == 2
        for block_index in solver.factorized_blocks:
            assert solver._block_solvers[block_index].name != "pardiso"

    def test_repeated_grouped_solves_reuse_the_prepared_solvers(self):
        # Backend-agnostic counterpart of the pardiso phase count below: no
        # new solver object may be built by any number of grouped solves.
        solver = self._prepared_solver(3)
        prepared = dict(solver._block_solvers)
        assert prepared

        for _ in range(3):
            solver.aggregate_for_demand(self._demand())

        assert set(solver._block_solvers) == set(prepared)
        for block_index, block_solver in prepared.items():
            assert solver._block_solvers[block_index] is block_solver

    def test_repeated_grouped_solves_stay_numerically_correct(self):
        # The reuse above is only worth having if it still gives the right
        # answer: a stale or wrongly-backed factorization would not.
        lca, _, _ = _setup()
        solver = self._prepared_solver(3)
        co2_row = lca.dicts.biosphere[bd.get_node(database="bio", code="CO2").id]

        # 1 steel (3 kg CO2) + 1 glider (2 steel -> 6 kg CO2) = 9 kg CO2.
        for _ in range(3):
            aggregate = solver.aggregate_for_demand(self._demand())
            assert aggregate[co2_row] == pytest.approx(9.0)

    @pytest.mark.skipif(not _pardiso_active(), reason="pardiso backend not active")
    @pytest.mark.usefixtures("lu_block_solver")
    def test_factorizations_do_not_grow_with_the_number_of_grouped_solves(
        self, monkeypatch
    ):
        """The regression this whole fix exists for.

        Before it, `_factorize_block` was a no-op on pardiso - it stored a CSR
        and nothing else - so every one of T time steps re-analysed and
        re-factorized each of the B blocks its demand reached: T*B full MKL
        factorizations where the pre-branch per-block SuperLU LU cost B.
        Counting the phases for two different T is what makes that visible;
        a single T could not tell T*B from B.
        """

        def factorizations_for(n_time_steps):
            phases = _phase_spy(monkeypatch)
            solver = self._prepared_solver(n_time_steps)
            for _ in range(n_time_steps):
                solver.aggregate_for_demand(self._demand())
            return len(_numeric_factorizations(phases))

        assert factorizations_for(10) == factorizations_for(2)


@pytest.mark.usefixtures("two_background_activities_db")
class TestBlockSolversAreReused:

    def test_each_block_builds_one_solver_across_chunks(self, monkeypatch):
        """Checking `block_index not in solver._block_solvers` before
        delegating only proves the dict gained an entry - a regression that
        rebuilds the backend on every call but still stores it under
        `block_index` (dropping reuse, keeping the write) is invisible to
        that check, since the key is present from the first call onward.

        Guard the real thing instead: `make_block_solver` is where a
        factorization actually happens, so spy on the name as imported into
        `bw_timex.background_solver` (not on `bw_timex.solvers`, where the
        patch would not be seen) and count calls directly; and capture the
        memoized backend object after every chunk, asserting it is the same
        object throughout rather than merely present.
        """
        import bw_timex.background_solver as background_solver_module

        _, _, solver = _setup()
        solver.max_batch_bytes = 1  # one column per chunk
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        block_index = solver.block_index_for(c1.id)
        assert solver.block_index_for(c2.id) == block_index
        assert solver.chunk_size() == 1

        calls = []
        original_make_block_solver = background_solver_module.make_block_solver

        def spy(backend, submatrix, **kwargs):
            calls.append(submatrix)
            return original_make_block_solver(backend, submatrix, **kwargs)

        monkeypatch.setattr(background_solver_module, "make_block_solver", spy)

        snapshots = []
        original_solve_and_cache_chunk = solver._solve_and_cache_chunk

        def snapshotting(activity_ids):
            original_solve_and_cache_chunk(activity_ids)
            snapshots.append(solver._block_solvers.get(block_index))

        solver._solve_and_cache_chunk = snapshotting

        solver.prepare([c1.id, c2.id])

        # Two chunks, one shared block: the actual factorization call must
        # happen exactly once for this block, no matter how many chunks
        # touch it.
        assert len(calls) == 1
        assert calls[0] is solver._submatrix(block_index)

        # And the memoized backend object must be the SAME object after
        # every chunk that touched the block - the identity check a
        # rebuild-but-still-store regression cannot satisfy.
        assert len(snapshots) == 2  # two pending activities, chunk size 1
        assert snapshots[0] is not None
        assert all(obj is snapshots[0] for obj in snapshots)
