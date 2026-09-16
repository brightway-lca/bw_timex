"""The iterative block solver, and the LU fallback behind it.

Pins the two things that make the substitution safe: the answer is the LU
answer, and a block the series cannot handle still gets solved.
"""

import numpy as np
import pytest
import scipy.sparse as sp

from bw_timex.solvers import (
    BACKENDS,
    make_block_solver,
    make_persistent_block_solver,
    select_backend,
)


def _leontief_block(n=40, seed=0):
    """`I - T` with column sums of `T` below one, as a real block has."""
    rng = np.random.default_rng(seed)
    dense = np.zeros((n, n))
    for column in range(n):
        rows = rng.choice([r for r in range(n) if r != column], size=3, replace=False)
        dense[rows, column] = -rng.uniform(0.05, 0.2, size=3)
    np.fill_diagonal(dense, 1.0)
    return sp.csc_matrix(dense)


def _lu_reference(matrix, rhs):
    from scipy.sparse.linalg import splu

    return splu(matrix.tocsc()).solve(rhs)


def _assert_matches(result, reference, tol=1e-9):
    """Assert every column matches the LU answer to `tol`, scaled by that
    column's largest entry."""
    columns = result if result.ndim == 2 else result.reshape(-1, 1)
    expected = reference if reference.ndim == 2 else reference.reshape(-1, 1)
    scale = np.abs(expected).max(axis=0)
    assert np.all(np.abs(columns - expected).max(axis=0) <= tol * scale)


def test_iterative_solver_is_used_when_allowed():
    matrix = _leontief_block()

    solver = make_block_solver(select_backend(), matrix, allow_iterative=True)

    assert solver.name == "iterative"


def test_default_still_returns_the_named_backend():
    # `allow_iterative` is opt-in: naming a backend returns that backend.
    backend = select_backend()

    assert make_block_solver(backend, _leontief_block()).name == backend


def test_iterative_result_matches_lu_for_a_one_dimensional_rhs():
    matrix = _leontief_block()
    rhs = np.zeros(matrix.shape[0])
    rhs[7] = 1.0

    result = make_block_solver(select_backend(), matrix, allow_iterative=True).solve(
        rhs
    )

    assert result.ndim == 1
    reference = _lu_reference(matrix, rhs)
    _assert_matches(result, reference)


def test_iterative_result_matches_lu_for_a_two_dimensional_rhs():
    # Columns 15 orders of magnitude apart: a batch-wide convergence
    # criterion would let the large one declare the small one converged.
    matrix = _leontief_block()
    rhs = np.zeros((matrix.shape[0], 3))
    rhs[7, 0] = 1.0
    rhs[11, 1] = 1e-9
    rhs[3, 2] = 1e6

    result = make_block_solver(select_backend(), matrix, allow_iterative=True).solve(
        rhs
    )

    assert result.shape == (matrix.shape[0], 3)
    _assert_matches(result, _lu_reference(matrix, rhs))


def test_a_solution_far_larger_than_its_demand_is_still_accepted():
    # One unit of demand pulling 1e12 units upstream: a real functional unit
    # does this (a vehicle demanding 20,000 kWh). The residual such a solve
    # leaves is large next to the demand and tiny next to the solution, so
    # acceptance has to be judged against the system, not against `b`.
    # Column 0 pulls 1e6 of product 1, and products 1 and 2 feed each other,
    # so the series ends on its tolerance rather than exactly.
    matrix = sp.csc_matrix(
        np.array(
            [
                [1.0, 0.0, 0.0],
                [-1e6, 1.0, -0.5],
                [0.0, -0.5, 1.0],
            ]
        )
    )
    rhs = np.array([1.0, 0.0, 0.0])

    solver = make_block_solver(select_backend(), matrix, allow_iterative=True)
    result = solver.solve(rhs)

    assert not solver.fell_back
    _assert_matches(result, _lu_reference(matrix, rhs))


def test_a_block_whose_jacobi_iteration_diverges_falls_back_to_lu():
    # Nonsingular, but `I - D^-1 A` has spectral radius 4: the series runs away.
    matrix = sp.csc_matrix(np.array([[1.0, 4.0], [4.0, 1.0]]))
    rhs = np.array([1.0, 2.0])

    solver = make_block_solver(select_backend(), matrix, allow_iterative=True)
    result = solver.solve(rhs)

    assert solver.fell_back
    assert np.allclose(matrix @ result, rhs)


def test_a_block_with_a_zero_diagonal_falls_back_to_lu():
    # No Jacobi preconditioner without a full diagonal.
    matrix = sp.csc_matrix(np.array([[0.0, 1.0], [1.0, 0.0]]))
    rhs = np.array([1.0, 2.0])

    solver = make_block_solver(select_backend(), matrix, allow_iterative=True)

    assert solver.name in BACKENDS
    assert np.allclose(matrix @ solver.solve(rhs), rhs)


def test_an_empty_block_row_is_still_rejected():
    # The singular-block diagnosis must survive the new path.
    matrix = sp.csr_matrix(np.array([[1.0, 0.0], [0.0, 0.0]]))

    with pytest.raises(ValueError, match="singular"):
        make_block_solver(select_backend(), matrix, allow_iterative=True)


def test_env_var_disables_the_iterative_path(monkeypatch):
    monkeypatch.setenv("BW_TIMEX_NO_ITERATIVE_SOLVER", "1")
    backend = select_backend()

    solver = make_block_solver(backend, _leontief_block(), allow_iterative=True)

    assert solver.name == backend


def test_persistent_solver_can_be_iterative_too():
    # The grouped path revisits a block once per time step, so its solver
    # must not re-analyse on every hop.
    matrix = _leontief_block()
    rhs = np.zeros(matrix.shape[0])
    rhs[5] = 1.0

    solver = make_persistent_block_solver(matrix, allow_iterative=True)

    assert solver.name == "iterative"
    _assert_matches(solver.solve(rhs), _lu_reference(matrix, rhs))


def test_iteration_matrix_is_built_once_across_solves():
    matrix = _leontief_block()
    solver = make_block_solver(select_backend(), matrix, allow_iterative=True)
    rhs = np.zeros(matrix.shape[0])
    rhs[5] = 1.0

    solver.solve(rhs)
    first = solver._iteration_matrix
    solver.solve(rhs)

    assert solver._iteration_matrix is first


@pytest.mark.usefixtures("dynamic_biosphere_matrix_db")
class TestBackgroundSolverUsesTheIterativePath:
    """The substitution, pinned where it runs rather than at the factory."""

    def test_unit_supply_is_identical_with_and_without_iteration(self):
        from tests.test_background_solver import _full_supply, _setup

        background_id = _background_id()

        _, structure, iterative = _setup()
        _, _, direct = _setup()
        direct.allow_iterative = False

        assert iterative.allow_iterative is True
        from_iteration = _full_supply(structure, iterative.unit_supply(background_id))
        from_lu = _full_supply(structure, direct.unit_supply(background_id))

        assert np.allclose(from_iteration, from_lu, rtol=1e-10, atol=0)

    def test_the_block_solver_it_builds_is_the_iterative_one(self):
        from tests.test_background_solver import _setup

        _, _, solver = _setup()
        solver.unit_supply(_background_id())

        assert {s.name for s in solver._block_solvers.values()} == {"iterative"}


def _background_id():
    import bw2data as bd

    return bd.get_node(database="db_2020", code="C").id
