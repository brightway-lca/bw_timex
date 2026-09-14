"""Backend selection and the solver performance warning."""

import warnings

import numpy as np
import pytest
import scipy.sparse as sp

from bw_timex.solvers import (
    SolverPerformanceWarning,
    make_block_solver,
    reset_warning_state,
    select_backend,
    umfpack_available,
    warn_if_suboptimal,
)


@pytest.fixture(autouse=True)
def _fresh_warning_state(monkeypatch):
    monkeypatch.delenv("BW_TIMEX_NO_SOLVER_WARNING", raising=False)
    monkeypatch.delenv("BW_TIMEX_BLOCK_SOLVER", raising=False)
    reset_warning_state()


def test_select_backend_matches_environment_capability():
    # Expectation derived independently of bw_timex's own probes, so this
    # cannot pass by reading the same flag the implementation reads.
    try:
        import pypardiso

        pardiso = pypardiso.pypardiso_solver.libmkl is not None
    except ImportError:
        pardiso = False
    try:
        import scikits.umfpack  # noqa: F401

        umfpack = True
    except ImportError:
        umfpack = False

    expected = "pardiso" if pardiso else "umfpack" if umfpack else "superlu"
    assert select_backend() == expected


@pytest.mark.parametrize("name", ["pardiso", "umfpack", "superlu"])
def test_env_var_forces_backend(monkeypatch, name):
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", name)
    assert select_backend() == name


def test_unknown_forced_backend_raises(monkeypatch):
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", "lapack")
    with pytest.raises(ValueError, match="lapack"):
        select_backend()


@pytest.mark.parametrize("backend", ["pardiso", "umfpack"])
def test_no_warning_when_a_fast_backend_is_active(backend):
    with warnings.catch_warnings():
        warnings.simplefilter("error", SolverPerformanceWarning)
        warn_if_suboptimal(backend, sys_platform="linux", machine="x86_64")


@pytest.mark.parametrize(
    "sys_platform,machine,needle",
    [
        ("darwin", "arm64", "brew install suite-sparse"),
        ("darwin", "x86_64", "brew install suite-sparse"),
        ("linux", "aarch64", "libsuitesparse-dev"),
        ("linux", "x86_64", "PYPARDISO_MKL_RT"),
        ("win32", "AMD64", "PYPARDISO_MKL_RT"),
    ],
)
def test_superlu_warning_names_a_command_that_works_there(
    sys_platform, machine, needle
):
    with pytest.warns(SolverPerformanceWarning, match=needle):
        warn_if_suboptimal("superlu", sys_platform=sys_platform, machine=machine)


def test_windows_arm_warning_recommends_nothing_installable():
    with pytest.warns(SolverPerformanceWarning) as record:
        warn_if_suboptimal("superlu", sys_platform="win32", machine="ARM64")
    message = str(record[0].message)
    assert "suite-sparse" not in message
    assert "PYPARDISO_MKL_RT" not in message


def test_warning_emitted_once_per_process():
    with pytest.warns(SolverPerformanceWarning):
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")
    with warnings.catch_warnings():
        warnings.simplefilter("error", SolverPerformanceWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def test_warning_suppressed_by_env_var(monkeypatch):
    monkeypatch.setenv("BW_TIMEX_NO_SOLVER_WARNING", "1")
    with warnings.catch_warnings():
        warnings.simplefilter("error", SolverPerformanceWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def test_warning_category_is_filterable():
    assert issubclass(SolverPerformanceWarning, UserWarning)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def _pardiso_importable():
    try:
        import pypardiso

        return pypardiso.pypardiso_solver.libmkl is not None
    except ImportError:
        return False


def _available_backends():
    names = ["superlu"]
    if umfpack_available():
        names.append("umfpack")
    if _pardiso_importable():
        names.append("pardiso")
    return names


def _test_matrix():
    return sp.csc_matrix(
        np.array([[4.0, 1.0, 0.0], [1.0, 3.0, 1.0], [0.0, 1.0, 2.0]])
    )


@pytest.mark.parametrize("backend", _available_backends())
def test_backend_solves_a_one_dimensional_rhs(backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([1.0, 2.0, 3.0])

    result = solver.solve(rhs)

    assert result.ndim == 1
    assert np.allclose(matrix @ result, rhs)


@pytest.mark.parametrize("backend", _available_backends())
def test_backend_solves_a_two_dimensional_rhs(backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([[1.0, 0.0, 2.0], [2.0, 1.0, 0.0], [3.0, 1.0, 1.0]])

    result = solver.solve(rhs)

    assert result.shape == (3, 3)
    assert np.allclose(matrix @ result, rhs)


@pytest.mark.parametrize("backend", _available_backends())
def test_two_dimensional_solve_matches_column_by_column(backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([[1.0, 0.0], [2.0, 1.0], [3.0, 1.0]])

    batched = solver.solve(rhs)
    columns = np.column_stack(
        [solver.solve(np.ascontiguousarray(rhs[:, j])) for j in range(rhs.shape[1])]
    )

    assert np.allclose(batched, columns)


@pytest.mark.skipif(not _pardiso_importable(), reason="MKL not available")
def test_pardiso_falls_back_when_a_block_row_is_empty():
    # pypardiso's _check_A raises on an empty row; SuperLU is used instead so
    # one degenerate block cannot fail the whole LCI.
    matrix = sp.csr_matrix(
        np.array([[1.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 1.0]])
    )
    solver = make_block_solver("pardiso", matrix)
    assert solver.name == "superlu"


@pytest.mark.skipif(not _pardiso_importable(), reason="MKL not available")
def test_pardiso_backend_stores_csr_once():
    matrix = _test_matrix()
    solver = make_block_solver("pardiso", matrix)
    first = solver._csr
    solver.solve(np.ones(3))
    solver.solve(np.ones(3))
    assert solver._csr is first
    assert solver._csr.format == "csr"
