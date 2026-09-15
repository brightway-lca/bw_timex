"""Backend selection and the solver performance warning."""

import os
import warnings

import numpy as np
import pytest
import scipy.sparse as sp

from bw_timex.solvers import (
    BACKENDS,
    SolverPerformanceWarning,
    make_block_solver,
    make_persistent_block_solver,
    reset_warning_state,
    select_backend,
    umfpack_available,
    warn_if_suboptimal,
)


def _pardiso_importable():
    # `pypardiso` exports its module-global solver as `ps`; `pypardiso_solver`
    # is the name it carries inside `scipy_aliases`, not on the package. Read
    # the wrong one and this raises AttributeError - which, being caught
    # nowhere, fails collection rather than reporting "no pardiso here".
    try:
        import pypardiso

        return pypardiso.ps.libmkl is not None
    except (ImportError, AttributeError):
        return False


def _available_backends():
    names = ["superlu"]
    if umfpack_available():
        names.append("umfpack")
    if _pardiso_importable():
        names.append("pardiso")
    return names


def _unavailable_backends():
    return [name for name in BACKENDS if name not in _available_backends()]


@pytest.fixture
def _fresh_warning_state(monkeypatch):
    """Isolate a test from the ambient solver environment.

    Requested explicitly, never `autouse`: CI runs the whole file with
    `BW_TIMEX_BLOCK_SOLVER` set to each backend in turn, and
    `test_active_backend_is_the_one_ci_expects` exists precisely to check that
    the override took. An autouse fixture stripped it before that assertion
    could see it, so the test compared the machine's natural backend against
    the leg's expectation and every non-natural leg failed.
    """
    monkeypatch.delenv("BW_TIMEX_NO_SOLVER_WARNING", raising=False)
    monkeypatch.delenv("BW_TIMEX_BLOCK_SOLVER", raising=False)
    reset_warning_state()


def test_select_backend_matches_environment_capability(_fresh_warning_state):
    # Expectation derived independently of bw_timex's own probes, so this
    # cannot pass by reading the same flag the implementation reads.
    pardiso = _pardiso_importable()
    try:
        import scikits.umfpack  # noqa: F401

        umfpack = True
    except ImportError:
        umfpack = False

    expected = "pardiso" if pardiso else "umfpack" if umfpack else "superlu"
    assert select_backend() == expected


@pytest.mark.parametrize("name", _available_backends())
def test_env_var_forces_backend(_fresh_warning_state, monkeypatch, name):
    # Parametrized over what this machine actually has, not over the three
    # literal names: a backend the machine cannot provide must now raise
    # rather than be handed back (see the test below), so asserting it is
    # returned would be asserting the bug.
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", name)
    assert select_backend() == name


@pytest.mark.skipif(
    not _unavailable_backends(), reason="every backend is available here"
)
@pytest.mark.parametrize("name", _unavailable_backends())
def test_forcing_an_unavailable_backend_raises(
    _fresh_warning_state, monkeypatch, name
):
    # The silent-degradation failure mode, inside the solver layer: SciPy's
    # `factorized` quietly returns a SuperLU closure when scikits.umfpack is
    # missing, so an unverified override produced an object reporting
    # `.name == "umfpack"` that solved with SuperLU.
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", name)
    with pytest.raises(RuntimeError, match=name):
        select_backend()


@pytest.mark.skipif(
    not _unavailable_backends(), reason="every backend is available here"
)
@pytest.mark.parametrize("name", _unavailable_backends())
def test_make_block_solver_refuses_an_unavailable_backend(_fresh_warning_state, name):
    with pytest.raises(RuntimeError, match=name):
        make_block_solver(name, _test_matrix())


def test_unavailable_backend_error_names_how_to_install_it(
    _fresh_warning_state, monkeypatch
):
    unavailable = _unavailable_backends()
    if not unavailable:
        pytest.skip("every backend is available here")
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", unavailable[0])
    with pytest.raises(RuntimeError) as excinfo:
        select_backend()
    message = str(excinfo.value)
    expected = "pypardiso" if unavailable[0] == "pardiso" else "scikit-umfpack"
    assert expected in message


def test_unknown_forced_backend_raises(_fresh_warning_state, monkeypatch):
    monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", "lapack")
    with pytest.raises(ValueError, match="lapack"):
        select_backend()


@pytest.mark.parametrize("backend", ["pardiso", "umfpack"])
def test_no_warning_when_a_fast_backend_is_active(_fresh_warning_state, backend):
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
    _fresh_warning_state, sys_platform, machine, needle
):
    with pytest.warns(SolverPerformanceWarning, match=needle):
        warn_if_suboptimal("superlu", sys_platform=sys_platform, machine=machine)


def test_windows_arm_warning_recommends_nothing_installable(_fresh_warning_state):
    with pytest.warns(SolverPerformanceWarning) as record:
        warn_if_suboptimal("superlu", sys_platform="win32", machine="ARM64")
    message = str(record[0].message)
    assert "suite-sparse" not in message
    assert "PYPARDISO_MKL_RT" not in message


def test_warning_emitted_once_per_process(_fresh_warning_state):
    with pytest.warns(SolverPerformanceWarning):
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")
    with warnings.catch_warnings():
        warnings.simplefilter("error", SolverPerformanceWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def test_warning_suppressed_by_env_var(_fresh_warning_state, monkeypatch):
    monkeypatch.setenv("BW_TIMEX_NO_SOLVER_WARNING", "1")
    with warnings.catch_warnings():
        warnings.simplefilter("error", SolverPerformanceWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def test_warning_category_is_filterable(_fresh_warning_state):
    assert issubclass(SolverPerformanceWarning, UserWarning)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        warn_if_suboptimal("superlu", sys_platform="darwin", machine="arm64")


def _test_matrix():
    return sp.csc_matrix(
        np.array([[4.0, 1.0, 0.0], [1.0, 3.0, 1.0], [0.0, 1.0, 2.0]])
    )


@pytest.mark.parametrize("backend", _available_backends())
def test_make_block_solver_returns_the_backend_it_was_asked_for(
    _fresh_warning_state, backend
):
    # Without this, nothing asserted that the umfpack backend is umfpack:
    # every backend solves this matrix correctly, so the numeric tests below
    # pass just as happily when `factorized` fell back to SuperLU.
    solver = make_block_solver(backend, _test_matrix())

    assert solver.name == backend


@pytest.mark.parametrize("backend", _available_backends())
def test_backend_solves_a_one_dimensional_rhs(_fresh_warning_state, backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([1.0, 2.0, 3.0])

    result = solver.solve(rhs)

    assert result.ndim == 1
    assert np.allclose(matrix @ result, rhs)


@pytest.mark.parametrize("backend", _available_backends())
def test_backend_solves_a_two_dimensional_rhs(_fresh_warning_state, backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([[1.0, 0.0, 2.0], [2.0, 1.0, 0.0], [3.0, 1.0, 1.0]])

    result = solver.solve(rhs)

    assert result.shape == (3, 3)
    assert np.allclose(matrix @ result, rhs)


@pytest.mark.parametrize("backend", _available_backends())
def test_two_dimensional_solve_matches_column_by_column(_fresh_warning_state, backend):
    matrix = _test_matrix()
    solver = make_block_solver(backend, matrix)
    rhs = np.array([[1.0, 0.0], [2.0, 1.0], [3.0, 1.0]])

    batched = solver.solve(rhs)
    columns = np.column_stack(
        [solver.solve(np.ascontiguousarray(rhs[:, j])) for j in range(rhs.shape[1])]
    )

    assert np.allclose(batched, columns)


@pytest.mark.parametrize("backend", _available_backends())
def test_persistent_block_solver_never_delegates_to_pardiso(
    _fresh_warning_state, backend
):
    # `make_persistent_block_solver` exists to guarantee a factorization the
    # returned object owns. Pardiso's lives in MKL's single global slot, so it
    # is substituted; every other backend already qualifies and is kept.
    matrix = _test_matrix()

    solver = make_persistent_block_solver(matrix, backend)

    assert solver.name != "pardiso"
    assert solver.name == ("umfpack" if backend == "umfpack" else "superlu")
    assert np.allclose(matrix @ solver.solve(np.array([1.0, 2.0, 3.0])), [1.0, 2.0, 3.0])


@pytest.mark.skipif(not _pardiso_importable(), reason="MKL not available")
def test_pardiso_falls_back_when_a_block_row_is_empty(_fresh_warning_state):
    # pypardiso's _check_A raises on an empty row; SuperLU is used instead so
    # one degenerate block cannot fail the whole LCI.
    matrix = sp.csr_matrix(
        np.array([[1.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 1.0]])
    )
    solver = make_block_solver("pardiso", matrix)
    assert solver.name == "superlu"


@pytest.mark.skipif(not _pardiso_importable(), reason="MKL not available")
def test_pardiso_backend_stores_csr_once(_fresh_warning_state):
    matrix = _test_matrix()
    solver = make_block_solver("pardiso", matrix)
    first = solver._csr
    solver.solve(np.ones(3))
    solver.solve(np.ones(3))
    assert solver._csr is first
    assert solver._csr.format == "csr"


@pytest.mark.skipif(
    "BW_TIMEX_EXPECT_SOLVER" not in os.environ,
    reason="BW_TIMEX_EXPECT_SOLVER not set; only enforced in CI",
)
def test_active_backend_is_the_one_ci_expects():
    # Without this, a failed pypardiso resolution or a missing mkl_rt
    # degrades silently to SuperLU and the suite still goes green - the
    # exact failure mode this whole change exists to fix.
    #
    # Takes no `_fresh_warning_state`, deliberately: CI's cross-backend leg
    # sets BW_TIMEX_BLOCK_SOLVER and BW_TIMEX_EXPECT_SOLVER to the same
    # backend, and this is the assertion that the override was honoured.
    # Clearing the environment first would test the opposite.
    assert select_backend() == os.environ["BW_TIMEX_EXPECT_SOLVER"]
