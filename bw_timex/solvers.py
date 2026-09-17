"""Sparse solvers for per-block background solving.

`bw_timex` solves one diagonal block of the technosphere at a time, many
times over. Blocks are solved iteratively (`_IterativeBlockSolver`, a
Jacobi-preconditioned Neumann series, no factorization); the LU backends
below are the fallback for blocks that series cannot handle, and what a
caller gets when it does not pass `allow_iterative`.

Three LU backends, in preference order:

- `pardiso` (Intel MKL, via `pypardiso`): the only one that takes a 2-D
  right-hand side into a single native call with `nrhs > 1`.
- `umfpack` (`scikits.umfpack`, through SciPy's `factorized`): a persistent
  LU per block, but its closure rejects a 2-D RHS, so columns are looped.
- `superlu` (SciPy's built-in): `splu` gives a persistent LU and takes a 2-D
  RHS, and is always available.

Pardiso's advantage is confined to the batched path. Its factorization lives
in MKL's single global slot, so a caller that alternates between blocks with
separate right-hand sides re-factorizes on every hop; such callers ask for
`make_persistent_block_solver` instead, which guarantees a per-object LU.
"""

import glob
import os
import platform
import sys
import warnings
from ctypes.util import find_library
from typing import Optional

import numpy as np

BACKENDS = ("pardiso", "umfpack", "superlu")

# A column of the Neumann series stops when its increment is this small
# relative to the column itself. Terms keep shrinking geometrically - each one
# is computed from the previous, so there is no roundoff floor to stall on -
# and 1e-15 buys agreement with an LU solve to ~1e-14 for ~25 extra terms.
ITERATIVE_RTOL = 1e-15

# Largest backward error, per column, a converged series may leave before the
# block is handed to an LU backend instead:
# `||A x - b|| / (||A|| ||x|| + ||b||)`. Scaling by `||b||` alone would reject
# correct solves of any system whose solution dwarfs its demand - a functional
# unit of one vehicle pulling 20,000 kWh does exactly that.
ITERATIVE_RESIDUAL_TOL = 1e-9

# Cap on series terms. Premise-sized blocks converge in ~110.
ITERATIVE_MAX_ITERATIONS = 500

# Consecutive growing increments before the series is called divergent.
ITERATIVE_DIVERGENCE_PATIENCE = 5

_warned = False


class SolverPerformanceWarning(UserWarning):
    """A faster sparse solver could be installed on this machine."""


def reset_warning_state() -> None:
    """Forget that the warning was already emitted. For tests."""
    global _warned
    _warned = False


def pardiso_available() -> bool:
    """True when MKL Pardiso is importable *and* `mkl_rt` actually loaded.

    Probed through `bw2calc`, which performs the same import and catches the
    `ImportError` that `PyPardisoSolver.__init__` raises when it cannot find
    `mkl_rt`. Sharing the probe keeps `bw_timex` and `bw2calc` from
    disagreeing about what is installed.
    """
    try:
        from bw2calc import PYPARDISO
    except ImportError:
        return False
    return bool(PYPARDISO)


def umfpack_available() -> bool:
    """True when `scikits.umfpack` imports.

    Probed by import, not by reading `scipy.sparse.linalg._dsolve.linsolve`'s
    `noScikit` / `useUmfpack`: both are private, and `useUmfpack` is a
    thread-local object in current SciPy.
    """
    try:
        import scikits.umfpack  # noqa: F401
    except ImportError:
        return False
    return True


_INSTALL_HINTS = {
    "pardiso": (
        "install it with `pip install pypardiso` and make sure an `mkl_rt` shared "
        "library is loadable (set PYPARDISO_MKL_RT if it lives somewhere unusual)"
    ),
    "umfpack": (
        "install it with `pip install scikit-umfpack` after the SuiteSparse system "
        'library (`brew install suite-sparse` / `apt install libsuitesparse-dev`), '
        'or `pip install "bw_timex[solvers]"`'
    ),
}


def backend_available(name: str) -> bool:
    """Whether `name`'s underlying library is actually usable on this machine."""
    if name == "pardiso":
        return pardiso_available()
    if name == "umfpack":
        return umfpack_available()
    if name == "superlu":
        return True
    raise ValueError(
        f"Unknown block solver {name!r}; expected one of {', '.join(BACKENDS)}"
    )


def require_backend(name: str) -> str:
    """Return `name`, or raise if the machine cannot actually provide it.

    An unavailable backend must never be returned. SciPy's `factorized`
    silently hands back a SuperLU closure when `scikits.umfpack` is missing,
    so an unverified `umfpack` request produces an object that *reports*
    umfpack while solving with SuperLU - a silent, platform-dependent
    degradation indistinguishable from the bug this module exists to remove.
    """
    if backend_available(name):
        return name
    raise RuntimeError(
        f"Block solver {name!r} was requested but is not available on this "
        f"machine: {_INSTALL_HINTS[name]}."
    )


def select_backend(override: Optional[str] = None) -> str:
    """Name of the block-solver backend to use.

    `override`, else `BW_TIMEX_BLOCK_SOLVER`, else the best available. A
    requested backend is verified, not taken on trust: an unknown name raises
    `ValueError`, a known but unavailable one `RuntimeError`.
    """
    name = override or os.environ.get("BW_TIMEX_BLOCK_SOLVER")
    if name:
        name = name.strip().lower()
        if name not in BACKENDS:
            raise ValueError(
                f"Unknown block solver {name!r}; expected one of {', '.join(BACKENDS)}"
            )
        return require_backend(name)
    if pardiso_available():
        return "pardiso"
    if umfpack_available():
        return "umfpack"
    return "superlu"


# Where a SuiteSparse installed by the usual system package managers puts
# libumfpack. `find_library` alone is not enough on macOS: Homebrew's lib
# directory is not on the default dyld search path, so a perfectly good
# SuiteSparse is invisible to it.
_SUITESPARSE_LIB_DIRS = (
    "/opt/homebrew/opt/suite-sparse/lib",  # Homebrew, Apple Silicon
    "/usr/local/opt/suite-sparse/lib",  # Homebrew, Intel
    "/usr/local/lib",
    "/usr/lib",
    "/usr/lib64",
    "/usr/lib/x86_64-linux-gnu",
    "/usr/lib/aarch64-linux-gnu",
)


def suitesparse_present() -> bool:
    """Whether the SuiteSparse C library looks installed on this system.

    Only used to sharpen the advice below. `scikit-umfpack` is a *binding*
    to this library and a separate install, and conflating the two is the
    likeliest way for someone who has done half the job to be told to do the
    half they already did.
    """
    if find_library("umfpack"):
        return True
    return any(
        glob.glob(os.path.join(directory, "libumfpack*"))
        for directory in _SUITESPARSE_LIB_DIRS
    )


def _install_umfpack_advice(system_install: str) -> str:
    """How to get UMFPACK, given what is already on the machine.

    `system_install` is the command that provides the C library on this
    platform.
    """
    if suitesparse_present():
        return (
            "The SuiteSparse system library is already installed here, but its "
            "Python binding is not - those are two separate installs. Add the "
            'binding with: pip install "bw_timex[solvers]"'
        )
    return (
        "Install UMFPACK - both the system library and its Python binding - with: "
        f'{system_install} && pip install "bw_timex[solvers]"'
    )


def _suboptimal_message(sys_platform: str, machine: str) -> str:
    """What to tell a user stuck on SuperLU, per platform.

    Every branch names something that works on the machine reading it. A
    recommendation that cannot succeed there is worse than staying quiet, and
    one that tells someone to redo a step they have already done is worse
    still - it reads as the advice being wrong rather than incomplete.
    """
    prefix = (
        "bw_timex is solving background systems with SciPy's SuperLU, which is "
        "substantially slower than the alternatives. "
    )
    if sys_platform == "darwin":
        return prefix + _install_umfpack_advice("brew install swig suite-sparse")
    if sys_platform == "linux" and machine != "x86_64":
        return prefix + (
            "Intel MKL has no wheel for this architecture. "
            + _install_umfpack_advice("apt install libsuitesparse-dev")
        )
    if (sys_platform == "linux" and machine == "x86_64") or (
        sys_platform == "win32" and machine == "AMD64"
    ):
        return prefix + (
            "pypardiso is a declared dependency of bw_timex on this platform, so MKL "
            "should have been available but could not be loaded. Point PYPARDISO_MKL_RT "
            "at an mkl_rt shared library, or reinstall the environment."
        )
    return prefix + (
        f"No faster sparse solver is packaged for {sys_platform}/{machine}."
    )


def warn_if_suboptimal(
    backend: str, sys_platform: Optional[str] = None, machine: Optional[str] = None
) -> None:
    """Warn once per process when `backend` is not the best one available.

    Called from `BackgroundSolver.__init__` rather than at import, so
    `import bw_timex` stays quiet and runs that never solve a background
    system never warn.
    """
    global _warned
    if backend != "superlu" or _warned:
        return
    if os.environ.get("BW_TIMEX_NO_SOLVER_WARNING"):
        return
    _warned = True
    if sys_platform is None:
        sys_platform = sys.platform
    if machine is None:
        machine = platform.machine()
    warnings.warn(
        _suboptimal_message(sys_platform, machine),
        SolverPerformanceWarning,
        stacklevel=3,
    )


class _SuperLUBlockSolver:
    """SciPy's built-in SuperLU. Always available; takes a 2-D RHS natively."""

    name = "superlu"

    def __init__(self, submatrix):
        from scipy.sparse.linalg import splu

        self._lu = splu(submatrix.tocsc())

    def solve(self, rhs):
        return self._lu.solve(rhs)


class _UmfpackBlockSolver:
    """UMFPACK through SciPy's `factorized`.

    The closure holds a persistent LU, but rejects a 2-D right-hand side
    (`ValueError: object too deep for desired array`), so a batch is looped
    column by column. Each column is still only a triangular solve against
    the one factorization.
    """

    name = "umfpack"

    def __init__(self, submatrix):
        from scipy.sparse.linalg import factorized

        self._solve_one = factorized(submatrix.tocsc())

    def solve(self, rhs):
        if rhs.ndim == 1:
            return np.asarray(self._solve_one(rhs), dtype=float)
        out = np.empty(rhs.shape, dtype=float)
        for j in range(rhs.shape[1]):
            out[:, j] = np.asarray(self._solve_one(np.ascontiguousarray(rhs[:, j])))
        return out


class _PardisoBlockSolver:
    """MKL Pardiso through `pypardiso`'s module-global solver.

    A 2-D right-hand side goes into one native call with `nrhs = k`, which is
    the whole reason this backend is preferred: one analysis, one numeric
    factorization, k triangular solves, all inside MKL.

    The CSR submatrix is held here because `pypardiso.spsolve` runs
    `A.tocsr()` on every call, which would otherwise be an O(nnz) copy per
    chunk.
    """

    name = "pardiso"

    def __init__(self, csr_submatrix):
        self._csr = csr_submatrix
        if not self._csr.has_sorted_indices:
            self._csr.sort_indices()

    def solve(self, rhs):
        from pypardiso import spsolve as pardiso_spsolve

        result = pardiso_spsolve(self._csr, rhs, squeeze=False)
        result = np.asarray(result, dtype=float)
        if rhs.ndim == 1:
            return result.ravel()
        return result


class _IterativeBlockSolver:
    """A block solved by summing the Jacobi-preconditioned Neumann series
    `A^-1 b = sum_k (I - D^-1 A)^k D^-1 b`, with no factorization.

    On a premise-sized vintage block (43.6k rows, 525k nonzeros) that is
    ~110 sparse matrix products, 20-25 ms per right-hand side, against ~4 s
    for the UMFPACK factorization the block would otherwise need.

    A block with a zero on its diagonal is rejected in `__init__`; one whose
    series diverges or misses `residual_tol` falls back from `solve`. Either
    way `fallback` - a zero-argument callable, so no LU is built unless it is
    needed - provides the solver used from then on.
    """

    name = "iterative"

    def __init__(
        self,
        submatrix,
        fallback,
        rtol: float = ITERATIVE_RTOL,
        residual_tol: float = ITERATIVE_RESIDUAL_TOL,
        max_iterations: int = ITERATIVE_MAX_ITERATIONS,
    ):
        import scipy.sparse as sp

        matrix = submatrix.tocsr()
        diagonal = matrix.diagonal()
        if np.any(diagonal == 0):
            raise _NoJacobiPreconditioner(
                f"{int(np.count_nonzero(diagonal == 0))} diagonal entries are zero"
            )
        self.rtol = rtol
        self.residual_tol = residual_tol
        self.max_iterations = max_iterations
        self._matrix = matrix
        # `||A||_inf`, the scale the backward error in `_accept` is measured
        # against. One pass over the nonzeros, once per block.
        self._matrix_norm = float(abs(matrix).sum(axis=1).max())
        self._inverse_diagonal = 1.0 / diagonal
        # `I - D^-1 A`, built once and reused by every solve on this block.
        self._iteration_matrix = (
            sp.eye(matrix.shape[0], format="csr")
            - sp.diags(self._inverse_diagonal) @ matrix
        ).tocsr()
        self._iteration_matrix.eliminate_zeros()

        self._build_fallback = fallback
        self._fallback = None
        self.fell_back = False
        # Series terms summed so far, across every solve. Diagnostic.
        self.iterations = 0

    def solve(self, rhs):
        if self._fallback is not None:
            return self._fallback.solve(rhs)

        two_dimensional = rhs.ndim == 2
        columns = rhs if two_dimensional else rhs.reshape(-1, 1)
        solution = self._series(columns)
        if solution is None:
            self._fallback = self._build_fallback()
            self.fell_back = True
            return self._fallback.solve(rhs)
        return solution if two_dimensional else solution.ravel()

    def _series(self, columns):
        """The summed series, or None if this block cannot be iterated.

        Convergence is judged per column, not on the norm of the batch: a
        cascade carries columns many orders of magnitude apart.
        """
        solution = self._inverse_diagonal[:, None] * columns
        term = solution.copy()
        previous = None
        growing = 0
        for _ in range(self.max_iterations):
            term = self._iteration_matrix @ term
            solution += term
            self.iterations += 1
            increment = np.linalg.norm(term, axis=0)
            if not np.all(np.isfinite(increment)):
                return None
            if np.all(increment <= self.rtol * np.linalg.norm(solution, axis=0)):
                return self._accept(solution, columns)
            largest = increment.max()
            if previous is not None and largest > previous:
                growing += 1
                if growing >= ITERATIVE_DIVERGENCE_PATIENCE:
                    return None
            else:
                growing = 0
            previous = largest
        return None

    def _accept(self, solution, columns):
        """The converged sum, or None if its backward error is too large."""
        residual = np.linalg.norm(self._matrix @ solution - columns, axis=0)
        scale = self._matrix_norm * np.linalg.norm(solution, axis=0) + np.linalg.norm(
            columns, axis=0
        )
        relative = np.divide(
            residual, scale, out=np.zeros_like(residual), where=scale > 0
        )
        if not np.all(np.isfinite(relative)) or relative.max() > self.residual_tol:
            return None
        return solution


def iterative_solver_enabled() -> bool:
    """Whether the series path may be used. `BW_TIMEX_NO_ITERATIVE_SOLVER`
    turns it off process-wide, leaving every block on the LU backend."""
    return not os.environ.get("BW_TIMEX_NO_ITERATIVE_SOLVER")


class _NoJacobiPreconditioner(Exception):
    """The block has a zero on its diagonal, so it cannot be iterated."""


def _reject_empty_rows(csr) -> None:
    """Raise if `csr` has a zero row, naming where and why.

    A square block with an empty row is singular, and no backend can
    factorize it - pardiso's `_check_A` refuses it outright, SuperLU's `splu`
    raises `RuntimeError: Factor is exactly singular`. Naming the condition
    once, here, keeps the diagnosis precise instead of surfacing whichever
    backend's internal error happens to fire first.
    """
    empty_rows = np.flatnonzero(np.diff(csr.indptr) == 0)
    if len(empty_rows):
        raise ValueError(
            f"Block is singular: {len(empty_rows)} of its {csr.shape[0]} rows are "
            f"empty (first at index {int(empty_rows[0])}), so the block contains a "
            "product that nothing inside it produces. No sparse solver can "
            "factorize this; the block structure or the underlying inventory "
            "needs fixing."
        )


def make_block_solver(backend: str, submatrix, allow_iterative: bool = False):
    """Build the block solver named by `backend` over `submatrix`.

    With `allow_iterative`, returns an `_IterativeBlockSolver` that builds
    `backend`'s LU only if the series cannot solve the block. Off by default,
    so naming a backend returns that backend's object.

    `backend` is verified first (`require_backend`), so asking for a backend
    this machine cannot provide raises instead of quietly handing back an
    object that reports the requested name while solving with something else.

    A block with an empty row is rejected here, for every backend. Such a
    block is square with a zero row, i.e. singular, and no backend can solve
    it: pardiso's `_check_A` raises
    `ValueError('Matrix A is singular, because it contains empty row(s)')`,
    and SuperLU's `splu` raises `RuntimeError: Factor is exactly singular`.
    Routing one to the other only exchanges a precise diagnosis for a vague
    one, so the condition is named once, here, where the offending rows can
    still be pointed at.
    """
    require_backend(backend)
    csr = submatrix.tocsr()
    _reject_empty_rows(csr)

    def build_lu():
        if backend == "pardiso":
            return _PardisoBlockSolver(csr)
        if backend == "umfpack":
            return _UmfpackBlockSolver(submatrix)
        if backend == "superlu":
            return _SuperLUBlockSolver(submatrix)
        raise ValueError(f"Unknown block solver {backend!r}")

    if allow_iterative:
        iterative = _try_iterative(csr, build_lu)
        if iterative is not None:
            return iterative
    return build_lu()


def _try_iterative(csr, build_lu):
    """An iterative solver for this block, or None when the path is switched
    off or the block has no Jacobi preconditioner. Non-convergence is not
    decided here; that falls back from inside `solve`."""
    if not iterative_solver_enabled():
        return None
    try:
        return _IterativeBlockSolver(csr, build_lu)
    except _NoJacobiPreconditioner:
        return None


def make_persistent_block_solver(
    submatrix, backend: Optional[str] = None, allow_iterative: bool = False
):
    """Build a block solver that *owns* its factorization for its lifetime.

    Use this - not `make_block_solver` - when one block will be solved many
    times with separate right-hand sides, interleaved with solves of other
    blocks. That access pattern is exactly what MKL Pardiso is worst at:
    `pypardiso` keeps a single global factorization slot and re-runs the
    analysis and the numeric factorization whenever the matrix handed to
    `spsolve` differs from the previous call, so alternating between B blocks
    over T rounds costs T*B full factorizations instead of B.

    SuperLU's `splu` and UMFPACK's `factorized` both hold their own LU inside
    the returned object, so either is safe here; pardiso is substituted with
    SuperLU. The pardiso multi-RHS win is unaffected - it lives on the batched
    path (`BackgroundSolver.prepare`), where one call carries every column and
    the global slot is used once per block.

    An empty-row block is rejected exactly as in `make_block_solver`, so both
    entry points report the same diagnosis for the same degeneracy.
    """
    if backend is None:
        backend = select_backend()
    csr = submatrix.tocsr()
    _reject_empty_rows(csr)

    def build_lu():
        if backend == "umfpack":
            return _UmfpackBlockSolver(submatrix)
        return _SuperLUBlockSolver(submatrix)

    if allow_iterative:
        # Persistent in the sense meant here: the iteration matrix is built
        # once and reused, so revisiting the block costs no re-analysis.
        iterative = _try_iterative(csr, build_lu)
        if iterative is not None:
            return iterative
    return build_lu()
