"""Sparse solver backends for per-block background solving.

`bw_timex` solves one diagonal block of the technosphere at a time, many
times over, so which sparse solver is available dominates `lci()` runtime.
This module probes what the machine actually has - never what its platform
suggests - and wraps each option behind one `solve(rhs)` interface.

Three backends, in preference order:

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

import os
import platform
import sys
import warnings
from typing import Optional

import numpy as np

BACKENDS = ("pardiso", "umfpack", "superlu")

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


def _suboptimal_message(sys_platform: str, machine: str) -> str:
    """What to tell a user stuck on SuperLU, per platform.

    Every branch names something that works on the machine reading it. A
    recommendation that cannot succeed there is worse than staying quiet.
    """
    prefix = (
        "bw_timex is solving background systems with SciPy's SuperLU, which is "
        "substantially slower than the alternatives. "
    )
    if sys_platform == "darwin":
        return prefix + (
            'Install UMFPACK with: brew install suite-sparse && pip install "bw_timex[solvers]"'
        )
    if sys_platform == "linux" and machine != "x86_64":
        return prefix + (
            "Intel MKL has no wheel for this architecture. Install UMFPACK with: "
            'apt install libsuitesparse-dev && pip install "bw_timex[solvers]"'
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


def make_block_solver(backend: str, submatrix):
    """Build the block solver named by `backend` over `submatrix`.

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
    if backend == "pardiso":
        return _PardisoBlockSolver(csr)
    if backend == "umfpack":
        return _UmfpackBlockSolver(submatrix)
    if backend == "superlu":
        return _SuperLUBlockSolver(submatrix)
    raise ValueError(f"Unknown block solver {backend!r}")


def make_persistent_block_solver(submatrix, backend: Optional[str] = None):
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
    _reject_empty_rows(submatrix.tocsr())
    if backend == "umfpack":
        return _UmfpackBlockSolver(submatrix)
    return _SuperLUBlockSolver(submatrix)
