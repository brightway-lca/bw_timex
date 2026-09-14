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
"""

import os
import platform
import sys
import warnings

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


def select_backend(override: str = None) -> str:
    """Name of the block-solver backend to use.

    `override`, else `BW_TIMEX_BLOCK_SOLVER`, else the best available.
    """
    name = override or os.environ.get("BW_TIMEX_BLOCK_SOLVER")
    if name:
        name = name.strip().lower()
        if name not in BACKENDS:
            raise ValueError(
                f"Unknown block solver {name!r}; expected one of {', '.join(BACKENDS)}"
            )
        return name
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
    backend: str, sys_platform: str = None, machine: str = None
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
