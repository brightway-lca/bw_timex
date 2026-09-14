"""Dependency markers, checked against synthetic environments.

Platform behaviour is asserted by faking the platform, never by asking the
current one: the point is what happens on machines CI may not have.

The source of truth is pyproject.toml, not installed metadata, so that marker
regressions are caught even without a reinstall.
"""

import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement

ENVIRONMENTS = {
    "linux-x86_64": {"sys_platform": "linux", "platform_machine": "x86_64"},
    "linux-aarch64": {"sys_platform": "linux", "platform_machine": "aarch64"},
    "darwin-arm64": {"sys_platform": "darwin", "platform_machine": "arm64"},
    "darwin-x86_64": {"sys_platform": "darwin", "platform_machine": "x86_64"},
    "win32-AMD64": {"sys_platform": "win32", "platform_machine": "AMD64"},
    "win32-ARM64": {"sys_platform": "win32", "platform_machine": "ARM64"},
}


def _load_requirements():
    """Load requirements from pyproject.toml, the source of truth."""
    # Walk up from test file to find pyproject.toml
    test_dir = Path(__file__).parent
    pyproject_path = test_dir.parent / "pyproject.toml"

    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    return (
        config["project"]["dependencies"],
        config["project"]["optional-dependencies"]["solvers"],
    )


def _resolves(name, environment, extra=None):
    """Check if a requirement resolves in the given environment."""
    env = dict(environment)
    env["extra"] = extra or ""

    hard_deps, solvers_deps = _load_requirements()

    # Choose which dependency list to search
    search_list = solvers_deps if extra == "solvers" else hard_deps

    for raw in search_list:
        requirement = Requirement(raw)
        if requirement.name.lower().replace("_", "-") != name:
            continue
        if requirement.marker is None:
            return True
        if requirement.marker.evaluate(env):
            return True
    return False


@pytest.mark.parametrize(
    "environment,expected",
    [
        ("linux-x86_64", True),
        ("win32-AMD64", True),
        ("linux-aarch64", False),
        ("darwin-arm64", False),
        ("darwin-x86_64", False),
        ("win32-ARM64", False),
    ],
)
def test_pypardiso_resolves_only_where_mkl_has_wheels(environment, expected):
    # `mkl` publishes wheels for manylinux_2_28_x86_64 and win_amd64 only.
    # Anywhere else the install would fail outright.
    assert _resolves("pypardiso", ENVIRONMENTS[environment]) is expected


@pytest.mark.parametrize("environment", list(ENVIRONMENTS))
def test_scikit_umfpack_is_never_a_hard_dependency(environment):
    # SDIST-only: as a hard dependency it would make every install compile
    # against SuiteSparse and fail where the headers are absent.
    assert _resolves("scikit-umfpack", ENVIRONMENTS[environment]) is False


@pytest.mark.parametrize(
    "environment,expected",
    [
        ("darwin-arm64", True),
        ("darwin-x86_64", True),
        ("linux-aarch64", True),
        ("linux-x86_64", False),
        ("win32-AMD64", False),
    ],
)
def test_solvers_extra_offers_umfpack_where_mkl_cannot_reach(environment, expected):
    assert (
        _resolves("scikit-umfpack", ENVIRONMENTS[environment], extra="solvers")
        is expected
    )
