"""Every solver backend, end to end, with and without the iterative path.

A full `TimexLCA` run must produce the same numbers whichever backend is
active and whether blocks and the functional unit are solved by Neumann
series or by an LU factorization.
"""

from datetime import datetime

import bw2data as bd
import numpy as np
import pytest
from bw2calc import LCA

from bw_timex import TimexLCA
from bw_timex.solvers import BACKENDS, backend_available

DATABASE_DATES = {
    "db_2020": datetime.strptime("2020", "%Y"),
    "db_2030": datetime.strptime("2030", "%Y"),
    "db_2040": datetime.strptime("2040", "%Y"),
    "foreground": "dynamic",
}

AVAILABLE = [name for name in BACKENDS if backend_available(name)]


def _run(monkeypatch, backend=None, iterative=True):
    """One full run, pinned to `backend` and to the given solver mode."""
    if backend is None:
        monkeypatch.delenv("BW_TIMEX_BLOCK_SOLVER", raising=False)
    else:
        monkeypatch.setenv("BW_TIMEX_BLOCK_SOLVER", backend)
    if iterative:
        monkeypatch.delenv("BW_TIMEX_NO_ITERATIVE_SOLVER", raising=False)
    else:
        monkeypatch.setenv("BW_TIMEX_NO_ITERATIVE_SOLVER", "1")

    tlca = TimexLCA(
        demand={bd.get_node(database="foreground", code="EV").key: 1},
        method=("GWP", "example"),
        database_dates=DATABASE_DATES,
        use_global_lci_cache=False,
    )
    tlca.build_timeline(starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"))
    tlca.lci()
    tlca.static_lcia()
    return tlca


_lci_calculation_calls = []


def _functional_unit_solvers(monkeypatch):
    """Record the solver `TimexLCA` builds for each whole-matrix solve.

    Also records any call to bw2calc's `lci_calculation`, the LU path the
    series is meant to replace.
    """
    import bw_timex.timex_lca as timex_lca

    names = []
    original_make = timex_lca.make_block_solver

    def spy(backend, submatrix, **kwargs):
        solver = original_make(backend, submatrix, **kwargs)
        names.append(solver.name)
        return solver

    monkeypatch.setattr(timex_lca, "make_block_solver", spy)

    _lci_calculation_calls.clear()
    original_lci = LCA.lci_calculation
    monkeypatch.setattr(
        LCA,
        "lci_calculation",
        lambda self: (_lci_calculation_calls.append(1), original_lci(self))[1],
    )
    return names


@pytest.mark.usefixtures("vehicle_db")
class TestEveryBackendAgrees:

    @pytest.mark.parametrize("backend", AVAILABLE)
    @pytest.mark.parametrize("iterative", [True, False])
    def test_static_score_is_the_same(self, monkeypatch, backend, iterative):
        reference = _run(monkeypatch, backend=AVAILABLE[0], iterative=False)
        expected = reference.static_score
        expected_supply = reference.lca.supply_array.copy()

        tlca = _run(monkeypatch, backend=backend, iterative=iterative)

        assert tlca.static_score == pytest.approx(expected, rel=1e-10)
        assert np.allclose(tlca.lca.supply_array, expected_supply, rtol=1e-9, atol=0)

    @pytest.mark.parametrize("backend", AVAILABLE)
    def test_dynamic_inventory_is_the_same(self, monkeypatch, backend):
        # Totalled rather than compared row by row: row ids are assigned in
        # build order, so positions are not comparable across runs.
        reference = _run(monkeypatch, backend=AVAILABLE[0], iterative=False)
        expected = reference.dynamic_inventory.sum()

        tlca = _run(monkeypatch, backend=backend, iterative=True)

        assert tlca.dynamic_inventory.sum() == pytest.approx(expected, rel=1e-10)

    @pytest.mark.parametrize("backend", AVAILABLE)
    def test_blocks_are_solved_by_the_series(self, monkeypatch, backend):
        tlca = _run(monkeypatch, backend=backend, iterative=True)

        names = {s.name for s in tlca._background_solver._block_solvers.values()}
        assert names == {"iterative"}

    @pytest.mark.parametrize("backend", AVAILABLE)
    def test_the_functional_unit_is_solved_by_the_series(self, monkeypatch, backend):
        names = _functional_unit_solvers(monkeypatch)

        tlca = _run(monkeypatch, backend=backend, iterative=True)

        assert tlca.lca.supply_array.any()
        assert set(names) == {"iterative"}
        # bw2calc's own LU solve of the whole expanded matrix is not reached.
        assert not _lci_calculation_calls

    @pytest.mark.parametrize("backend", AVAILABLE)
    def test_the_functional_unit_uses_the_lu_when_iteration_is_off(
        self, monkeypatch, backend
    ):
        names = _functional_unit_solvers(monkeypatch)

        _run(monkeypatch, backend=backend, iterative=False)

        assert set(names) == {backend}
