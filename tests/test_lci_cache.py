from datetime import datetime

import bw2calc
import bw2data as bd
import pytest

import bw_timex
from bw_timex import TimexLCA
from bw_timex._lci_cache import (
    BACKGROUND_AGGREGATE_CACHE,
    BACKGROUND_SUPPLY_CACHE,
    BIOSPHERE_EXCHANGES_CACHE,
    LCI_SOLVE_CACHE,
    NODES_CACHE,
)


def _make_tlca(**kwargs):
    """A TimexLCA with its timeline built, but no `lci()` call yet."""
    node_a = bd.get_node(database="foreground", code="A")
    database_dates = {
        "db_2020": datetime.strptime("2020", "%Y"),
        "foreground": "dynamic",
    }
    tlca = TimexLCA(
        demand={node_a: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
        **kwargs,
    )
    tlca.build_timeline(starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"))
    return tlca


def _build_tlca(expand_technosphere=True, **kwargs):
    tlca = _make_tlca(**kwargs)
    tlca.lci(
        expand_technosphere=expand_technosphere, build_dynamic_biosphere=True
    )
    return tlca


@pytest.mark.usefixtures("dynamic_biosphere_matrix_db")
class TestModuleLevelLCICache:

    @pytest.fixture(autouse=True)
    def _clear(self):
        bw_timex.clear_background_lci_cache()
        yield
        bw_timex.clear_background_lci_cache()

    @staticmethod
    def _db_2020_c_keys():
        modified = bd.databases["db_2020"].get("modified")
        project = bd.projects.current
        return [
            k
            for k in BACKGROUND_SUPPLY_CACHE
            if k[0] == "db_code"
            and k[1] == project
            and k[2] == "db_2020"
            and k[3] == "C"
            and k[4] == modified
        ]

    def test_global_cache_populated_with_background_process(self):
        _build_tlca()
        assert self._db_2020_c_keys()

    def test_cache_reused_across_different_lci_structures(self):
        # Build with expand_technosphere=True populates the cache.
        _build_tlca()
        n_before = len([k for k in BACKGROUND_SUPPLY_CACHE if k[0] == "db_code"])
        assert n_before > 0

        # Build with expand_technosphere=False — different lca_obj structure
        # (no expanded foreground/biosphere matrices). Background unit LCI
        # for "db_2020/C" should still be reused, not recomputed.
        node_a = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        }
        tlca2 = TimexLCA(
            demand={node_a: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca2.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        tlca2.lci(expand_technosphere=False, build_dynamic_biosphere=True)

        # Same db_code entries — no new misses logged.
        n_after = len([k for k in BACKGROUND_SUPPLY_CACHE if k[0] == "db_code"])
        assert n_after == n_before

    def test_cache_persists_and_is_reused_across_objects(self):
        _build_tlca()
        keys = self._db_2020_c_keys()
        assert len(keys) == 1
        key = keys[0]
        cached_payload = BACKGROUND_SUPPLY_CACHE[key]

        tlca_warm = _build_tlca()
        # Identical scenario re-run: same key, same payload reused (not
        # recomputed), no extra entry created, and no solve performed.
        assert self._db_2020_c_keys() == [key]
        assert BACKGROUND_SUPPLY_CACHE[key] is cached_payload
        assert tlca_warm._background_solver.n_solves == 0

    def test_opt_out_does_not_use_global_cache(self):
        tlca = _build_tlca(use_global_lci_cache=False)
        assert len(BACKGROUND_SUPPLY_CACHE) == 0
        assert len(BACKGROUND_AGGREGATE_CACHE) == 0
        # The background LCIs went into the object's private caches instead.
        assert len(tlca._background_supply_cache) > 0
        assert tlca._background_solver.shared_cache is tlca._background_supply_cache

    def test_clear_background_lci_cache_empties_it(self):
        _build_tlca()
        assert len(BACKGROUND_SUPPLY_CACHE) > 0
        bw_timex.clear_background_lci_cache()
        assert len(BACKGROUND_SUPPLY_CACHE) == 0

    def test_global_cache_does_not_leak_across_structures(self):
        # expand_technosphere=True then a second object with expand=False must
        # NOT reuse the (structurally incompatible) cached inventory.
        node_a = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        }
        tlca_expanded = TimexLCA(
            demand={node_a: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca_expanded.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        tlca_expanded.lci(expand_technosphere=True, build_dynamic_biosphere=True)

        tlca_flat = TimexLCA(
            demand={node_a: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca_flat.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        tlca_flat.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        # The flat build has its own (unexpanded) lca object: it must not have
        # picked up the expanded run's cached solve, and its inventory - built
        # from the timeline - must still give the same score.
        assert tlca_flat.expanded_technosphere is False
        assert tlca_flat._lci_used_cached_solve is False
        tlca_flat.static_lcia()
        tlca_expanded.static_lcia()
        assert tlca_flat.static_score == pytest.approx(
            tlca_expanded.static_score, rel=1e-9
        )

    def test_cold_cache_solves_background_lcis_off_the_main_matrix(self):
        # Background unit LCIs are solved per block by `BackgroundSolver`, so
        # a cold run performs them without ever touching `self.lca`.
        tlca = _build_tlca()
        assert tlca._background_solver.n_solves > 0

    def test_warm_cache_performs_no_background_solves(self):
        _build_tlca()
        tlca_warm = _build_tlca()
        assert tlca_warm._background_solver.n_solves == 0

    def test_expanded_lci_solves_the_main_matrix_exactly_once(self, monkeypatch):
        # Since background unit LCIs never run through `self.lca` any more, the
        # expanded matrix needs one solve and no `redo_lci` reset afterwards.
        tlca = _make_tlca()
        calls = {"lci_calculation": 0, "redo_lci": 0}
        original_lci_calculation = bw2calc.LCA.lci_calculation
        original_redo_lci = bw2calc.LCA.redo_lci

        def counting_lci_calculation(lca_obj, *args, **kwargs):
            calls["lci_calculation"] += 1
            return original_lci_calculation(lca_obj, *args, **kwargs)

        def counting_redo_lci(lca_obj, *args, **kwargs):
            calls["redo_lci"] += 1
            return original_redo_lci(lca_obj, *args, **kwargs)

        monkeypatch.setattr(bw2calc.LCA, "lci_calculation", counting_lci_calculation)
        monkeypatch.setattr(bw2calc.LCA, "redo_lci", counting_redo_lci)
        tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)

        assert calls == {"lci_calculation": 1, "redo_lci": 0}
        # And the surviving fu inventory really is the functional unit's.
        assert tlca._background_solver.n_solves > 0
        tlca.static_lcia()
        assert tlca.static_score == pytest.approx(tlca.dynamic_inventory.sum())

    def test_biosphere_exchanges_cache_persists_across_objects(self):
        _build_tlca()
        first = dict(BIOSPHERE_EXCHANGES_CACHE)
        assert len(first) > 0
        _build_tlca()
        # No new entries added on the second build — same exchanges reused.
        assert set(BIOSPHERE_EXCHANGES_CACHE) == set(first)
        for key, value in first.items():
            assert BIOSPHERE_EXCHANGES_CACHE[key] is value

    def test_clear_background_lci_cache_clears_biosphere_exchanges_too(self):
        _build_tlca()
        assert len(BIOSPHERE_EXCHANGES_CACHE) > 0
        bw_timex.clear_background_lci_cache()
        assert len(BIOSPHERE_EXCHANGES_CACHE) == 0

    def test_from_timeline_reuses_cached_background_lcis(self):
        # Building from the timeline needs the same background unit LCIs as the
        # expanded path, and reuses the same cache entries.
        tlca = _build_tlca(expand_technosphere=False)
        assert tlca._background_solver.n_solves > 0
        tlca_warm = _build_tlca(expand_technosphere=False)
        assert tlca_warm._background_solver.n_solves == 0

    def test_from_timeline_matches_expanded_score(self):
        tlca_expanded = _build_tlca(expand_technosphere=True)
        tlca_expanded.static_lcia()
        tlca_timeline = _build_tlca(expand_technosphere=False)
        assert tlca_timeline.dynamic_inventory.sum() == pytest.approx(
            tlca_expanded.dynamic_inventory.sum()
        )

    def test_warm_skips_initial_lca_solve(self):
        # First run populates both unit-LCI cache and solve cache.
        _build_tlca()
        assert len(LCI_SOLVE_CACHE) > 0

        # Second run with identical scenario should reuse the solve and
        # never call lci_calculation again.
        tlca2 = _build_tlca()
        assert tlca2._lci_used_cached_solve is True

    def test_cold_does_full_lca_solve(self):
        tlca = _build_tlca()
        assert tlca._lci_used_cached_solve is False

    def test_clear_background_lci_cache_clears_solve_cache_too(self):
        _build_tlca()
        assert len(LCI_SOLVE_CACHE) > 0
        bw_timex.clear_background_lci_cache()
        assert len(LCI_SOLVE_CACHE) == 0

    def test_warm_cached_solve_matches_score(self):
        # Reusing the cached solve must not change the LCIA result.
        tlca_cold = _build_tlca()
        tlca_cold.static_lcia()
        cold_score = tlca_cold.static_score
        tlca_warm = _build_tlca()
        tlca_warm.static_lcia()
        assert tlca_warm.static_score == pytest.approx(cold_score)

    @staticmethod
    def _nodes_keys(db="db_2020"):
        modified = bd.databases[db].get("modified")
        project = bd.projects.current
        return [
            k
            for k in NODES_CACHE
            if k[0] == "nodes"
            and k[1] == project
            and k[2] == db
            and k[3] == modified
        ]

    def test_nodes_cache_populated_per_database(self):
        tlca = _build_tlca()
        # One entry per database in database_dates (db_2020 + foreground).
        assert self._nodes_keys("db_2020")
        assert self._nodes_keys("foreground")
        # Cached node proxies are exactly what the object exposes.
        cached = {}
        for k in NODES_CACHE:
            cached.update(NODES_CACHE[k])
        assert set(cached) == set(tlca.nodes)

    def test_nodes_cache_reused_across_objects(self):
        tlca1 = _build_tlca()
        key = self._nodes_keys("db_2020")[0]
        cached_db = NODES_CACHE[key]

        tlca2 = _build_tlca()
        # Same per-db dict object reused, not re-queried.
        assert self._nodes_keys("db_2020") == [key]
        assert NODES_CACHE[key] is cached_db
        # Proxies are shared across objects.
        node_id = next(iter(cached_db))
        assert tlca1.nodes[node_id] is tlca2.nodes[node_id]

    def test_nodes_cache_opt_out_does_not_use_global(self):
        _build_tlca(use_global_lci_cache=False)
        assert len(NODES_CACHE) == 0

    def test_clear_background_lci_cache_clears_nodes_too(self):
        _build_tlca()
        assert len(NODES_CACHE) > 0
        bw_timex.clear_background_lci_cache()
        assert len(NODES_CACHE) == 0

    def test_opt_out_produces_same_score_as_global(self):
        tlca_global = _build_tlca()
        tlca_global.static_lcia()
        global_score = tlca_global.static_score

        bw_timex.clear_background_lci_cache()
        tlca_isolated = _build_tlca(use_global_lci_cache=False)
        tlca_isolated.static_lcia()

        assert tlca_isolated.static_score == pytest.approx(global_score)
