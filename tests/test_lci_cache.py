from datetime import datetime

import bw2data as bd
import pytest

import bw_timex
from bw_timex import TimexLCA
from bw_timex._lci_cache import (
    BACKGROUND_UNIT_LCI_CACHE,
    BIOSPHERE_EXCHANGES_CACHE,
    LCI_SOLVE_CACHE,
)


def _build_tlca(**kwargs):
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
    tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)
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
            for k in BACKGROUND_UNIT_LCI_CACHE
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
        n_before = len([k for k in BACKGROUND_UNIT_LCI_CACHE if k[0] == "db_code"])
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
        n_after = len([k for k in BACKGROUND_UNIT_LCI_CACHE if k[0] == "db_code"])
        assert n_after == n_before

    def test_cache_persists_and_is_reused_across_objects(self):
        _build_tlca()
        keys = self._db_2020_c_keys()
        assert len(keys) == 1
        key = keys[0]
        cached_matrix = BACKGROUND_UNIT_LCI_CACHE[key]

        _build_tlca()
        # Identical scenario re-run: same key, same object reused (not recomputed),
        # and no extra entry created.
        assert self._db_2020_c_keys() == [key]
        assert BACKGROUND_UNIT_LCI_CACHE[key] is cached_matrix

    def test_opt_out_does_not_use_global_cache(self):
        _build_tlca(use_global_lci_cache=False)
        assert len(BACKGROUND_UNIT_LCI_CACHE) == 0

    def test_clear_background_lci_cache_empties_it(self):
        _build_tlca()
        assert len(BACKGROUND_UNIT_LCI_CACHE) > 0
        bw_timex.clear_background_lci_cache()
        assert len(BACKGROUND_UNIT_LCI_CACHE) == 0

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
        with pytest.raises(ValueError, match="expanded matrix"):
            tlca_flat.static_lcia()

    def test_warm_cache_skips_factorization(self):
        # First run populates the global cache.
        _build_tlca()
        # Second run with identical scenario should have 0 cache misses and
        # therefore skip the expensive technosphere LU factorization.
        tlca2 = _build_tlca()
        assert tlca2._lci_did_factorize is False

    def test_cold_cache_factorizes_when_above_threshold(self, monkeypatch):
        # Force the threshold down so the tiny test scenario triggers factorize.
        import bw_timex.timex_lca as tlca_mod

        monkeypatch.setattr(tlca_mod, "FACTORIZE_SOLVES_THRESHOLD", 1)
        tlca = _build_tlca()
        assert tlca._lci_did_factorize is True

    def test_cold_cache_skips_factorization_when_below_threshold(self, monkeypatch):
        # With the threshold high, the few cache misses should not justify
        # factorizing — single sparse solves are cheaper.
        import bw_timex.timex_lca as tlca_mod

        monkeypatch.setattr(tlca_mod, "FACTORIZE_SOLVES_THRESHOLD", 1000)
        tlca = _build_tlca()
        assert tlca._lci_did_factorize is False

    def test_warm_cache_skips_trailing_redo_lci_reset(self):
        # First run populates global cache.
        _build_tlca()
        # Second run has zero background unit-LCI solves during the build,
        # so the trailing redo_lci(self.fu) reset is unnecessary.
        tlca2 = _build_tlca()
        assert tlca2._lci_did_reset is False

    def test_cold_cache_does_trailing_redo_lci_reset(self):
        # Cold cache: build will call redo_lci on at least one background act,
        # so the trailing reset back to the functional unit is required.
        tlca = _build_tlca()
        assert tlca._lci_did_reset is True

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

    def test_opt_out_produces_same_score_as_global(self):
        tlca_global = _build_tlca()
        tlca_global.static_lcia()
        global_score = tlca_global.static_score

        bw_timex.clear_background_lci_cache()
        tlca_isolated = _build_tlca(use_global_lci_cache=False)
        tlca_isolated.static_lcia()

        assert tlca_isolated.static_score == pytest.approx(global_score)
