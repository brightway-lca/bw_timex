from datetime import datetime

import bw2data as bd
import pytest

import bw_timex
from bw_timex import TimexLCA
from bw_timex._lci_cache import BACKGROUND_UNIT_LCI_CACHE


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
        return [
            k
            for k in BACKGROUND_UNIT_LCI_CACHE
            if k[0] == "db_code"
            and k[1] == "db_2020"
            and k[2] == "C"
            and k[3] == modified
        ]

    def test_global_cache_populated_with_background_process(self):
        _build_tlca()
        assert self._db_2020_c_keys()

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

    def test_opt_out_produces_same_score_as_global(self):
        tlca_global = _build_tlca()
        tlca_global.static_lcia()
        global_score = tlca_global.static_score

        bw_timex.clear_background_lci_cache()
        tlca_isolated = _build_tlca(use_global_lci_cache=False)
        tlca_isolated.static_lcia()

        assert tlca_isolated.static_score == pytest.approx(global_score)
