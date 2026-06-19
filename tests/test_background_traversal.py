from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def test_fixture_loads(background_td_db):
    assert "background_2020" in bd.databases
    assert "background_2030" in bd.databases
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score > 0


def test_classification_keys_on_shares_not_db(background_td_db):
    """With traverse_background=False, results are unchanged by the refactor."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    tlca.lci()
    tlca.static_lcia()
    # bg_A is the only first-level background producer -> exactly one temporal market.
    assert len(tlca.node_collections["temporal_markets"]) == 1


def _strip_background_tds():
    """Remove the temporal_distribution on every background bg_A->bg_B exchange."""
    for dbname in ("background_2020", "background_2030"):
        for act in bd.Database(dbname):
            for exc in act.technosphere():
                if "temporal_distribution" in exc:
                    del exc["temporal_distribution"]
                    exc.save()
        bd.Database(dbname).process()


def _score(traverse_background):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=traverse_background,
    )
    tlca.lci()
    tlca.static_lcia()
    return tlca.static_score, tlca.timeline


def test_bfs_equivalence_without_background_tds(background_td_db):
    _strip_background_tds()
    score_static, tl_static = _score(False)
    score_traverse, tl_traverse = _score(True)
    # With traverse_background=True the BFS descends into bg_B, so the
    # traverse timeline must contain bg_B edges (more rows than the static one).
    assert len(tl_traverse) > len(tl_static), (
        "traverse_background=True must produce a deeper timeline than False"
    )
    assert score_traverse == pytest.approx(score_static, rel=1e-9)


def test_bfs_leaf_background_td_routes_by_variant(background_td_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tl = tlca.timeline
    bg_b = tl[tl["producer_name"] == "bg_B"]
    assert sorted({d.year for d in bg_b["date_producer"]}) == [2024, 2034]
    row_2034 = bg_b[[d.year == 2034 for d in bg_b["date_producer"]]].iloc[0]
    assert set(row_2034["temporal_market_shares"].keys()) == {"background_2030"}
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(10.0, rel=1e-9)  # 2 * 5 * 1


def test_bfs_respective_variant_deep_chain(background_td_deep_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    # Respective-variant reads: 2034 cohort sources bg_B->bg_C from background_2030 (=7),
    # not the referenced background_2020 (=1). Correct total CO2 = 48.4 kg.
    assert tlca.static_score == pytest.approx(48.4, rel=1e-9)
    # bg_C must appear at both 2024 and 2034.
    bg_c = tlca.timeline[tlca.timeline["producer_name"] == "bg_C"]
    assert sorted({d.year for d in bg_c["date_producer"]}) == [2024, 2034]
