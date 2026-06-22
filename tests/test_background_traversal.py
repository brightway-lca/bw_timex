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


def _strip_background_tds_deep():
    """Remove temporal_distribution on every background technosphere exchange
    (bg_A->bg_B and bg_B->bg_C) in both variants of the deep fixture."""
    _strip_background_tds()


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


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_respective_variant_deep_chain_both_engines(
    background_td_deep_db, graph_traversal
):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(48.4, rel=1e-9)
    bg_c = tlca.timeline[tlca.timeline["producer_name"] == "bg_C"]
    assert sorted({d.year for d in bg_c["date_producer"]}) == [2024, 2034]


def test_priority_equivalence_without_background_tds(background_td_deep_db):
    _strip_background_tds_deep()

    def score(tb):
        t = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
        t.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="priority",
            traverse_background=tb,
        )
        t.lci()
        t.static_lcia()
        return t.static_score

    assert score(True) == pytest.approx(score(False), rel=1e-9)


def test_bfs_per_variant_td_reads_respective_variant(background_td_deep_tdvar_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    bg_c = tlca.timeline[tlca.timeline["producer_name"] == "bg_C"]
    years = sorted({d.year for d in bg_c["date_producer"]})
    # +20y spread (2044, 2054) appears ONLY if the 2030-variant TD was read.
    assert 2044 in years, f"expected 2044 cohort from background_2030 TD, got {years}"
    assert 2054 in years, f"expected 2054 cohort from background_2030 TD, got {years}"
    # amounts equal across variants -> total CO2 unchanged at 10 kg
    assert tlca.static_score == pytest.approx(10.0, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["bfs", "priority"])
def test_multi_date_consumer_raises(
    background_td_multidate_consumer_db, graph_traversal
):
    """A foreground TD feeding into a non-leaf background activity causes bg_A
    to be reached at >1 cohort date. The variant split cannot handle a multi-date
    consumer and must raise NotImplementedError loudly."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    with pytest.raises(NotImplementedError):
        tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal=graph_traversal,
            traverse_background=True,
        )


def test_bfs_traversed_background_not_double_counted(background_td_deep_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    tl = tlca.timeline
    # bg_B is descended into -> temporalized, NOT a temporal market (no shares).
    bg_b = tl[tl["producer_name"] == "bg_B"]
    assert bg_b["temporal_market_shares"].isnull().all()
    # Score is the hand-computed 48.4 (no inventory doubling).
    assert tlca.static_score == pytest.approx(48.4, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_foreground_td_into_traversed_background_convolves(
    background_td_fg_and_bg_db, graph_traversal
):
    """A foreground TD on the background->foreground edge (B->A) must convolve
    correctly with a background TD (C->B) when the traversal descends into the
    background. This stresses the convolution + graph-traversal path.

    Non-uniform weights are used on purpose so a symmetric bug cannot hide.

    Hand-computation (starting 2024):
    - A->B (amount 3), TD 70% @2024 / 30% @2030  -> B: 2.1 @2024, 0.9 @2030.
    - B->C (amount 2), TD 60% @+0y / 40% @+10y    -> electricity C cohorts (kWh):
        from B@2024: 2.1*2*0.6 = 2.52 @2024,  2.1*2*0.4 = 1.68 @2034
        from B@2030: 0.9*2*0.6 = 1.08 @2030,  0.9*2*0.4 = 0.72 @2040
    - C->CO2 by date-routed grid variant: @2024 -> 0.6*11+0.4*7 = 9.4;
      @2030/@2034/@2040 -> 7 (clean 2030 grid).
    - CO2 = 2.52*9.4 + 1.08*7 + 1.68*7 + 0.72*7
          = 23.688 + 7.56 + 11.76 + 5.04 = 48.048 kg.
    """
    tlca = TimexLCA({("foreground", "A"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(48.048, rel=1e-9)

    tl = tlca.timeline
    # The foreground TD spreads B (the descended background process) over 2024 & 2030.
    b_years = sorted({d.year for d in tl[tl["producer_name"] == "B"]["date_producer"]})
    assert b_years == [2024, 2030]
    # Convolving both TDs yields four electricity cohorts.
    c_years = sorted({d.year for d in tl[tl["producer_name"] == "C"]["date_producer"]})
    assert c_years == [2024, 2030, 2034, 2040]
    # B is descended into -> temporalized (no shares); C is the routed leaf.
    assert tl[tl["producer_name"] == "B"]["temporal_market_shares"].isnull().all()
    assert tl[tl["producer_name"] == "C"]["temporal_market_shares"].notnull().all()

    # Per-row amounts are per-unit-of-parent-cohort (codebase convention):
    #   B->A: 3 * [0.7, 0.3] = [2.1, 0.9]
    #   C->B: 2 * [0.6, 0.4] = [1.2, 0.8] for each B cohort -> C rows [1.2, 1.2, 0.8, 0.8]
    #   FU A: 1.0
    amount_by = lambda name: sorted(
        round(a, 6) for a in tl[tl["producer_name"] == name]["amount"]
    )
    assert amount_by("A") == [1.0]
    assert amount_by("B") == [0.9, 2.1]
    assert amount_by("C") == [0.8, 0.8, 1.2, 1.2]
