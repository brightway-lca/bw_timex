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
def test_multi_date_consumer_split(
    background_td_multidate_consumer_db, graph_traversal
):
    """A foreground TD feeding a non-leaf background activity makes bg_A reached
    at >1 cohort date, so the bg_A->bg_B variant split has a multi-date consumer.
    The split routes each consumer cohort independently.

    Hand-computation (starting 2024, fu->bg_A TD 60% @2024 / 40% @2034; all
    intermediate amounts 1; bg_C->CO2 = 11 in 2020, 7 in 2030):
    - bg_A: 0.6 @2024, 0.4 @2034  -> bg_B / bg_C carry the same dates.
    - bg_B@2024 (0.6) routes {2020:0.6, 2030:0.4} -> 0.36 via 2020, 0.24 via 2030.
    - bg_B@2034 (0.4) routes {2030:1.0}           -> 0.40 via 2030.
    - locked-variant bg_C emissions: 0.36*11 + 0.24*7 + 0.40*7
      = 3.96 + 1.68 + 2.80 = 8.44 kg CO2.
    """
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(8.44, rel=1e-9)


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


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_background_td_with_explicit_product_paradigm(
    explicit_background_td_db, graph_traversal
):
    """Background temporal distributions work when the foreground uses the
    explicit process/product paradigm: a bare ``service_product`` plus a
    ``service_process`` that owns the production edge to it, and the demand is
    the *product*.

    Hand-computation (demand service_product = 1, starting 2024):
    - service_process -> 3 kWh electricity (descended -> temporalized).
    - electricity -> fuel (amount 2), TD 60% +0y / 40% +10y -> fuel cohorts:
        3.6 kg @2024,  2.4 kg @2034.
    - fuel -> CO2 routed linearly between the 2020 and 2040 grids:
        @2024 -> 0.8*11 + 0.2*7 = 10.2 -> 36.72
        @2034 -> 0.3*11 + 0.7*7 = 8.2  -> 19.68
    - total = 56.4 kg CO2 (the all-2020-grid static base would be 66.0).
    """
    import bw2calc as bc

    database_dates = explicit_background_td_db
    product = bd.get_node(database="foreground", code="service_product")
    demand = {product.key: 1}

    slca = bc.LCA(demand, method=METHOD)
    slca.lci()
    slca.lcia()
    assert slca.score == pytest.approx(66.0, rel=1e-9)

    tlca = TimexLCA(demand=demand, method=METHOD, database_dates=database_dates)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()

    assert tlca.base_lca.score == pytest.approx(slca.score)
    assert tlca.static_score == pytest.approx(56.4, rel=1e-9)

    tl = tlca.timeline
    # The demanded explicit product's process is descended -> temporalized (no shares).
    elec = tl[tl["producer_name"] == "electricity"]
    assert elec["temporal_market_shares"].isnull().all()
    # The background TD spreads fuel across two cohorts, each routed across variants.
    fuel = tl[tl["producer_name"] == "fuel"]
    assert sorted({d.year for d in fuel["date_producer"]}) == [2024, 2034]
    assert fuel["temporal_market_shares"].notnull().all()


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_single_background_db_with_td_matches_static(
    background_td_single_db, graph_traversal
):
    """With a single background database, an internal background TD only
    redistributes emissions in time. Since every flow is sourced from that one
    database regardless of when it occurs, the time-aggregated (static) score is
    unchanged: traverse_background must match the plain static LCA score."""
    import bw2calc as bc

    from datetime import datetime

    demand = {("foreground", "fu"): 1}
    database_dates = {"background": datetime(2020, 1, 1), "foreground": "dynamic"}

    slca = bc.LCA(demand, method=METHOD)
    slca.lci()
    slca.lcia()

    tlca = TimexLCA(demand=demand, method=METHOD, database_dates=database_dates)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()

    # The background TD still spreads fuel over time...
    fuel = tlca.timeline[tlca.timeline["producer_name"] == "fuel"]
    assert sorted({d.year for d in fuel["date_producer"]}) == [2024, 2034]
    # ...but with one background db the score equals the static LCA exactly.
    assert tlca.static_score == pytest.approx(slca.score, rel=1e-9)
