"""Several static background databases may share the same date."""

from datetime import datetime

import bw2data as bd
import pytest
from loguru import logger

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "modified_2020": datetime.strptime("2020", "%Y"),
    "modified_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _shares_by_producer(timeline):
    return {
        row.producer_name: row.temporal_market_shares
        for row in timeline.itertuples()
        if row.temporal_market_shares
    }


def test_shares_route_within_each_database_family(same_date_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")
    shares = _shares_by_producer(tlca.timeline)

    assert set(shares["electricity"]) == {"background_2020", "background_2030"}
    assert set(shares["steel, without EOL"]) == {"modified_2020", "modified_2030"}
    assert shares["electricity"]["background_2020"] == pytest.approx(0.5, abs=0.01)
    assert shares["steel, without EOL"]["modified_2020"] == pytest.approx(0.5, abs=0.01)


def test_score_interpolates_within_each_family(same_date_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")
    tlca.lci()
    tlca.static_lcia()
    # electricity: 0.5*10 + 0.5*5 = 7.5; steel: 0.5*20 + 0.5*10 = 15
    assert tlca.static_score == pytest.approx(22.5, abs=0.2)


def test_same_triplet_at_same_date_raises(same_date_db):
    """A copy that keeps name/reference product/location is ambiguous."""
    collision = bd.Database("modified_2020").new_node(
        "electricity_collision", name="electricity", unit="kWh"
    )
    collision["reference product"] = "electricity"
    collision["location"] = "GLO"
    collision.save()
    collision.new_edge(input=collision, amount=1, type="production").save()
    bd.Database("modified_2020").process()

    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    with pytest.raises(ValueError, match="more than one database"):
        tlca.build_timeline(starting_datetime="2025-01-01")


def test_producer_in_a_single_vintage_warns_and_is_time_invariant(same_date_db):
    """A copy made into only one vintage stays constant over time, with a warning."""
    bd.Database("modified_2030").get("steel_without_eol").delete()
    bd.Database("modified_2030").process()

    messages = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
        tlca.build_timeline(starting_datetime="2025-01-01")
    finally:
        logger.remove(sink_id)

    shares = _shares_by_producer(tlca.timeline)
    assert shares["steel, without EOL"] == {"modified_2020": 1}
    assert any("steel, without EOL" in message for message in messages)


def test_background_traversal_routes_within_the_modified_family(same_date_deep_db):
    """Descending into the background must not confuse same-date databases.

    ``database_dates`` is given with the ``modified_*`` entries FIRST here
    (unlike ``DATABASE_DATES`` above), so that a routing bug which resolves a
    cohort date through a global, insertion-order-dependent ``{date:
    database}`` inversion would pick ``background_*`` — the wrong family for
    ``smelting``/``coke``, which exist only under ``modified_*`` — instead of
    coincidentally landing on the right family. With the ordering used by the
    other tests in this file, that same bug happens to resolve to the right
    family by accident and this test would not catch it.
    """
    database_dates = {
        "modified_2020": datetime.strptime("2020", "%Y"),
        "modified_2030": datetime.strptime("2030", "%Y"),
        "background_2020": datetime.strptime("2020", "%Y"),
        "background_2030": datetime.strptime("2030", "%Y"),
        "foreground": "dynamic",
    }
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, database_dates)
    tlca.build_timeline(
        starting_datetime="2025-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    producers = set(tlca.timeline["producer_name"])
    assert "smelting" in producers

    # `smelting` exists only in the modified family, so no background_* database
    # may be picked up for it.
    smelting_rows = tlca.timeline[tlca.timeline["producer_name"] == "smelting"]
    for shares in smelting_rows["temporal_market_shares"]:
        if shares:
            assert set(shares) <= {"modified_2020", "modified_2030"}

    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score > 0


def test_background_traversal_same_triplet_at_same_date_raises(same_date_deep_db):
    """A same-date, same-triplet collision reached only through background
    descent must still raise loudly.

    `smelting` is resolved to its candidate databases via
    `_candidate_databases_for_node` (it is the node whose split the `coke`
    input in `same_date_deep_db` was added to trigger — see that fixture's
    docstring), not via `TimelineBuilder`'s temporal-market leaf logic. The
    collision here is wired into `background_2020` only, never into the
    foreground, so it is invisible to anything except the interdatabase
    mapping the extractor consults mid-descent.

    The assertion pins this down to `_candidate_databases_for_node`
    specifically (rather than `TimelineBuilder.candidate_databases_for_producers`,
    exercised by `test_same_triplet_at_same_date_raises` above) by matching on
    the node identity `_candidate_databases_for_node` includes in its message
    — name, reference product AND location — which only that path renders;
    `TimelineBuilder`'s equivalent message names only the producer, not its
    full triplet. This is about what's IN the message (the node under test),
    not incidental differences in how the two messages are worded.
    """
    collision = bd.Database("background_2020").new_node(
        "smelting_collision", name="smelting", unit="kg"
    )
    collision["reference product"] = "smelting"
    collision["location"] = "GLO"
    collision.save()
    collision.new_edge(input=collision, amount=1, type="production").save()
    bd.Database("background_2020").process()

    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    with pytest.raises(
        ValueError,
        match=r"'smelting' \(reference product: 'smelting', location: 'GLO'\)",
    ):
        tlca.build_timeline(
            starting_datetime="2025-01-01",
            graph_traversal="bfs",
            traverse_background=True,
        )


def test_partial_coverage_across_three_dates_interpolates_over_available(
    same_date_db_three_dates,
):
    """A producer present at two of three configured points in time must
    interpolate over the two it has, not collapse to a single candidate."""
    bd.Database("modified_2040").get("steel_without_eol").delete()
    bd.Database("modified_2040").process()

    database_dates = {
        "background_2020": datetime.strptime("2020", "%Y"),
        "background_2030": datetime.strptime("2030", "%Y"),
        "background_2040": datetime.strptime("2040", "%Y"),
        "modified_2020": datetime.strptime("2020", "%Y"),
        "modified_2030": datetime.strptime("2030", "%Y"),
        "modified_2040": datetime.strptime("2040", "%Y"),
        "foreground": "dynamic",
    }

    messages = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, database_dates)
        tlca.build_timeline(starting_datetime="2025-01-01")
    finally:
        logger.remove(sink_id)

    shares = _shares_by_producer(tlca.timeline)
    assert set(shares["steel, without EOL"]) == {"modified_2020", "modified_2030"}
    assert shares["steel, without EOL"]["modified_2020"] == pytest.approx(0.5, abs=0.01)
    assert any("steel, without EOL" in message for message in messages)


def test_nearest_interpolation_stays_within_each_family(same_date_db):
    """`interpolation_type="nearest"` must resolve each temporal market from
    within its own family of same-date databases, exercising the `nearest`
    branch of the per-producer loop with more than one database family
    present at once."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01", interpolation_type="nearest")
    shares = _shares_by_producer(tlca.timeline)

    assert shares["electricity"] in ({"background_2020": 1}, {"background_2030": 1})
    assert shares["steel, without EOL"] in (
        {"modified_2020": 1},
        {"modified_2030": 1},
    )


def test_background_traversal_partial_coverage_warns(same_date_deep_db):
    """A node reached mid-descent that exists in only one vintage of its
    family must still trigger the partial-coverage warning, just like a
    leaf temporal market does (`test_producer_in_a_single_vintage_warns_and_is_time_invariant`
    above). Without this, a node pinned to a single vintage during background
    traversal is silently pinned with no warning at all.
    """
    bd.Database("modified_2030").get("smelting").delete()
    bd.Database("modified_2030").process()

    messages = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
        tlca.build_timeline(
            starting_datetime="2025-01-01",
            graph_traversal="bfs",
            traverse_background=True,
        )
    finally:
        logger.remove(sink_id)

    assert any("smelting" in message for message in messages)


def test_interdatabase_mapping_is_filled_by_the_timeline_builder(same_date_db):
    """The builder's triplet scan feeds the mapping; no second scan is needed."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")

    steel_copy_2020 = bd.Database("modified_2020").get("steel_without_eol")
    steel_copy_2030 = bd.Database("modified_2030").get("steel_without_eol")
    assert (
        tlca.interdatabase_activity_mapping.find_match(steel_copy_2020.id, "modified_2030")
        == steel_copy_2030.id
    )

    electricity_2020 = bd.Database("background_2020").get("electricity")
    electricity_2030 = bd.Database("background_2030").get("electricity")
    assert (
        tlca.interdatabase_activity_mapping.find_match(electricity_2020.id, "background_2030")
        == electricity_2030.id
    )
    # The copy has no counterpart in the untouched family, and none is invented.
    with pytest.raises(KeyError):
        tlca.interdatabase_activity_mapping.find_match(steel_copy_2020.id, "background_2030")


def test_foreground_triplet_collision_does_not_affect_timeline_results(same_date_db):
    """A foreground node with a triplet collision is not added to the mapping.

    The optimized path reuses only static background database matches,
    deliberately excluding any foreground/dynamic databases. This test verifies
    that omitting a foreground triplet collision is inert: the foreground node is
    never queried through the mapping (only static db names are looked up), so
    the score and temporal market shares remain unchanged.
    """
    # Add a foreground node with the same (name, reference product, location)
    # as a background market producer.
    fg_collision = bd.Database("foreground").new_node(
        "electricity_fg_collision", name="electricity", unit="kWh"
    )
    fg_collision["reference product"] = "electricity"
    fg_collision["location"] = "GLO"
    fg_collision.save()
    fg_collision.new_edge(input=fg_collision, amount=1, type="production").save()
    bd.Database("foreground").process()

    # Build timeline and compute results.
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")

    # Verify the mapping only holds background databases (not the foreground).
    shares = tlca.timeline.loc[
        tlca.timeline["producer_name"] == "electricity", "temporal_market_shares"
    ].iloc[0]
    assert set(shares.keys()) == {"background_2020", "background_2030"}

    # Compute LCA and verify the results are unaffected by the foreground collision.
    tlca.lci()
    tlca.static_lcia()
    # electricity: 0.5*10 + 0.5*5 = 7.5; steel: 0.5*20 + 0.5*10 = 15
    assert tlca.static_score == pytest.approx(22.5, abs=0.2)
