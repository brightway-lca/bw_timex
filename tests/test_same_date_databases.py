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
