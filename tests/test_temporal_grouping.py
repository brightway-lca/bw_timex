"""
Testing if the temporal grouping options (year, month, day and hour) work correctly.
"""

from datetime import datetime
from pathlib import Path

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
def test_monthly_resolution_score():

    fu = bd.get_node(database="foreground", code="A")

    database_dates = {
        "db_2022": datetime.strptime("2022", "%Y"),
        "db_2024": datetime.strptime("2024", "%Y"),
        "foreground": "dynamic",
    }

    tlca = TimexLCA(
        demand={fu.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(
        temporal_grouping="month",
        starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
    )
    tlca.lci()
    tlca.static_lcia()
    print(tlca.timeline)

    expected_score = 1 / 3 * 15 + 1 / 6 * 15 + 1 / 6 * 10 + 1 / 3 * 10

    assert tlca.static_score == expected_score


@pytest.mark.usefixtures("temporal_grouping_db_daily")
def test_daily_resolution_score():

    # first change TD in database to daily (although not strictly necessary)
    fu = bd.get_node(database="foreground", code="A")

    database_dates = {
        "db_2022": datetime.strptime(
            "2024-01-10", "%Y-%m-%d"
        ),  # give dbs also a daily resolution (makes calculating the expected score easier)
        "db_2024": datetime.strptime("2024-01-25", "%Y-%m-%d"),
        "foreground": "dynamic",
    }

    tlca = TimexLCA(
        demand={fu.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(
        temporal_grouping="day",
        starting_datetime=datetime.strptime("2024-01-25", "%Y-%m-%d"),
    )
    tlca.lci()
    tlca.static_lcia()
    print(tlca.timeline)

    expected_score = 1 / 3 * 15 + 2 / 9 * 15 + 1 / 9 * 10 + 1 / 3 * 10

    assert tlca.static_score == pytest.approx(expected_score, rel=0.00005)


@pytest.mark.usefixtures("temporal_grouping_db_hourly")
def test_hourly_resolution_score():

    # first change TD in database to hourly (although not strictly necessary)
    fu = bd.get_node(database="foreground", code="A")

    database_dates = {
        "db_2022": datetime.strptime(
            "2024-01-10 06:00", "%Y-%m-%d %H:%M"
        ),  # give dbs also a hourly resolution (makes calculating the expected score easier)
        "db_2024": datetime.strptime("2024-01-10 21:00", "%Y-%m-%d %H:%M"),
        "foreground": "dynamic",
    }

    tlca = TimexLCA(
        demand={fu.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(
        temporal_grouping="hour",
        starting_datetime=datetime.strptime("2024-01-10 21:00", "%Y-%m-%d %H:%M"),
    )
    tlca.lci()
    tlca.static_lcia()
    print(tlca.timeline)

    expected_score = 1 / 3 * 15 + 2 / 9 * 15 + 1 / 9 * 10 + 1 / 3 * 10

    assert tlca.static_score == pytest.approx(expected_score, rel=0.00005)
