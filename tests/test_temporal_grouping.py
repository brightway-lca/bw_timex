"""
Testing if the temporal grouping options (year, month, day and hour) work correctly.
"""

from datetime import datetime
from pathlib import Path

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution

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


def _write_monthly_chain_db():
    """A -> B -> C, both edges spread evenly over 12 months; C emits 10 kg CO2."""
    bd.Database("bio").write({("bio", "CO2"): {"type": "biosphere", "name": "CO2"}})
    bd.Database("db_2024").write(
        {
            ("db_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2024", "C")},
                    {"amount": 10, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            }
        }
    )

    def td():
        return TemporalDistribution(
            np.arange(0, 12).astype("timedelta64[M]"), np.full(12, 1 / 12)
        )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("foreground", "A")},
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": td(),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("foreground", "B")},
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("db_2024", "C"),
                        "temporal_distribution": td(),
                    },
                ],
            },
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "CO2"), 1)])


@pytest.mark.parametrize("expand_technosphere", [True, False])
@pytest.mark.parametrize("temporal_grouping", ["month", "year"])
@bw2test
def test_grouping_coarser_than_temporal_distribution(
    temporal_grouping, expand_technosphere
):
    # A consumer that occurs several times in one time window becomes one process
    # there, so its inputs are averaged over those occurrences, not summed (#84).
    _write_monthly_chain_db()
    tlca = TimexLCA(
        demand={("foreground", "A"): 1},
        method=("GWP", "example"),
        database_dates={"db_2024": datetime(2024, 1, 1), "foreground": "dynamic"},
    )
    tlca.build_timeline(
        temporal_grouping=temporal_grouping, starting_datetime=datetime(2024, 1, 1)
    )
    tlca.lci(expand_technosphere=expand_technosphere)
    tlca.static_lcia()

    assert tlca.static_score == pytest.approx(10)
