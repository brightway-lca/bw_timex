"""
Testing if the temporal grouping options (year, month, day and hour) work correctly.
"""

from pathlib import Path
import pytest
import bw2calc as bc
import bw2data as bd
from bw_timex import TimexLCA
from datetime import datetime
from bw_temporalis import TemporalDistribution
import numpy as np


# make sure the test db is loaded
def test_temporal_grouping_db_fixture(temporal_grouping_db):
    assert len(bd.databases) == 4


@pytest.mark.usefixtures("temporal_grouping_db")
class TestClass_Grouping:

    # yearly resolution is alreday tested in test_electric_vehicle.py

    def test_monthly_resolution_score(self):

        self.fu = bd.get_node(database="foreground", code="A")

        database_date_dict = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.fu.key: 1},
            method=("GWP", "example"),
            database_date_dict=database_date_dict,
        )
        self.tlca.build_timeline(
            temporal_grouping="month",
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        self.tlca.lci()
        self.tlca.static_lcia()
        print(self.tlca.timeline)

        expected_score = 1 / 3 * 15 + 1 / 6 * 15 + 1 / 6 * 10 + 1 / 3 * 10

        assert self.tlca.score == expected_score

    def test_daily_resolution_score(self):

        # first change TD in database to daily (although not strictly necessary)
        self.fu = bd.get_node(database="foreground", code="A")

        for exc in self.fu.exchanges():
            if exc.input["name"] == "B":
                exc["temporal_distribution"] = TemporalDistribution(
                    np.array(
                        [
                            -15,
                            -10,
                            0,
                        ],
                        dtype="timedelta64[D]",
                    ),
                    np.array([1 / 3, 1 / 3, 1 / 3]),
                )
                exc.save()

        database_date_dict = {
            "db_2022": datetime.strptime(
                "2024-01-10", "%Y-%m-%d"
            ),  # give dbs also a daily resolution (makes calculating the expected score easier)
            "db_2024": datetime.strptime("2024-01-25", "%Y-%m-%d"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.fu.key: 1},
            method=("GWP", "example"),
            database_date_dict=database_date_dict,
        )
        self.tlca.build_timeline(
            temporal_grouping="day",
            starting_datetime=datetime.strptime("2024-01-25", "%Y-%m-%d"),
        )
        self.tlca.lci()
        self.tlca.static_lcia()
        print(self.tlca.timeline)

        expected_score = 1 / 3 * 15 + 2 / 9 * 15 + 1 / 9 * 10 + 1 / 3 * 10

        assert self.tlca.static_score == pytest.approx(expected_score, rel=0.00005)

    def test_hourly_resolution_score(self):

        # first change TD in database to hourly (although not strictly necessary)
        self.fu = bd.get_node(database="foreground", code="A")

        for exc in self.fu.exchanges():
            if exc.input["name"] == "B":
                exc["temporal_distribution"] = TemporalDistribution(
                    np.array(
                        [
                            -15,
                            -10,
                            0,
                        ],
                        dtype="timedelta64[h]",
                    ),
                    np.array([1 / 3, 1 / 3, 1 / 3]),
                )
                exc.save()

        database_date_dict = {
            "db_2022": datetime.strptime(
                "2024-01-10 06:00", "%Y-%m-%d %H:%M"
            ),  # give dbs also a hourly resolution (makes calculating the expected score easier)
            "db_2024": datetime.strptime("2024-01-10 21:00", "%Y-%m-%d %H:%M"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.fu.key: 1},
            method=("GWP", "example"),
            database_date_dict=database_date_dict,
        )
        self.tlca.build_timeline(
            temporal_grouping="hour",
            starting_datetime=datetime.strptime("2024-01-10 21:00", "%Y-%m-%d %H:%M"),
        )
        self.tlca.lci()
        self.tlca.static_lcia()
        print(self.tlca.timeline)

        expected_score = 1 / 3 * 15 + 2 / 9 * 15 + 1 / 9 * 10 + 1 / 3 * 10

        assert self.tlca.static_score == pytest.approx(expected_score, rel=0.00005)
