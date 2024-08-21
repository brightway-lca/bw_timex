from datetime import datetime

import bw2data as bd
import numpy as np
import pandas as pd
import pytest

from bw_timex import TimexLCA


# make sure the test db is loaded
def test_dynamic_biomatrix_fixture(dynamic_biomatrix_db):
    assert len(bd.databases) == 3


@pytest.mark.usefixtures("dynamic_biomatrix_db")
class TestClass_dynamic_biomatrix:

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.node_a = bd.get_node(database="foreground", code="A")

        database_date_dict = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.node_a: 1},
            method=("GWP", "example"),
            database_date_dict=database_date_dict,
        )

        self.tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        self.tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)
        self.tlca.static_lcia()

    def test_identical_time_mapped_producers_exist_in_timeline(self):

        duplicates = self.tlca.timeline["time_mapped_producer"].duplicated().any()
        assert duplicates, "No duplicates found in column time_mapped_producer"

    def test_dynamic_biomatrix_for_multiple_identical_time_mapped_producers_in_timeline(
        self,
    ):

        expected_dynamic_biomatrix_as_array = np.array(
            [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.5]]
        )  # 1.5 CO2 is only added once for the time_mapped producer, depsite it occuring twice in the timeline! Before #78, it was added twice , resulting in 3.0

        assert np.array_equal(
            self.tlca.dynamic_biomatrix.toarray(), expected_dynamic_biomatrix_as_array
        ), "Arrays are not equal"
