import math
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA


# make sure the test db is loaded
def test_nonunitary_db_fixture(nonunitary_db):
    assert len(bd.databases) == 4


@pytest.mark.usefixtures("nonunitary_db")
class TestClass_EV:

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.node_a = bd.get_node(database="foreground", code="A")

        database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.node_a: 0.75},  # non-1 amount
            method=("GWP", "example"),
            database_dates=database_dates,
        )

        self.tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        self.tlca.lci()
        self.tlca.static_lcia()

    def test_non_unitary_timex_lca_score(self):

        expected_score = (  # D in bg_2020 and bg_2030 have the same emission score, so I can calculate the expected score without having to consider the temporal distribution
            0.75 / 0.8 * 1.5 / 3 * 0.5  # all ds at A
            + 0.75 / 0.8 * 4 / 7 * 0.9  # all direct CO2 emissions at all 3 bs
            + 0.75 / 0.8 * 4 / 7 * -2 / -1 * 6  # all direct CO2 emissions at all 3 cs
            + 0.75 / 0.8 * 4 / 7 * -2 / -1 * -1 / 3 * 0.5
        )  # all emissions of d via C at B

        assert math.isclose(self.tlca.static_score, expected_score, rel_tol=1e-7)
