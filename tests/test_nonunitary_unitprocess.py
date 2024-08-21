import math
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA


# make sure the test db is loaded
def test_nonunitary_db_fixture(nonunitary_db):
    assert len(bd.databases) == 3


@pytest.mark.usefixtures("nonunitary_db")
class TestClass_EV:

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
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        self.tlca.lci()
        self.tlca.static_lcia()

    def test_timex_lca_score(self):

        expected_score = (
            1 * 1.5 / 3 * 0.5 + 1 * 4 / 7 * 0.9  # A -> Nonunitary  C in bd_2020
        )  # A -> B -> Nonunitary B in foreground

        false_score = (
            1 * 1.5 / 3 * 0.5 + 1 * 4 * 0.9  # A -> Nonunitary C in bd_2020
        )  # A -> B -> Nonunitary B in foreground (not scaled by production amount!)

        print(false_score)

        assert math.isclose(self.tlca.score, expected_score, rel_tol=1e-9)
