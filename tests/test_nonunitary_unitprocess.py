import math
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA


# make sure the test db is loaded
def test_nonunitary_db_fixture(nonunitary_db):
    assert len(bd.databases) == 3


EXPECTED_SCORE = (  #
    0.75 / 0.8 * 1.5 / 3 * 0.5  # all d at A
    + 0.75 / 0.8 * 4 / 7 * 0.9  # all direct CO2 emissions at all 3 b
    + 0.75 / 0.8 * 4 / 7 * -2 / -1 * 6  # all direct CO2 emissions at all 3 c
    + 0.75 / 0.8 * 4 / 7 * -2 / -1 * -1 / 3 * 0.5  # all d via C at B
)


@pytest.mark.usefixtures("nonunitary_db")
class TestClass_EV:

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.node_a = bd.get_node(database="foreground", code="A")

        database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
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

        assert math.isclose(self.tlca.static_score, EXPECTED_SCORE, rel_tol=1e-7)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
@pytest.mark.usefixtures("nonunitary_db")
def test_non_unitary_production_amounts_from_timeline(graph_traversal):
    """Building the dynamic inventory from the timeline (`expand_technosphere=False`)
    must scale each process by the same supply the expanded solve would give it.

    `timeline.cumulative_amount` normalizes every edge by the *consumer's*
    absolute production amount, while a linear solve scales each process by its
    *own, signed* production amount. Processes whose production amount is not
    +1 (here: c produces -1, b produces 7, d produces 3) therefore ended up with
    wrong supplies - for negative production amounts even with a flipped sign.
    """
    node_a = bd.get_node(database="foreground", code="A")
    tlca = TimexLCA(
        demand={node_a: 0.75},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(
        starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        graph_traversal=graph_traversal,
    )
    tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)

    # CO2 is the only flow and its characterization factor is 1
    assert math.isclose(
        tlca.dynamic_inventory.sum(), EXPECTED_SCORE, rel_tol=1e-7
    )
