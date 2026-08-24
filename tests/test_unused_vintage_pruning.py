"""Vintages the timeline never sources from must not be loaded by `lci()`.

`TimexLCA` maps every database that declares a point in time, but a study only
ever draws on the vintages its timeline actually reaches. The others contribute
no matrix entries - the expanded technosphere only references databases that
appear in a row's `temporal_market_shares` - so loading their datapackages only
inflates the matrix that has to be solved.
"""

from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA

DATES_ALL = {
    "background_2020": datetime(2020, 1, 1),
    "modified_2020": datetime(2020, 1, 1),
    "background_2030": datetime(2030, 1, 1),
    "modified_2030": datetime(2030, 1, 1),
    "background_2040": datetime(2040, 1, 1),
    "modified_2040": datetime(2040, 1, 1),
    "foreground": "dynamic",
}
DATES_USED = {k: v for k, v in DATES_ALL.items() if not k.endswith("2040")}


def _run(database_dates):
    tlca = TimexLCA(
        demand={("foreground", "fu"): 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(starting_datetime="2026-01-01")
    tlca.lci()
    tlca.static_lcia()
    return tlca


def _node_ids(database):
    return {node.id for node in bd.Database(database)}


@pytest.mark.usefixtures("same_date_db_three_dates")
class TestUnusedVintagePruning:

    def test_unused_vintage_is_not_in_the_expanded_matrix(self):
        """The 2040 vintage gets no temporal market share for a 2026 demand."""
        tlca = _run(DATES_ALL)
        used_databases = set()
        for shares in tlca.timeline["temporal_market_shares"]:
            if shares:
                used_databases.update(shares)
        assert "background_2040" not in used_databases

        in_matrix = set(tlca.lca.dicts.activity) & _node_ids("background_2040")
        assert not in_matrix

    def test_pruning_does_not_change_the_score(self):
        assert _run(DATES_ALL).static_score == pytest.approx(
            _run(DATES_USED).static_score
        )

    def test_used_vintages_stay_in_the_matrix(self):
        tlca = _run(DATES_ALL)
        for database in ("background_2020", "background_2030"):
            assert set(tlca.lca.dicts.activity) & _node_ids(database)
