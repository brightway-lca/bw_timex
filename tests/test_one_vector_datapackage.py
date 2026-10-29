"""The matrix modifications go into the datapackage as one vector per matrix.

One resource per matrix entry made every lookup in `bw2calc` scan all of them,
so loading the expanded matrices grew with the square of the timeline length.
With one vector, an entry repeated across timeline rows (here: a temporal
market shared by two consumers) must still be set once, not summed.
"""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _tlca(expand_technosphere):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01")
    tlca.lci(expand_technosphere=expand_technosphere)
    tlca.static_lcia()
    return tlca


def test_one_vector_per_matrix(shared_market_db):
    tlca = _tlca(True)
    technosphere, biosphere = tlca.datapackage
    # one vector is three resources: indices, data and flip
    assert len(technosphere.resources) == 3
    # this fixture's foreground has no biosphere flows of its own
    assert len(biosphere.resources) in (0, 3)


def test_shared_market_entry_is_not_summed(shared_market_db):
    assert _tlca(True).static_score == pytest.approx(
        _tlca(False).static_score, rel=1e-9
    )
