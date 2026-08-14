"""`static_lcia` must also work when the dynamic inventory was built directly
from the timeline (`expand_technosphere=False`): the time-explicit inventory is
there, it just has to be characterized with the static factors."""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _tlca(traverse_background):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        traverse_background=traverse_background,
    )
    return tlca


@pytest.mark.parametrize("traverse_background", [False, True])
def test_static_score_from_timeline_matches_expanded(
    background_td_db, traverse_background
):
    expanded = _tlca(traverse_background)
    expanded.lci(expand_technosphere=True)
    expanded.static_lcia()

    from_timeline = _tlca(traverse_background)
    from_timeline.lci(expand_technosphere=False, build_dynamic_biosphere=True)
    from_timeline.static_lcia()

    assert expanded.static_score > 0
    assert from_timeline.static_score == pytest.approx(
        expanded.static_score, rel=1e-9
    )


def test_static_score_before_static_lcia_raises(background_td_db):
    tlca = _tlca(False)
    tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
    with pytest.raises(AttributeError, match="static_lcia"):
        tlca.static_score
