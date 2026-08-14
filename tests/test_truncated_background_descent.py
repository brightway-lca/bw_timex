"""When the background descent stops early (``max_calc`` budget or ``cutoff``),
the nodes at the truncation frontier are leaves: nothing downstream of them is
in the timeline. They must therefore be sourced as temporal markets, i.e. with
their full background LCI, exactly like the background frontier of a run
without ``traverse_background``. Otherwise everything upstream of the
truncation is silently lost from the inventory."""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _score(max_calc, traverse_background, graph_traversal):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=traverse_background,
        max_calc=max_calc,
    )
    tlca.lci()
    tlca.static_lcia()
    return tlca.static_score


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_truncated_descent_keeps_full_inventory(
    background_td_deep_chain_db, graph_traversal
):
    """The whole chain's CO2 sits at its deepest node, which a small max_calc
    cuts off. The score must not depend on how far the descent got."""
    full = _score(10_000, True, graph_traversal)
    truncated = _score(3, True, graph_traversal)
    assert full > 0
    assert truncated == pytest.approx(full, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_truncated_descent_matches_static_background(
    background_td_deep_chain_db, graph_traversal
):
    """A descent truncated right at the first background node must give the
    same score as not traversing the background at all."""
    static_background = _score(10_000, False, graph_traversal)
    truncated = _score(3, True, graph_traversal)
    assert truncated == pytest.approx(static_background, rel=1e-9)
