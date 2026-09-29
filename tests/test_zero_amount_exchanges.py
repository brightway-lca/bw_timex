"""A technosphere exchange with amount 0 carries no mass. Traversal must skip it,
as the matrix does, instead of convolving it into an empty TemporalDistribution
(`temporal_convolution` drops zero entries, and an empty TD raises)."""

from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}

W_2030 = round(
    (datetime(2024, 1, 1) - datetime(2020, 1, 1)).days
    / (datetime(2030, 1, 1) - datetime(2020, 1, 1)).days,
    3,
)
W_2020 = 1 - W_2030
# Only bg_A counts; bg_C sits behind the zero edge.
EXPECTED = W_2020 * 1.0 + W_2030 * 0.5


def _tlca(graph_traversal, traverse_background):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=traverse_background,
    )
    tlca.lci()
    tlca.static_lcia()
    return tlca


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
@pytest.mark.parametrize("traverse_background", [False, True])
def test_zero_amount_exchange_is_skipped(
    zero_amount_exchange_db, graph_traversal, traverse_background
):
    tlca = _tlca(graph_traversal, traverse_background)
    assert tlca.static_score == pytest.approx(EXPECTED, rel=1e-9)

    producers = {bd.get_node(id=id_)["name"] for id_ in tlca.timeline.producer}
    assert "bg_C" not in producers
