"""A node cohort that receives zero supply is dropped from the timeline
(`amount != 0`), but the edges *out* of that cohort can still carry non-zero
exchange amounts. Those orphaned edges must not be looked up in the activity
time mapping, where their consumer was never registered."""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
STARTING_DATETIME = "2024-01-01"


def _score(graph_traversal):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime=STARTING_DATETIME,
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lcia()
    return tlca.static_score


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_zero_supply_cohort_does_not_break_timeline(
    zero_weight_background_td_db, graph_traversal
):
    # 2024 is interpolated between the 2020 (1 kg CO2) and 2030 (0.5) vintages,
    # with weights rounded to 3 decimals by `linear_interpolation_weights`.
    w_2030 = round(
        (datetime(2024, 1, 1) - datetime(2020, 1, 1)).days
        / (datetime(2030, 1, 1) - datetime(2020, 1, 1)).days,
        3,
    )
    expected = (1 - w_2030) * 1.0 + w_2030 * 0.5
    assert _score(graph_traversal) == pytest.approx(expected, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_zero_weight_date_is_equivalent_to_leaving_it_out(
    request, graph_traversal
):
    request.getfixturevalue("zero_weight_background_td_db")
    with_zero = _score(graph_traversal)
    request.getfixturevalue("single_date_background_td_db")
    without_zero = _score(graph_traversal)
    assert with_zero == pytest.approx(without_zero, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_all_zero_cohort_does_not_break_the_descent(
    zero_weight_first_background_td_db, graph_traversal
):
    """The zero-weight date routes to a different background variant than the
    non-zero one, so one variant's cohort is all zeros. Convolving it away
    yields an empty temporal distribution, which must not blow up the descent:
    the cohort simply carries nothing."""
    # All the supply happens in 2034, i.e. is sourced from the 2030 vintage.
    assert _score(graph_traversal) == pytest.approx(0.5, rel=1e-9)
