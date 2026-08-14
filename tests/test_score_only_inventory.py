"""`keep_activity_dimension=False` drops the per-activity columns of the
dynamic inventory and accumulates emissions per (flow, time) only. Scores must
be identical; only contribution analysis by activity is given up."""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _tlca(expand_technosphere, keep_activity_dimension, traverse_background=False):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01", traverse_background=traverse_background
    )
    tlca.lci(
        expand_technosphere=expand_technosphere,
        build_dynamic_biosphere=True,
        keep_activity_dimension=keep_activity_dimension,
    )
    tlca.static_lcia()
    return tlca


@pytest.mark.parametrize("expand_technosphere", [False, True])
@pytest.mark.parametrize("traverse_background", [False, True])
def test_scores_are_unchanged(
    background_td_db, expand_technosphere, traverse_background
):
    full = _tlca(expand_technosphere, True, traverse_background)
    lean = _tlca(expand_technosphere, False, traverse_background)

    assert full.static_score > 0
    assert lean.static_score == pytest.approx(full.static_score, rel=1e-9)
    assert lean.dynamic_inventory.sum() == pytest.approx(
        full.dynamic_inventory.sum(), rel=1e-9
    )


@pytest.mark.parametrize("expand_technosphere", [False, True])
def test_activity_dimension_is_dropped(background_td_db, expand_technosphere):
    lean = _tlca(expand_technosphere, False)
    assert lean.dynamic_biosphere_matrix.shape[1] == 1
    assert lean.dynamic_inventory.shape[1] == 1
    assert lean.dynamic_inventory_df["activity"].nunique() == 1


def test_emissions_over_time_are_unchanged(background_td_db):
    """Only the activity dimension goes; the timing of the emissions stays."""
    full = _tlca(False, True)
    lean = _tlca(False, False)

    full_over_time = full.dynamic_inventory_df.groupby(["date", "flow"])[
        "amount"
    ].sum()
    lean_over_time = lean.dynamic_inventory_df.groupby(["date", "flow"])["amount"].sum()
    assert set(full_over_time.index) == set(lean_over_time.index)
    for key, amount in full_over_time.items():
        assert lean_over_time[key] == pytest.approx(amount, rel=1e-9)
