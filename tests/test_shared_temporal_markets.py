"""Building the dynamic inventory from the timeline (`expand_technosphere=False`)
gives every timeline row its own matrix column. Rows that share a time-mapped
temporal market carry the same background LCI, so storing it once per row is
pure duplication - real background systems reach hundreds of thousands of such
rows. They are collapsed onto one column, with their supplies summed."""

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
    tlca.lci(expand_technosphere=expand_technosphere, build_dynamic_biosphere=True)
    tlca.static_lcia()
    return tlca


def test_shared_market_score_is_unchanged(shared_market_db):
    """fg_A and fg_B together pull 5 kg of bg_X through one shared market."""
    from_timeline = _tlca(False)
    expanded = _tlca(True)
    assert from_timeline.static_score == pytest.approx(
        expanded.static_score, rel=1e-9
    )
    assert from_timeline.dynamic_inventory_df["amount"].sum() == pytest.approx(
        expanded.dynamic_inventory_df["amount"].sum(), rel=1e-9
    )


def test_shared_market_uses_a_single_column(shared_market_db):
    """Both bg_X rows are served by one column, not one each."""
    tlca = _tlca(False)
    timeline = tlca.timeline
    market_rows = timeline.index[timeline["temporal_market_shares"].notna()]
    assert len(market_rows) == 2, "fixture must produce two rows sharing one market"
    assert timeline.loc[market_rows, "time_mapped_producer"].nunique() == 1

    matrix = tlca.dynamic_biosphere_matrix.tocsc()
    filled = [col for col in market_rows if matrix[:, col].nnz]
    assert len(filled) == 1
