"""Several biosphere exchanges of one activity to the same flow are summed by
bw2calc, and must be summed in the dynamic inventory too - not deduplicated.
Deduplication is only right for repeated *visits* of one activity, which happen
when several timeline rows share a time-mapped producer."""

from datetime import datetime

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

# fu needs 1 + 2 = 3 units of A. Each emits -1 + 3 = 2 kg CO2 itself and
# consumes 1 kg of bg, whose CO2 is interpolated between the two vintages.
A_SUPPLY = 3
FOREGROUND_CO2 = A_SUPPLY * 2
EXPECTED = FOREGROUND_CO2 + A_SUPPLY * (W_2020 * 1.0 + W_2030 * 0.5)

SETTINGS = pytest.mark.parametrize(
    "expand_technosphere, keep_activity_dimension",
    [(True, True), (True, False), (False, True), (False, False)],
)


def _tlca(expand_technosphere, keep_activity_dimension):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01")
    tlca.lci(
        expand_technosphere=expand_technosphere,
        build_dynamic_biosphere=True,
        keep_activity_dimension=keep_activity_dimension,
    )
    tlca.static_lcia()
    return tlca


@SETTINGS
def test_duplicate_biosphere_exchanges_are_summed(
    duplicate_biosphere_exchange_db, expand_technosphere, keep_activity_dimension
):
    tlca = _tlca(expand_technosphere, keep_activity_dimension)
    assert tlca.dynamic_inventory.sum() == pytest.approx(EXPECTED, rel=1e-9)
    assert tlca.static_score == pytest.approx(EXPECTED, rel=1e-9)


@SETTINGS
def test_duplicate_biosphere_exchanges_with_td_are_summed(
    duplicate_biosphere_exchange_td_db, expand_technosphere, keep_activity_dimension
):
    """Both exchanges share a TD, so they hit the same (flow, date) rows."""
    tlca = _tlca(expand_technosphere, keep_activity_dimension)
    assert tlca.dynamic_inventory.sum() == pytest.approx(EXPECTED, rel=1e-9)

    per_date = tlca.dynamic_inventory_df.groupby("date")["amount"].sum().sort_index()
    background_co2 = EXPECTED - FOREGROUND_CO2
    assert len(per_date) == 2
    assert per_date.iloc[0] == pytest.approx(
        FOREGROUND_CO2 / 2 + background_co2, rel=1e-9
    )
    assert per_date.iloc[1] == pytest.approx(FOREGROUND_CO2 / 2, rel=1e-9)
