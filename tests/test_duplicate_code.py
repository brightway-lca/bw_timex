"""Codes are only unique within a database, not within a project."""

from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


@pytest.mark.usefixtures("duplicate_code_db")
def test_biosphere_exchanges_of_duplicated_code_come_from_the_right_database():
    """An unrelated database sharing a foreground code must not confuse the
    dynamic biosphere build: neither raising `MultipleResults` nor silently
    reading the decoy node's exchanges."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01")
    tlca.lci()
    tlca.static_lcia()

    # 5 kg CO2 direct from fu + 3 kWh grid, interpolated between the 2020
    # (10 kg/kWh) and 2030 (2 kg/kWh) database. The decoy node emits 1000 kg
    # and must not contribute at all.
    assert tlca.static_score < 100
    assert tlca.dynamic_inventory.sum() == pytest.approx(tlca.static_score)
