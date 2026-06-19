from datetime import datetime

import bw2data as bd

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def test_fixture_loads(background_td_db):
    assert "background_2020" in bd.databases
    assert "background_2030" in bd.databases
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score > 0
