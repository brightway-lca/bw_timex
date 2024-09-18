import math
from datetime import datetime

import bw2data as bd
import pytest
from bw2data.tests import bw2test

from bw_timex import TimexLCA

@pytest.fixture
@bw2test
def ab_db():
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "biosphere",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2030").write(
        {
            ("db_2030", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "B"),
                    },
                    {
                        "amount": 10,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("db_2020").write(
        {
            ("db_2020", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "B"),
                    },
                    {
                        "amount": 15,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1,
                        "input": ("db_2020", "B"),
                        "type": "technosphere",
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("bio", "CO2"), 1),
        ]
    )

def test_starting_datetime(ab_db):
    method = ("GWP", "example")
    database_date_dict = {
        "db_2020": datetime.strptime("2020", "%Y"),
        "db_2030": datetime.strptime("2030", "%Y"),
        "foreground": "dynamic",
    }
    fu = ("foreground", "A")
    tlca = TimexLCA({fu: 1}, method, database_date_dict)
    
    tlca.build_timeline(starting_datetime="2020-01-01")
    tlca.lci()
    tlca.static_lcia()
    assert math.isclose(tlca.static_score, 15, rel_tol=1e-9)
    
    tlca.build_timeline(starting_datetime="2030-01-01")
    tlca.lci()
    tlca.static_lcia()
    assert math.isclose(tlca.static_score, 10, rel_tol=1e-9)

    tlca.build_timeline(starting_datetime="2025-01-01")
    tlca.lci()
    tlca.static_lcia()
    assert math.isclose(tlca.static_score, 12.5, rel_tol=1e-3)
