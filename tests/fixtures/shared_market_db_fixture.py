import bw2data as bd
import pytest
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def shared_market_db():
    """fu -> {fg_A, fg_B} -> bg_X -> CO2.

    Two foreground processes consume the same background node at the same time,
    so the timeline holds two rows sharing one time-mapped temporal market.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name, co2_amount in [("background_2020", 1.0), ("background_2030", 0.5)]:
        bd.Database(db_name).write(
            {
                (db_name, "bg_X"): {
                    "name": "bg_X",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg_X"), "amount": 1, "type": "production"},
                        {
                            "input": ("bio", "co2"),
                            "amount": co2_amount,
                            "type": "biosphere",
                        },
                    ],
                },
            }
        )

    bd.Database("foreground").write(
        {
            ("foreground", "fu"): {
                "name": "fu",
                "unit": "unit",
                "location": "GLO",
                "reference product": "fu",
                "exchanges": [
                    {"input": ("foreground", "fu"), "amount": 1, "type": "production"},
                    {
                        "input": ("foreground", "fg_A"),
                        "amount": 1,
                        "type": "technosphere",
                    },
                    {
                        "input": ("foreground", "fg_B"),
                        "amount": 1,
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "fg_A"): {
                "name": "fg_A",
                "unit": "unit",
                "location": "GLO",
                "reference product": "fg_A",
                "exchanges": [
                    {"input": ("foreground", "fg_A"), "amount": 1, "type": "production"},
                    {
                        "input": ("background_2020", "bg_X"),
                        "amount": 2,
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "fg_B"): {
                "name": "fg_B",
                "unit": "unit",
                "location": "GLO",
                "reference product": "fg_B",
                "exchanges": [
                    {"input": ("foreground", "fg_B"), "amount": 1, "type": "production"},
                    {
                        "input": ("background_2020", "bg_X"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                ],
            },
        }
    )

    for db in bd.databases:
        bd.Database(db).process()
