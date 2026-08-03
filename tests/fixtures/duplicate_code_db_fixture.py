import bw2data as bd
import pytest
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def duplicate_code_db():
    """fu -> grid, where an unrelated database re-uses the foreground code "fu".

    Codes are only unique *within* a database in Brightway, so any project can
    contain several nodes sharing a code (e.g. a benchmark/scenario copy of a
    foreground). The decoy database is written *before* the foreground so its
    node has the lower id, i.e. a lookup that simply takes the first match by
    code picks the wrong node and its (very different) biosphere exchange.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    # Decoy: same code as the foreground node, different biosphere amount.
    # Not part of the studied product system and not in `database_dates`.
    bd.Database("decoy").write(
        {
            ("decoy", "fu"): {
                "name": "fu",
                "unit": "unit",
                "location": "GLO",
                "reference product": "fu",
                "exchanges": [
                    {"input": ("decoy", "fu"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 1000, "type": "biosphere"},
                ],
            }
        }
    )

    for db_name, co2_amount in (("background_2020", 10), ("background_2030", 2)):
        bd.Database(db_name).write(
            {
                (db_name, "grid"): {
                    "name": "grid",
                    "unit": "kWh",
                    "location": "GLO",
                    "reference product": "electricity",
                    "exchanges": [
                        {
                            "input": (db_name, "grid"),
                            "amount": 1,
                            "type": "production",
                        },
                        {
                            "input": ("bio", "co2"),
                            "amount": co2_amount,
                            "type": "biosphere",
                        },
                    ],
                }
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
                    {"input": ("bio", "co2"), "amount": 5, "type": "biosphere"},
                    {
                        "input": ("background_2020", "grid"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )

    for db in bd.databases:
        bd.Database(db).process()
