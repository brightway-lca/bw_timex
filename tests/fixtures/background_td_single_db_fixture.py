import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_single_db():
    """fu -> electricity -> fuel -> CO2 with only ONE background database.

    The background carries an internal temporal distribution (electricity ->
    fuel), but there is a single time-representative background database, so
    every flow - whenever it occurs - is sourced from that same database. The
    background TD therefore only redistributes emissions in time; the
    time-aggregated (static) score must be unchanged. Used to assert
    ``traverse_background=True`` equals the plain static LCA score.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_electricity_fuel = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    bd.Database("background").write(
        {
            ("background", "electricity"): {
                "name": "electricity",
                "unit": "kWh",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("background", "electricity"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("background", "fuel"),
                        "amount": 2,
                        "type": "technosphere",
                        "temporal_distribution": td_electricity_fuel,
                    },
                ],
            },
            ("background", "fuel"): {
                "name": "fuel",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "fuel"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 11, "type": "biosphere"},
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
                        "input": ("background", "electricity"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )

    for db in bd.databases:
        bd.Database(db).process()
