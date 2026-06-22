from datetime import datetime

import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def explicit_background_td_db():
    """Explicit process/product foreground pulling a background chain that
    carries an internal temporal distribution, across two dated variants.

    Foreground (explicit paradigm): a bare ``service_product`` plus a
    ``service_process`` that owns the production edge to it and a technosphere
    edge to background ``electricity``. The demand is the *product*.

    Background (two variants, 2020 / 2040): ``electricity -> fuel`` (amount 2)
    carries a TD (60% +0y, 40% +10y); ``fuel -> CO2`` decarbonizes (11 kg in
    2020, 7 kg in 2040). With ``traverse_background=True`` the traversal descends
    into ``electricity`` (temporalized), the ``electricity -> fuel`` TD spreads
    fuel over time, and each fuel cohort is routed to the temporally appropriate
    grid variant.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_electricity_fuel = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    co2_per_variant = {"bg_2020": 11, "bg_2040": 7}

    for dbname, co2 in co2_per_variant.items():
        bd.Database(dbname).write(
            {
                (dbname, "electricity"): {
                    "name": "electricity",
                    "unit": "kWh",
                    "location": "GLO",
                    "exchanges": [
                        {
                            "input": (dbname, "electricity"),
                            "amount": 1,
                            "type": "production",
                        },
                        {
                            "input": (dbname, "fuel"),
                            "amount": 2,
                            "type": "technosphere",
                            "temporal_distribution": td_electricity_fuel,
                        },
                    ],
                },
                (dbname, "fuel"): {
                    "name": "fuel",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (dbname, "fuel"), "amount": 1, "type": "production"},
                        {"input": ("bio", "co2"), "amount": co2, "type": "biosphere"},
                    ],
                },
            }
        )

    bd.Database("foreground").write(
        {
            ("foreground", "service_product"): {
                "name": "service product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
            ("foreground", "service_process"): {
                "name": "service process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "service_product"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("bg_2020", "electricity"),
                        "amount": 3,
                        "type": "technosphere",
                    },
                ],
            },
        }
    )

    for db in bd.databases:
        bd.Database(db).process()

    return {
        "bg_2020": datetime(2020, 1, 1),
        "bg_2040": datetime(2040, 1, 1),
        "foreground": "dynamic",
    }
