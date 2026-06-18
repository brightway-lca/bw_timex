import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def vehicle_explicit_db():
    """
    Same EV system as ``vehicle_db`` but with the foreground modeled in the
    explicit process/product paradigm (separate product node + process node)
    instead of a chimaera node.
    """
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2020").write(
        {
            ("db_2020", "glider"): {
                "name": "market for glider, passenger car",
                "location": "somewhere",
                "reference product": "market for glider, passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "glider")},
                    {"amount": 6.29, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2020", "powertrain"): {
                "name": "market for powertrain, for electric passenger car",
                "location": "somewhere",
                "reference product": "market for powertrain, for electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "powertrain")},
                    {"amount": 17.89, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2020", "battery"): {
                "name": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "location": "somewhere",
                "reference product": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "battery")},
                    {"amount": 8.23, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2020", "electricity"): {
                "name": "market group for electricity, low voltage",
                "location": "somewhere",
                "reference product": "market group for electricity, low voltage",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "electricity")},
                    {"amount": 0.73, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2020", "dismantling"): {
                "name": "market for manual dismantling of used electric passenger car",
                "location": "somewhere",
                "reference product": "market for manual dismantling of used electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "dismantling")},
                    {"amount": 0.0091, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2020", "battery_recycling"): {
                "name": "market for used Li-ion battery",
                "location": "somewhere",
                "reference product": "market for used Li-ion battery",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2020", "battery_recycling")},
                    {"amount": -1.18, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
        }
    )

    bd.Database("db_2030").write(
        {
            ("db_2030", "glider"): {
                "name": "market for glider, passenger car",
                "location": "somewhere",
                "reference product": "market for glider, passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "glider")},
                    {"amount": 4.37, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2030", "powertrain"): {
                "name": "market for powertrain, for electric passenger car",
                "location": "somewhere",
                "reference product": "market for powertrain, for electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "powertrain")},
                    {"amount": 11.90, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2030", "battery"): {
                "name": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "location": "somewhere",
                "reference product": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "battery")},
                    {"amount": 5.26, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2030", "electricity"): {
                "name": "market group for electricity, low voltage",
                "location": "somewhere",
                "reference product": "market group for electricity, low voltage",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "electricity")},
                    {"amount": 0.23, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2030", "dismantling"): {
                "name": "market for manual dismantling of used electric passenger car",
                "location": "somewhere",
                "reference product": "market for manual dismantling of used electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "dismantling")},
                    {"amount": 0.008, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2030", "battery_recycling"): {
                "name": "market for used Li-ion battery",
                "location": "somewhere",
                "reference product": "market for used Li-ion battery",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2030", "battery_recycling")},
                    {"amount": -0.60, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
        }
    )

    bd.Database("db_2040").write(
        {
            ("db_2040", "glider"): {
                "name": "market for glider, passenger car",
                "location": "somewhere",
                "reference product": "market for glider, passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "glider")},
                    {"amount": 3.71, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2040", "powertrain"): {
                "name": "market for powertrain, for electric passenger car",
                "location": "somewhere",
                "reference product": "market for powertrain, for electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "powertrain")},
                    {"amount": 9.79, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2040", "battery"): {
                "name": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "location": "somewhere",
                "reference product": "battery production, Li-ion, LiMn2O4, rechargeable, prismatic",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "battery")},
                    {"amount": 4.25, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2040", "electricity"): {
                "name": "market group for electricity, low voltage",
                "location": "somewhere",
                "reference product": "market group for electricity, low voltage",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "electricity")},
                    {"amount": 0.067, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2040", "dismantling"): {
                "name": "market for manual dismantling of used electric passenger car",
                "location": "somewhere",
                "reference product": "market for manual dismantling of used electric passenger car",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "dismantling")},
                    {"amount": 0.0077, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
            ("db_2040", "battery_recycling"): {
                "name": "market for used Li-ion battery",
                "location": "somewhere",
                "reference product": "market for used Li-ion battery",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("db_2040", "battery_recycling")},
                    {"amount": -0.40, "type": "biosphere", "input": ("bio", "CO2")},
                ],
            },
        }
    )

    ELECTRICITY_CONSUMPTION = 0.2
    MILEAGE = 150_000
    LIFETIME = 16
    MASS_GLIDER = 840
    MASS_POWERTRAIN = 80
    MASS_BATTERY = 280

    bd.Database("foreground").write(
        {
            ("foreground", "EV_product"): {
                "name": "electric vehicle life cycle product",
                "location": "somewhere",
                "unit": "unit",
                "type": "product",
                "exchanges": [],
            },
            ("foreground", "EV_process"): {
                "name": "electric vehicle life cycle process",
                "location": "somewhere",
                "unit": "unit",
                "type": "process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "EV_product"),
                    },
                    {
                        "amount": MASS_GLIDER,
                        "type": "technosphere",
                        "input": ("db_2020", "glider"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2, -1], dtype="timedelta64[Y]"),
                            amount=np.array([0.6, 0.4]),
                        ),
                    },
                    {
                        "amount": MASS_POWERTRAIN,
                        "type": "technosphere",
                        "input": ("db_2020", "powertrain"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2, -1], dtype="timedelta64[Y]"),
                            amount=np.array([0.6, 0.4]),
                        ),
                    },
                    {
                        "amount": MASS_BATTERY,
                        "type": "technosphere",
                        "input": ("db_2020", "battery"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2, -1], dtype="timedelta64[Y]"),
                            amount=np.array([0.6, 0.4]),
                        ),
                    },
                    {
                        "amount": ELECTRICITY_CONSUMPTION * MILEAGE,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([int(LIFETIME / 2)], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": MASS_GLIDER,
                        "type": "technosphere",
                        "input": ("db_2020", "dismantling"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([LIFETIME + 1], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": -MASS_BATTERY,
                        "type": "technosphere",
                        "input": ("db_2020", "battery_recycling"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([LIFETIME + 1], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write([(("bio", "CO2"), 1)])

    for db in bd.databases:
        bd.Database(db).register()
        bd.Database(db).process()
