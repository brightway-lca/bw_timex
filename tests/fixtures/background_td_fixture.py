import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_db():
    bd.config.p["biosphere_database"] = "bio"

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
            ("db_2020", "electricity"): {
                "name": "electricity production",
                "location": "somewhere",
                "reference product": "electricity production",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "electricity"),
                    },
                    {
                        "amount": 10.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
            ("db_2020", "steel"): {
                "name": "steel production",
                "location": "somewhere",
                "reference product": "steel production",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "steel"),
                    },
                    {
                        "amount": 5.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                    {
                        "amount": 100,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2], dtype="timedelta64[Y]"),
                            amount=np.array([1.0]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Database("db_2030").write(
        {
            ("db_2030", "electricity"): {
                "name": "electricity production",
                "location": "somewhere",
                "reference product": "electricity production",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "electricity"),
                    },
                    {
                        "amount": 5.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
            ("db_2030", "steel"): {
                "name": "steel production",
                "location": "somewhere",
                "reference product": "steel production",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "steel"),
                    },
                    {
                        "amount": 3.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                    {
                        "amount": 100,
                        "type": "technosphere",
                        "input": ("db_2030", "electricity"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2], dtype="timedelta64[Y]"),
                            amount=np.array([1.0]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "product"): {
                "name": "product assembly",
                "location": "somewhere",
                "reference product": "product assembly",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "product"),
                    },
                    {
                        "amount": 10,
                        "type": "technosphere",
                        "input": ("db_2020", "steel"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-1], dtype="timedelta64[Y]"),
                            amount=np.array([1.0]),
                        ),
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

    for db in bd.databases:
        bd.Database(db).register()
        bd.Database(db).process()
