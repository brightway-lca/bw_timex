import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def substitution_db():
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
            ("db_2020", "Sub"): {
                "name": "sub",
                "location": "somewhere",
                "reference product": "sub",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "Sub"),
                    },
                    {
                        "amount": 0.5,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("db_2030").write(
        {
            ("db_2030", "Sub"): {
                "name": "sub",
                "location": "somewhere",
                "reference product": "sub",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "Sub"),
                    },
                    {
                        "amount": 0.7,  # changed compared to db_2020
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "a",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                    {
                        "amount": 0.75,
                        "type": "substitution",
                        "input": ("db_2020", "Sub"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-4], dtype="timedelta64[Y]"), np.array([1])
                        ),  # occurs in 2020
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([4], dtype="timedelta64[Y]"), np.array([1])
                        ),  # occurs in 2028
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "b",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 1,
                        "type": "substitution",
                        "input": ("db_2020", "Sub"),
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
