import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def nonunitary_db():
    bd.projects.set_current("__test_nonunitary__")
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
            ("db_2020", "D"): {
                "name": "d",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 3,  # produces a non unitary (not 1) amount
                        "type": "production",
                        "input": ("db_2020", "D"),
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

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "a",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 0.8,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1.5,
                        "type": "technosphere",
                        "input": ("db_2020", "D"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-24, -12, 0], dtype="timedelta64[M]"),
                            np.array([0.5, 0.3, 0.2]),
                        ),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-24, -12, 0], dtype="timedelta64[M]"),
                            np.array([0.5, 0.3, 0.2]),
                        ),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "b",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 7,  # produces a non unitary (not 1) amount
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                    {
                        "amount": -2,
                        "type": "technosphere",
                        "input": ("foreground", "C"),
                    },
                ],
            },
            ("foreground", "C"): {
                "name": "c",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": -1,  # produces a non unitary (not 1) amount
                        "type": "production",
                        "input": ("foreground", "C"),
                    },
                    {
                        "amount": -1,
                        "type": "technosphere",
                        "input": ("db_2020", "D"),
                    },
                    {
                        "amount": 6,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
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
