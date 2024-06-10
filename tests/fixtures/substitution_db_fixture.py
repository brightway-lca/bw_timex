import pytest
import bw2data as bd
import numpy as np

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
            ("db_2020", "D"): {
                "name": "d",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "D"),
                    },
                    {
                        "amount": 0.5,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
            },

            ("db_2020", "E"): {
                "name": "e",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "E"),
                    },
                    {
                        "amount": 2,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
            }
        }
    )

    bd.Database("db_2030").write(
        {
            ("db_2030", "D"): {
                "name": "d",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "D"),
                    },
                    {
                        "amount": 0.5,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
            },

            ("db_2030", "E"): {
                "name": "e",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "E"),
                    },
                    {
                        "amount": 2,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
            }
        }
    )

    bd.Database("foreground").write(
        {
            ("db", "A"): {
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
                        "amount": 5,  
                        "type": "substitution",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(np.array([4], dtype="timedelta64[Y]"), np.array([1])),
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
                        "type": "technosphere",
                        "input": ("db_2020", "D"),
                    },

                    {
                        "amount": 1,
                        "type": "substitution",
                        "input": ("db_2020", "E"),
                    },

                    {
                        "amount": 1,  
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