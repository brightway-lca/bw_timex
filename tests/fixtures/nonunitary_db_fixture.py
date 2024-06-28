import pytest
import bw2data as bd
import numpy as np

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
            ("db_2020", "C"): {
                "name": "c",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 3, #produces a non unitary (not 1) amount
                        "type": "production",
                        "input": ("db_2020", "C"),
                    },
                    {
                        "amount": 0.5,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
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
                        "amount": 1.5,
                        "type": "technosphere",
                        "input": ("db_2020", "C"),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                    },
                ]
            },

            ("foreground", "B"): {
                "name": "b",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 7, #produces a non unitary (not 1) amount
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 0.9,  
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ]
            }
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("bio", "CO2"), 1),
        ]
    )
