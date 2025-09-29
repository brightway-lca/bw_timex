import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def dynamic_biosphere_matrix_db():
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
                "name": "node c",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "C"),
                    },
                    {
                        "amount": 1.5,
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
                "name": "node a",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([4], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": (
                            "foreground",
                            "B_1",
                        ),  # occuring at exactly the same time as B
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([4], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "node b",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("db_2020", "C"),
                    },
                ],
            },
            ("foreground", "B_1"): {  # identical to B
                "name": "node b_1",
                "location": "somewhere",
                "reference product": "B_1",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B_1"),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("db_2020", "C"),
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
