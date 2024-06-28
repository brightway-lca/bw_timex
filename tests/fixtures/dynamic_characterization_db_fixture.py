import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def delayed_pulse_emission_db():
    bd.projects.set_current("__test_delayed_pulse_emission__")
    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CH4"): {  # only biosphere flow is CH4
                "type": "emission",
                "name": "methane",
                "temporalis code": "ch4",
            },
        }
    )

    bd.Database("test").write(  # dummy system containing 1 activity
        {
            ("test", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("test", "A"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CH4"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([10], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),  # emission of CH4 10 years after execution of process A
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CH4"), 29.8),  # GWP100 from IPCC AR6
        ]
    )


@pytest.fixture
@bw2test
def early_and_delayed_pulse_emission_db():
    bd.projects.set_current("__test_early_and_delayed_pulse_emission__")
    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CH4"): {  # only biosphere flow is CH4
                "type": "emission",
                "name": "methane",
                "temporalis code": "ch4",
            },
        }
    )

    bd.Database("test").write(  # dummy system containing 1 activity
        {
            ("test", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("test", "A"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CH4"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([10], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),  # emission of CH4 10 years after execution of process A
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CH4"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([1], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),  # emission of CH4 10 years after execution of process A
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CH4"), 29.8),  # GWP100 from IPCC AR6
        ]
    )
