import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def edges_td_db():
    """
    Minimal database for the edges integration.

    The foreground process emits 10 kg CO2, but the biosphere exchange carries a temporal
    distribution that splits the emission 40 % / 60 % across two consecutive years. That split is
    what distinguishes emission-time from process-time characterisation.
    """
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "categories": ("air",),
                "unit": "kilogram",
            },
        },
    )

    bd.Database("db_2020").write(
        {
            ("db_2020", "electricity"): {
                "name": "electricity production",
                "location": "CH",
                "reference product": "electricity",
                "unit": "kilowatt hour",
                "type": "process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "electricity"),
                    },
                    {
                        "amount": 2,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "heat"): {
                "name": "heat production",
                "location": "CH",
                "reference product": "heat",
                "unit": "megajoule",
                "type": "process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "heat"),
                    },
                    {
                        "amount": 10,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                        # Day granularity (not "Y") is deliberate: numpy's timedelta64[Y] is a
                        # 365.2425-day average, so "+1 Y" from a leap-year start date (as used in
                        # the tests) lands on 2024-12-31, still within the same calendar year. 366
                        # days from 2024-01-01 lands exactly on 2025-01-01, guaranteeing the split
                        # actually crosses into the next calendar year.
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([0, 366], dtype="timedelta64[D]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                    {
                        "amount": 3,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
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
