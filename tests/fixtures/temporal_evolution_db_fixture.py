import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution
from datetime import datetime


@pytest.fixture
@bw2test
def temporal_evolution_db():
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
                "reference product": "electricity",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "electricity"),
                    },
                    {
                        "amount": 1.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
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
                "reference product": "electricity",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "electricity"),
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
            ("foreground", "consumer"): {
                "name": "consuming process",
                "location": "somewhere",
                "reference product": "consuming process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "consumer"),
                    },
                    {
                        "amount": 10,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([0, 5], dtype="timedelta64[Y]"),
                            amount=np.array([0.5, 0.5]),
                        ),
                        "temporal_evolution_factors": {
                            datetime(2020, 1, 1): 1.0,
                            datetime(2030, 1, 1): 0.5,
                        },
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


@pytest.fixture
@bw2test
def temporal_evolution_amounts_db():
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
                "reference product": "electricity",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "electricity"),
                    },
                    {
                        "amount": 1.0,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
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
                "reference product": "electricity",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2030", "electricity"),
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
            ("foreground", "consumer"): {
                "name": "consuming process",
                "location": "somewhere",
                "reference product": "consuming process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "consumer"),
                    },
                    {
                        "amount": 10,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([0, 5], dtype="timedelta64[Y]"),
                            amount=np.array([0.5, 0.5]),
                        ),
                        "temporal_evolution_amounts": {
                            datetime(2020, 1, 1): 10.0,
                            datetime(2030, 1, 1): 5.0,
                        },
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
