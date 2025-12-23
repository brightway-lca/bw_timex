import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def temporal_grouping_db_monthly():
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "biosphere",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2024").write(
        {
            ("db_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2024", "C"),
                    },
                    {
                        "amount": 10,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("db_2022").write(
        {
            ("db_2022", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2022", "C"),
                    },
                    {
                        "amount": 15,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1,
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-24, -12, 0], dtype="timedelta64[M]"),
                            np.array([1 / 3, 1 / 3, 1 / 3]),
                        ),
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 1,
                        "input": ("db_2024", "C"),
                        "type": "technosphere",
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
def temporal_grouping_db_daily():
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "biosphere",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2024").write(
        {
            ("db_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2024", "C"),
                    },
                    {
                        "amount": 10,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("db_2022").write(
        {
            ("db_2022", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2022", "C"),
                    },
                    {
                        "amount": 15,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1,
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-15, -10, 0], dtype="timedelta64[D]"),
                            np.array([1 / 3, 1 / 3, 1 / 3]),
                        ),
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 1,
                        "input": ("db_2024", "C"),
                        "type": "technosphere",
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
def temporal_grouping_db_hourly():
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "biosphere",
                "name": "carbon dioxide",
            },
        },
    )

    bd.Database("db_2024").write(
        {
            ("db_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2024", "C"),
                    },
                    {
                        "amount": 10,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("db_2022").write(
        {
            ("db_2022", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2022", "C"),
                    },
                    {
                        "amount": 15,
                        "input": ("bio", "CO2"),
                        "type": "biosphere",
                    },
                ],
            },
        },
    )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 1,
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            np.array([-15, -10, 0], dtype="timedelta64[h]"),
                            np.array([1 / 3, 1 / 3, 1 / 3]),
                        ),
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "B",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 1,
                        "input": ("db_2024", "C"),
                        "type": "technosphere",
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
