import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


def _write_databases(bg_a_to_b_td):
    """fu -> bg_A -> bg_B -> bg_C -> CO2, background TD on bg_A -> bg_B."""
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name, co2_amount in [("background_2020", 1.0), ("background_2030", 0.5)]:
        bd.Database(db_name).write(
            {
                (db_name, "bg_A"): {
                    "name": "bg_A",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg_A"), "amount": 1, "type": "production"},
                        {
                            "input": (db_name, "bg_B"),
                            "amount": 1,
                            "type": "technosphere",
                            "temporal_distribution": bg_a_to_b_td,
                        },
                    ],
                },
                (db_name, "bg_B"): {
                    "name": "bg_B",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg_B"), "amount": 1, "type": "production"},
                        {
                            "input": (db_name, "bg_C"),
                            "amount": 1,
                            "type": "technosphere",
                        },
                    ],
                },
                (db_name, "bg_C"): {
                    "name": "bg_C",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg_C"), "amount": 1, "type": "production"},
                        {
                            "input": ("bio", "co2"),
                            "amount": co2_amount,
                            "type": "biosphere",
                        },
                    ],
                },
            }
        )

    bd.Database("foreground").write(
        {
            ("foreground", "fu"): {
                "name": "fu",
                "unit": "unit",
                "location": "GLO",
                "reference product": "fu",
                "exchanges": [
                    {"input": ("foreground", "fu"), "amount": 1, "type": "production"},
                    {
                        "input": ("background_2020", "bg_A"),
                        "amount": 1,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )

    for db in bd.databases:
        bd.Database(db).process()


@pytest.fixture
@bw2test
def zero_weight_background_td_db():
    """Background TD with a zero-weight date: bg_B is reached at t0 (weight 1)
    and at t0+10a (weight 0). The zero-weight cohort of bg_B carries no supply,
    but its own inputs (bg_B -> bg_C) do have non-zero exchange amounts, so the
    edges out of that cohort survive while the edge into it does not."""
    _write_databases(
        TemporalDistribution(
            date=np.array([0, 10], dtype="timedelta64[Y]"),
            amount=np.array([1.0, 0.0]),
        )
    )


@pytest.fixture
@bw2test
def zero_weight_first_background_td_db():
    """Like `zero_weight_background_td_db`, but the zero weight is on the date
    that routes to a different background variant than the non-zero one. The
    variant split then hands the descent a cohort whose amounts are *all* zero.
    """
    _write_databases(
        TemporalDistribution(
            date=np.array([0, 10], dtype="timedelta64[Y]"),
            amount=np.array([0.0, 1.0]),
        )
    )


@pytest.fixture
@bw2test
def single_date_background_td_db():
    """Reference for `zero_weight_background_td_db` with the zero-weight date
    left out entirely. Must give the same score."""
    _write_databases(
        TemporalDistribution(
            date=np.array([0], dtype="timedelta64[Y]"),
            amount=np.array([1.0]),
        )
    )
