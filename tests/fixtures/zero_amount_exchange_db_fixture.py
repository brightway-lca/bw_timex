import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


def _write_databases(zero_exchanges):
    """fu -> bg_A -> CO2, where fu also has zero-amount exchange(s) to bg_C.

    Zero-amount technosphere exchanges are routine in real databases. bg_C emits
    CO2, but reached through a zero edge it contributes nothing, and the edge
    must be skipped rather than crash the traversal.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name, co2_amount in [("background_2020", 1.0), ("background_2030", 0.5)]:
        bd.Database(db_name).write(
            {
                (db_name, code): {
                    "name": code,
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, code), "amount": 1, "type": "production"},
                        {"input": ("bio", "co2"), "amount": amount, "type": "biosphere"},
                    ],
                }
                for code, amount in [("bg_A", co2_amount), ("bg_C", 5.0)]
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
                    *zero_exchanges,
                ],
            }
        }
    )

    for db in bd.databases:
        bd.Database(db).process()


def _to_bg_c(amount, **extra):
    return {
        "input": ("background_2020", "bg_C"),
        "amount": amount,
        "type": "technosphere",
        **extra,
    }


@pytest.fixture(params=["zero", "zero_with_td", "cancelling_duplicates"])
@bw2test
def zero_amount_exchange_db(request):
    """fu consumes bg_C with a zero amount: plainly, with a temporal
    distribution, or via two duplicate exchanges that cancel out."""
    zero_exchanges = {
        "zero": [_to_bg_c(0.0)],
        "zero_with_td": [
            _to_bg_c(
                0.0,
                temporal_distribution=TemporalDistribution(
                    date=np.array([0, 1], dtype="timedelta64[Y]"),
                    amount=np.array([0.5, 0.5]),
                ),
            )
        ],
        "cancelling_duplicates": [_to_bg_c(1.0), _to_bg_c(-1.0)],
    }[request.param]
    _write_databases(zero_exchanges)
