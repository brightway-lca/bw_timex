import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


def _write_databases(td=None):
    """fu -> B -> A and fu -> C -> A, where A emits CO2 via TWO exchanges.

    A's two biosphere exchanges to the same flow (-1 and +3 kg) must be summed,
    as bw2calc does. A is consumed by two foreground processes at the same
    time, so it shows up in two timeline rows sharing one time-mapped producer:
    visiting it twice must not count its emissions twice either.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name, co2_amount in [("background_2020", 1.0), ("background_2030", 0.5)]:
        bd.Database(db_name).write(
            {
                (db_name, "bg"): {
                    "name": "bg",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg"), "amount": 1, "type": "production"},
                        {
                            "input": ("bio", "co2"),
                            "amount": co2_amount,
                            "type": "biosphere",
                        },
                    ],
                },
            }
        )

    co2_exchanges = []
    for amount in (-1.0, 3.0):
        exchange = {"input": ("bio", "co2"), "amount": amount, "type": "biosphere"}
        if td is not None:
            exchange["temporal_distribution"] = td
        co2_exchanges.append(exchange)

    def process(code, inputs, extra=()):
        return {
            "name": code,
            "unit": "unit",
            "location": "GLO",
            "reference product": code,
            "exchanges": [
                {"input": ("foreground", code), "amount": 1, "type": "production"},
                *(
                    {"input": key, "amount": amount, "type": "technosphere"}
                    for key, amount in inputs
                ),
                *extra,
            ],
        }

    bd.Database("foreground").write(
        {
            ("foreground", "fu"): process(
                "fu", [(("foreground", "B"), 1), (("foreground", "C"), 1)]
            ),
            ("foreground", "B"): process("B", [(("foreground", "A"), 1)]),
            ("foreground", "C"): process("C", [(("foreground", "A"), 2)]),
            ("foreground", "A"): process(
                "A", [(("background_2020", "bg"), 1)], extra=co2_exchanges
            ),
        }
    )

    for db in bd.databases:
        bd.Database(db).process()


@pytest.fixture
@bw2test
def duplicate_biosphere_exchange_db():
    """A emits CO2 via two exchanges (-1 and +3 kg), neither with a TD."""
    _write_databases()


@pytest.fixture
@bw2test
def duplicate_biosphere_exchange_td_db():
    """Same as `duplicate_biosphere_exchange_db`, but both CO2 exchanges spread
    over the same two years, so they land on the same (flow, date) rows."""
    _write_databases(
        td=TemporalDistribution(
            date=np.array([0, 1], dtype="timedelta64[Y]"),
            amount=np.array([0.5, 0.5]),
        )
    )
