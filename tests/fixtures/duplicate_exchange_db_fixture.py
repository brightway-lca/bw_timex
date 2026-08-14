import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


def _write_databases(td_on_first_duplicate=None):
    """fu -> bg_A -> bg_B -> CO2, where bg_A consumes bg_B via TWO exchanges.

    Several exchanges between the same pair of nodes are a legitimate modelling
    choice (ecoinvent/premise do it all the time, e.g. two heat inputs from the
    same market). Both exchanges must be picked up together when the background
    is traversed.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name, co2_amount in [("background_2020", 1.0), ("background_2030", 0.5)]:
        first_duplicate = {
            "input": (db_name, "bg_B"),
            "amount": 2,
            "type": "technosphere",
        }
        if td_on_first_duplicate is not None:
            first_duplicate["temporal_distribution"] = td_on_first_duplicate

        bd.Database(db_name).write(
            {
                (db_name, "bg_A"): {
                    "name": "bg_A",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {
                            "input": (db_name, "bg_A"),
                            "amount": 1,
                            "type": "production",
                        },
                        first_duplicate,
                        {
                            "input": (db_name, "bg_B"),
                            "amount": 3,
                            "type": "technosphere",
                        },
                    ],
                },
                (db_name, "bg_B"): {
                    "name": "bg_B",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {
                            "input": (db_name, "bg_B"),
                            "amount": 1,
                            "type": "production",
                        },
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


def _write_self_loop_databases(td_on_self_loop=None):
    """fu -> bg_A -> CO2, where bg_A consumes 0.2 kg of its own product.

    The self-loop means the same node pair carries two exchanges of *different*
    types: the production exchange and the technosphere self-input. The
    technosphere matrix holds their net (1 - 0.2 = 0.8 kg of net product per
    process run).
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for db_name in ["background_2020", "background_2030"]:
        self_input = {
            "input": (db_name, "bg_A"),
            "amount": 0.2,
            "type": "technosphere",
        }
        if td_on_self_loop is not None:
            self_input["temporal_distribution"] = td_on_self_loop

        bd.Database(db_name).write(
            {
                (db_name, "bg_A"): {
                    "name": "bg_A",
                    "unit": "kg",
                    "location": "GLO",
                    "exchanges": [
                        {"input": (db_name, "bg_A"), "amount": 1, "type": "production"},
                        self_input,
                        {"input": ("bio", "co2"), "amount": 1, "type": "biosphere"},
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
def self_loop_db():
    """Production and technosphere exchange between the same node pair, neither
    carrying temporal information."""
    _write_self_loop_databases()


@pytest.fixture
@bw2test
def self_loop_with_td_db():
    """Same as `self_loop_db`, but the technosphere self-input carries a
    temporal distribution, so the two differently-typed exchanges would have to
    be merged in time - which is ambiguous."""
    _write_self_loop_databases(
        td_on_self_loop=TemporalDistribution(
            date=np.array([-2], dtype="timedelta64[Y]"),
            amount=np.array([1.0]),
        )
    )


@pytest.fixture
@bw2test
def duplicate_exchange_db():
    """Duplicate bg_A -> bg_B exchanges (amounts 2 and 3), neither with a TD."""
    _write_databases()


@pytest.fixture
@bw2test
def duplicate_exchange_td_db():
    """Duplicate bg_A -> bg_B exchanges where only the first one (amount 2)
    carries a temporal distribution, shifting its share 10 years into the
    future. The merged edge must keep both shares apart in time."""
    _write_databases(
        td_on_first_duplicate=TemporalDistribution(
            date=np.array([10], dtype="timedelta64[Y]"),
            amount=np.array([1.0]),
        )
    )
