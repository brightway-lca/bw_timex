import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


@pytest.fixture
@bw2test
def split_foreground_db():
    """A foreground split across two databases, only one of which holds the FU.

    ``foreground`` holds the functional unit, which consumes an intermediate
    process living in a *second* foreground database,
    ``intermediate_foreground``. That second database represents no point in
    time either, but nothing marks it as dynamic automatically: only the
    database holding the functional unit gets that treatment. Unless the user
    marks it (or lists it in ``database_dates``), it is missing from the
    mapping and its nodes cannot be placed in time.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    for year, co2 in (("2020", 10), ("2030", 5)):
        bd.Database(f"background_{year}").write(
            {
                (f"background_{year}", "electricity"): {
                    "name": "electricity",
                    "unit": "kWh",
                    "location": "GLO",
                    "reference product": "electricity",
                    "exchanges": [
                        {
                            "input": (f"background_{year}", "electricity"),
                            "amount": 1,
                            "type": "production",
                        },
                        {"input": ("bio", "co2"), "amount": co2, "type": "biosphere"},
                    ],
                }
            }
        )

    bd.Database("intermediate_foreground").write(
        {
            ("intermediate_foreground", "assembly"): {
                "name": "assembly",
                "unit": "unit",
                "location": "GLO",
                "reference product": "assembly",
                "exchanges": [
                    {
                        "input": ("intermediate_foreground", "assembly"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("background_2020", "electricity"),
                        "amount": 2,
                        "type": "technosphere",
                    },
                ],
            }
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
                        "input": ("intermediate_foreground", "assembly"),
                        "amount": 1,
                        "type": "technosphere",
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([5], dtype="timedelta64[Y]"),
                            amount=np.array([1.0]),
                        ),
                    },
                ],
            }
        }
    )

    for db in bd.databases:
        bd.Database(db).process()
