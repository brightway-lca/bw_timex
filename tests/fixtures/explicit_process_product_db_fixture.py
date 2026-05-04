from datetime import datetime

import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution


@pytest.fixture
def explicit_process_product_db():
    bd.projects.set_current("__test_explicit_process_product__")
    bd.databases.clear()
    bd.methods.clear()

    bio = bd.Database("bio")
    bio.write(
        {
            ("bio", "co2"): {
                "name": "Carbon dioxide",
                "unit": "kg",
                "type": "emission",
            }
        }
    )

    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    bd.Database("bg_2030").write(
        {
            ("bg_2030", "electricity"): {
                "name": "electricity",
                "unit": "kWh",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("bg_2030", "electricity"),
                        "amount": 1,
                        "type": "production",
                    },
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )

    td = TemporalDistribution(
        date=np.array([0, 1], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "fleet_driving_process"): {
                "name": "fleet driving process",
                "unit": "unit",
                "location": "GLO",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("foreground", "fleet_driving_product"),
                        "amount": 1,
                        "type": "production",
                        "temporal_distribution": td,
                    },
                    {
                        "input": ("bg_2030", "electricity"),
                        "amount": 10,
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "fleet_driving_product"): {
                "name": "fleet driving product",
                "unit": "unit",
                "location": "GLO",
                "type": "product",
                "exchanges": [],
            },
        }
    )

    return {
        "database_dates": {
            "bg_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",
        }
    }
