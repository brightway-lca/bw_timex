"""The biosphere datapackage passes one `flip_array` entry per matrix entry.

`bw_processing` only validates the shape of `flip_array` when it contains at
least one `True`, so a length-1 all-`False` array slips through unnoticed on
some versions and raises `ShapeMismatch` on others. The contract is the same
either way: the flip vector has to be as long as the indices vector.
"""

from datetime import datetime

import bw2data as bd
import bw_processing as bwp
import numpy as np
import pytest
from bw2data.tests import bw2test

from bw_timex import TemporalDistribution, TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


@pytest.fixture
@bw2test
def multi_bioflow_db():
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {"type": "emission", "name": "carbon dioxide"},
            ("bio", "CH4"): {"type": "emission", "name": "methane"},
            ("bio", "N2O"): {"type": "emission", "name": "nitrous oxide"},
        }
    )

    for db_name, co2_amount in (("background_2020", 1), ("background_2030", 0.5)):
        bd.Database(db_name).write(
            {
                (db_name, "B"): {
                    "name": "node b",
                    "location": "somewhere",
                    "reference product": "B",
                    "exchanges": [
                        {"amount": 1, "type": "production", "input": (db_name, "B")},
                        {
                            "amount": co2_amount,
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ],
                },
            }
        )

    bd.Database("foreground").write(
        {
            ("foreground", "A"): {
                "name": "node a",
                "location": "somewhere",
                "reference product": "A",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": ("foreground", "A")},
                    # more than one biosphere flow on the temporalized process:
                    # this is what makes the flip vector too short
                    {"amount": 2, "type": "biosphere", "input": ("bio", "CO2")},
                    {"amount": 3, "type": "biosphere", "input": ("bio", "CH4")},
                    {"amount": 4, "type": "biosphere", "input": ("bio", "N2O")},
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2020", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-2], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(METHOD).write([(("bio", "CO2"), 1), (("bio", "CH4"), 25), (("bio", "N2O"), 300)])

    for db in bd.databases:
        bd.Database(db).process()


def test_biosphere_flip_array_matches_indices(multi_bioflow_db, monkeypatch):
    calls = []
    original = bwp.Datapackage.add_persistent_vector

    def recording_add_persistent_vector(self, **kwargs):
        if kwargs.get("matrix") == "biosphere_matrix":
            calls.append(kwargs)
        return original(self, **kwargs)

    monkeypatch.setattr(
        bwp.Datapackage, "add_persistent_vector", recording_add_persistent_vector
    )

    tlca = TimexLCA({("foreground", "A"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01")
    tlca.lci()

    assert calls, "no biosphere matrix entries were created"
    for kwargs in calls:
        flip_array = kwargs.get("flip_array")
        if flip_array is None:
            continue
        assert flip_array.shape == kwargs["indices_array"].shape, (
            "`flip_array` shape "
            f"{flip_array.shape} doesn't match `indices_array` "
            f"{kwargs['indices_array'].shape}"
        )
