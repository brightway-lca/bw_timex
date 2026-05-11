from datetime import datetime
from typing import Tuple

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


def _make_chimaera_project(name: str) -> Tuple[bd.Node, dict]:
    """Tiny chimaera project: 1 background, 1 foreground process+product.

    Returns ``(demanded_node, database_dates)``.
    """
    bd.projects.set_current(name)
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])
    bd.Database("foreground").write(
        {
            ("foreground", "fg_process"): {
                "name": "fg process",
                "reference product": "fg product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "fg_process"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 2.0,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    return (
        bd.get_node(database="foreground", code="fg_process"),
        {
            "background": datetime(2025, 1, 1),
            "foreground": "dynamic",
        },
    )


def test_demand_td_value_accepted_by_validation():
    node, dbdates = _make_chimaera_project("__test_td_demand_validation_ok__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    assert tlca is not None


def test_demand_td_negative_amount_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_neg__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, -10.0]),
    )
    with pytest.raises(Exception, match="non-negative"):
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )


def test_demand_td_empty_amount_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_empty__")
    # bw_temporalis itself rejects empty arrays before we can pass the TD to
    # TimexLCA, so wrap construction.
    with pytest.raises(Exception, match="at least one|[Ee]mpty"):
        td = TemporalDistribution(
            date=np.array([], dtype="datetime64[s]"),
            amount=np.array([]),
        )
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )


def test_demand_td_wrong_dtype_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_dtype__")
    # bw_temporalis itself rejects non-datetime64/timedelta64 date dtypes
    # before we can pass the TD to TimexLCA, so wrap construction.
    with pytest.raises(Exception, match="datetime64|timedelta64|dtype"):
        td = TemporalDistribution(
            date=np.array([2025.0, 2030.0], dtype="float64"),
            amount=np.array([60.0, 40.0]),
        )
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )


def test_demand_td_static_base_score_matches_scalar_sum():
    """Static base score for demand={p: TD} == bw2calc.LCA({p: sum(TD.amount)})."""
    node, dbdates = _make_chimaera_project("__test_td_demand_base_parity__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )

    slca = bc.LCA({node.key: 100.0}, method=("GWP", "example"))
    slca.lci()
    slca.lcia()

    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    assert tlca.base_lca.score == pytest.approx(slca.score)
    assert tlca._scalar_demand == {node.key: 100.0}
