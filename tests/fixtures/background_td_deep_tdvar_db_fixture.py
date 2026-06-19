import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_deep_tdvar_db():
    """fu -> bg_A -> bg_B -> bg_C -> CO2, two dated variants.

    bg_A -> bg_B: TD 60% +0y / 40% +10y (identical both variants).
    bg_B -> bg_C: amount 1 in BOTH variants, but the TD DIFFERS:
        background_2020: 100% +0y (no spread)
        background_2030:  50% +0y / 50% +20y
    So the +20y bg_C cohorts appear ONLY if the 2030 variant's TD is read
    (respective-variant TD reads). Amounts are equal across variants, so total
    CO2 stays 10 kg; only the temporal distribution of bg_C differs.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    node_co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    background_2020 = bd.Database("background_2020")
    background_2020.register()
    background_2030 = bd.Database("background_2030")
    background_2030.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"), amount=np.array([0.6, 0.4])
    )
    td_b_to_c = {
        "background_2020": TemporalDistribution(
            date=np.array([0], dtype="timedelta64[Y]"), amount=np.array([1.0])
        ),
        "background_2030": TemporalDistribution(
            date=np.array([0, 20], dtype="timedelta64[Y]"), amount=np.array([0.5, 0.5])
        ),
    }

    variant_nodes = {}
    for db, dbname in [
        (background_2020, "background_2020"),
        (background_2030, "background_2030"),
    ]:
        bg_a = db.new_node("bg_A", name="bg_A", unit="kWh")
        bg_a["reference product"] = "bg_A"
        bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="kg")
        bg_b["reference product"] = "bg_B"
        bg_b.save()
        bg_c = db.new_node("bg_C", name="bg_C", unit="kg")
        bg_c["reference product"] = "bg_C"
        bg_c.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_b.new_edge(input=bg_b, amount=1, type="production").save()
        bg_c.new_edge(input=bg_c, amount=1, type="production").save()

        a_to_b = bg_a.new_edge(input=bg_b, amount=5, type="technosphere")
        a_to_b["temporal_distribution"] = td_a_to_b
        a_to_b.save()

        b_to_c = bg_b.new_edge(input=bg_c, amount=1, type="technosphere")
        b_to_c["temporal_distribution"] = td_b_to_c[dbname]
        b_to_c.save()

        bg_c.new_edge(input=node_co2, amount=1, type="biosphere").save()
        variant_nodes[dbname] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c}

    fu.new_edge(
        input=variant_nodes["background_2020"]["bg_A"], amount=2, type="technosphere"
    ).save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variant_nodes
