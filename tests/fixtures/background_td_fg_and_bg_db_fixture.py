import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_fg_and_bg_db():
    """fu -> A(fg) -> B(bg) -> C(bg) -> CO2, two dated background variants.

    Exercises convolution of a FOREGROUND temporal distribution (on the
    background->foreground edge B->A) with a BACKGROUND temporal distribution
    (on the internal background edge C->B), while the traversal descends into
    the background.

    - A -> B (amount 3) carries a TD: 70% at +0y, 30% at +6y.
    - B -> C (amount 2) carries a TD: 60% at +0y, 40% at +10y (same both variants).
    - C -> CO2 decarbonizes: 11 kg in 2020, 7 kg in 2030.

    C is an emission-only leaf, so it is routed to the temporally-appropriate
    grid variant by date (the existing temporal-market mechanism); B is
    descended into. The combination produces four electricity cohorts at
    2024 / 2030 / 2034 / 2040.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "CO2"}}
    )
    node_co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    background_2020 = bd.Database("background_2020")
    background_2020.register()
    background_2030 = bd.Database("background_2030")
    background_2030.register()

    td_c_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"), amount=np.array([0.6, 0.4])
    )
    co2_per_variant = {"background_2020": 11, "background_2030": 7}

    variant_nodes = {}
    for db, dbname in [
        (background_2020, "background_2020"),
        (background_2030, "background_2030"),
    ]:
        b = db.new_node("B", name="B", unit="kg")
        b["reference product"] = "B"
        b.save()
        c = db.new_node("C", name="C", unit="kWh")
        c["reference product"] = "C"
        c.save()

        b.new_edge(input=b, amount=1, type="production").save()
        c.new_edge(input=c, amount=1, type="production").save()

        c_to_b = b.new_edge(input=c, amount=2, type="technosphere")
        c_to_b["temporal_distribution"] = td_c_to_b
        c_to_b.save()

        c.new_edge(input=node_co2, amount=co2_per_variant[dbname], type="biosphere").save()
        variant_nodes[dbname] = {"B": b, "C": c}

    a = foreground.new_node("A", name="A", unit="unit")
    a["reference product"] = "A"
    a.save()
    a.new_edge(input=a, amount=1, type="production").save()

    b_to_a = a.new_edge(
        input=variant_nodes["background_2020"]["B"], amount=3, type="technosphere"
    )
    b_to_a["temporal_distribution"] = TemporalDistribution(
        date=np.array([0, 6], dtype="timedelta64[Y]"), amount=np.array([0.7, 0.3])
    )
    b_to_a.save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variant_nodes
