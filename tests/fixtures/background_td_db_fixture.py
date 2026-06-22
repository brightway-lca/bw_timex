import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_db():
    """foreground.fu -> background.bg_A -> background.bg_B -> CO2.

    Two dated background variants (2020, 2030). The bg_A -> bg_B exchange
    carries a temporal_distribution so that, when traversed, bg_B is spread
    over time and sourced from the temporally-appropriate variant.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {
            ("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"},
        }
    )
    node_co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()

    background_2020 = bd.Database("background_2020")
    background_2020.register()
    background_2030 = bd.Database("background_2030")
    background_2030.register()

    # --- foreground ---
    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    # --- build both background variants identically (structure) ---
    # bg_A -> bg_B TD: spread 60% at +0y, 40% at +10y from bg_A's time.
    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    variant_nodes = {}
    for db, dbname in [(background_2020, "background_2020"), (background_2030, "background_2030")]:
        bg_a = db.new_node("bg_A", name="bg_A", unit="kWh")
        bg_a["reference product"] = "bg_A"
        bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="kg")
        bg_b["reference product"] = "bg_B"
        bg_b.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_b.new_edge(input=bg_b, amount=1, type="production").save()

        a_to_b = bg_a.new_edge(input=bg_b, amount=5, type="technosphere")
        a_to_b["temporal_distribution"] = td_a_to_b
        a_to_b.save()

        bg_b.new_edge(input=node_co2, amount=1, type="biosphere").save()
        variant_nodes[dbname] = (bg_a, bg_b)

    # --- foreground -> background_2020.bg_A ---
    bg_a_2020 = variant_nodes["background_2020"][0]
    fu.new_edge(input=bg_a_2020, amount=2, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for dbn in bd.databases:
        bd.Database(dbn).process()

    return variant_nodes
