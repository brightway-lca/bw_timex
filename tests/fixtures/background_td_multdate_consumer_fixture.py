import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_multidate_consumer_db():
    """fu -> bg_A -> bg_B -> bg_C -> CO2, with a TD on the FOREGROUND fu->bg_A exchange.

    The TD on fu->bg_A (foreground exchange) spreads bg_A across two absolute
    dates (2024 and 2034).  bg_A is a non-leaf background activity: it has
    bg_A->bg_B, and bg_B itself has a technosphere input bg_B->bg_C, so bg_B
    qualifies for a variant split.

    When the queue processes bg_A (reached at 2 dates) and triggers the variant
    split on bg_A->bg_B, the consumer (bg_A) has a multi-date abs_td. The split
    handles this by routing each consumer cohort independently.

    bg_C -> CO2 decarbonizes (11 kg in 2020, 7 kg in 2030), so the result depends
    on the per-cohort variant routing (it is not symmetric).
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

    co2_per_variant = {"background_2020": 11, "background_2030": 7}

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

        # bg_A -> bg_B: plain amount.  bg_B has a further technosphere input
        # (bg_B -> bg_C), so bg_B is a non-leaf and qualifies for variant split.
        bg_a.new_edge(input=bg_b, amount=1, type="technosphere").save()
        bg_b.new_edge(input=bg_c, amount=1, type="technosphere").save()
        bg_c.new_edge(
            input=node_co2, amount=co2_per_variant[dbname], type="biosphere"
        ).save()

        variant_nodes[dbname] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c}

    # Foreground exchange WITH a TD: spreads bg_A consumption over +0y and +10y.
    # This makes bg_A's abs_td have 2 dates when it enters the queue, so the
    # bg_A->bg_B variant split has a multi-date consumer.
    td_fu_to_bg_a = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    fg_exc = fu.new_edge(
        input=variant_nodes["background_2020"]["bg_A"], amount=1, type="technosphere"
    )
    fg_exc["temporal_distribution"] = td_fu_to_bg_a
    fg_exc.save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variant_nodes
