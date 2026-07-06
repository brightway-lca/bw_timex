import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_prod_td_db():
    """fu -> bg_A -> bg_B -> bg_C -> CO2, two dated variants.

    bg_A->bg_B carries a technosphere TD (triggers the variant-split descent).
    bg_B carries a PRODUCTION-edge TD spread over several years, so the descent
    must register bg_B at the same production-TD-weighted cohorts it consumes
    bg_C at. All coefficients are 1, so the total impact must equal 1.0.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    bg20 = bd.Database("background_2020")
    bg20.register()
    bg30 = bd.Database("background_2030")
    bg30.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod_td_b = TemporalDistribution(
        date=np.array([0, 3, 6], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.3, 0.2]),
    )

    variants = {}
    for db in (bg20, bg30):
        bg_a = db.new_node("bg_A", name="bg_A", unit="k"); bg_a["reference product"] = "bg_A"; bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="k"); bg_b["reference product"] = "bg_B"; bg_b.save()
        bg_c = db.new_node("bg_C", name="bg_C", unit="k"); bg_c["reference product"] = "bg_C"; bg_c.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        pb = bg_b.new_edge(input=bg_b, amount=1, type="production")
        pb["temporal_distribution"] = prod_td_b
        pb.save()
        bg_c.new_edge(input=bg_c, amount=1, type="production").save()

        e = bg_a.new_edge(input=bg_b, amount=1, type="technosphere")
        e["temporal_distribution"] = td_a_to_b
        e.save()
        bg_b.new_edge(input=bg_c, amount=1, type="technosphere").save()
        bg_c.new_edge(input=co2, amount=1, type="biosphere").save()
        variants[db.name] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c}

    fu.new_edge(input=variants["background_2020"]["bg_A"], amount=1, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variants


@pytest.fixture
@bw2test
def background_prod_td_deep_db():
    """fu -> bg_A -> bg_B -> bg_C -> bg_D -> CO2, two variants.

    bg_A->bg_B carries a technosphere TD (starts the descent, one level above
    the node under test). bg_C -- reached one level deeper, inside the
    locked-variant descent -- carries the production-edge TD and has its own
    downstream technosphere child (bg_D), so bg_C is re-queued and dequeued as
    ``cur_id`` inside ``_descend_variant_subtree`` (unlike a leaf production-TD
    node, which never re-enters the loop and so can't expose the bug). This
    exercises the descent emit site rather than the first-level split. Total
    impact must equal 1.0.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    bg20 = bd.Database("background_2020")
    bg20.register()
    bg30 = bd.Database("background_2030")
    bg30.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod_td_c = TemporalDistribution(
        date=np.array([0, 3, 6], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.3, 0.2]),
    )

    variants = {}
    for db in (bg20, bg30):
        bg_a = db.new_node("bg_A", name="bg_A", unit="k"); bg_a["reference product"] = "bg_A"; bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="k"); bg_b["reference product"] = "bg_B"; bg_b.save()
        bg_c = db.new_node("bg_C", name="bg_C", unit="k"); bg_c["reference product"] = "bg_C"; bg_c.save()
        bg_d = db.new_node("bg_D", name="bg_D", unit="k"); bg_d["reference product"] = "bg_D"; bg_d.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_b.new_edge(input=bg_b, amount=1, type="production").save()
        pc = bg_c.new_edge(input=bg_c, amount=1, type="production")
        pc["temporal_distribution"] = prod_td_c
        pc.save()
        bg_d.new_edge(input=bg_d, amount=1, type="production").save()

        e = bg_a.new_edge(input=bg_b, amount=1, type="technosphere")
        e["temporal_distribution"] = td_a_to_b
        e.save()
        bg_b.new_edge(input=bg_c, amount=1, type="technosphere").save()
        bg_c.new_edge(input=bg_d, amount=1, type="technosphere").save()
        bg_d.new_edge(input=co2, amount=1, type="biosphere").save()
        variants[db.name] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c, "bg_D": bg_d}

    fu.new_edge(input=variants["background_2020"]["bg_A"], amount=1, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variants
