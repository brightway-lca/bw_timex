import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution

# Length of the linear background chain bg_0 -> bg_1 -> ... -> bg_{N-1}.
CHAIN_LEN = 6


@pytest.fixture
@bw2test
def background_td_deep_chain_db():
    """fu -> bg_0 -> bg_1 -> ... -> bg_5 -> CO2, a long linear background chain
    in two identical dated variants.

    bg_0 -> bg_1 carries a TD so the variant-aware proxy descent is exercised.
    Every link has amount 1, so with the default ``max_calc`` the descent walks
    the whole chain. With a small ``max_calc`` the descent must stop early
    (the documented ``max_calc`` bound applies to the background descent, not
    only to the referenced-variant traversal), leaving the deepest nodes out of
    the timeline.
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

    td_0_to_1 = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    variant_nodes = {}
    for db, dbname in [
        (background_2020, "background_2020"),
        (background_2030, "background_2030"),
    ]:
        nodes = []
        for i in range(CHAIN_LEN):
            n = db.new_node(f"bg_{i}", name=f"bg_{i}", unit="kg")
            n["reference product"] = f"bg_{i}"
            n.save()
            n.new_edge(input=n, amount=1, type="production").save()
            nodes.append(n)

        for i in range(CHAIN_LEN - 1):
            edge = nodes[i].new_edge(input=nodes[i + 1], amount=1, type="technosphere")
            if i == 0:
                edge["temporal_distribution"] = td_0_to_1
            edge.save()
        # The deepest node emits CO2.
        nodes[-1].new_edge(input=node_co2, amount=1, type="biosphere").save()
        variant_nodes[dbname] = {f"bg_{i}": nodes[i] for i in range(CHAIN_LEN)}

    fu.new_edge(
        input=variant_nodes["background_2020"]["bg_0"], amount=1, type="technosphere"
    ).save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variant_nodes
