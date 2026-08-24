"""A deeper version of the EV example, for performance work.

The example foregrounds shipped with bw_timex are three or four processes, so
their timelines stay in the tens of rows - too small to show how `lci()`
behaves on a foreground that actually branches. Here `ev_production` is fed by
a balanced tree of sub-assemblies (`--depth` levels, `--branching` children
each) with a temporal distribution on every edge. The distributions convolve
down the chain, so the number of (process, time) pairs grows roughly as
`td_points ** depth`, and the timeline explodes:

    --depth 3 --branching 3 --materials 12   ->    241 rows yearly,   3137 monthly
    --depth 4 --branching 3 --materials 81   ->   1024 rows yearly,  15611 monthly

It also reports the two competing background-solve strategies, which is what
the model was built to explore (see `TimexLCA._plan_background_solves`):

- *per background process* - one unit LCI per distinct background activity,
  cached across runs. Scales with how many background processes the foreground
  reaches.
- *per time step* - the background demands of every market row landing at the
  same time, summed and solved together. Scales with `(time, block)` pairs, and
  caches nothing.

A wide foreground over few time steps favours the second; the plain EV models
favour the first.

Usage::

    python build_complex_ev_foreground.py --depth 4 --materials 81
    python build_complex_ev_foreground.py --temporal-grouping year
"""
import argparse
from datetime import datetime

import numpy as np

PROJECT = "ev_complex_foreground"

# Background materials the leaf assemblies draw on, with their CO2 intensity
# in 2020 / 2030 / 2040 (falling, as in the original example).
MATERIALS = {
    "steel": (2.1, 1.4, 0.9),
    "aluminium": (8.5, 5.2, 3.1),
    "copper": (3.4, 2.2, 1.5),
    "plastic": (2.8, 2.0, 1.4),
    "glass": (1.2, 0.8, 0.5),
    "rubber": (1.9, 1.3, 0.9),
    "electronics": (12.0, 7.5, 4.4),
    "magnet": (15.0, 9.0, 5.0),
    "cathode_material": (9.5, 6.0, 3.5),
    "anode_material": (4.5, 3.0, 1.8),
    "electrolyte": (5.5, 3.5, 2.0),
    "separator": (6.0, 4.0, 2.4),
}

# The original EV example's own background processes.
EV_PROCESSES = {
    "glider": (10, 5, 2.5),
    "powertrain": (20, 10, 7.5),
    "battery": (10, 5, 4),
    "electricity": (0.5, 0.25, 0.075),
    "glider_eol": (0.01, 0.0075, 0.005),
    "powertrain_eol": (0.01, 0.0075, 0.005),
    "battery_eol": (1, 0.5, 0.25),
}

ELECTRICITY_CONSUMPTION = 0.2  # kWh/km
MILEAGE = 150_000  # km
LIFETIME = 15  # years
MASS_GLIDER = 840  # kg
MASS_POWERTRAIN = 80  # kg
MASS_BATTERY = 280  # kg

YEARS = (2020, 2030, 2040)


def make_materials(n):
    """`n` background materials with falling 2020/2030/2040 CO2 intensities."""
    if n <= len(MATERIALS):
        return dict(list(MATERIALS.items())[:n])
    materials = dict(MATERIALS)
    rng = np.random.default_rng(0)
    for index in range(len(MATERIALS), n):
        base = float(rng.uniform(1.0, 15.0))
        materials[f"mat_{index:03d}"] = (base, base * 0.65, base * 0.4)
    return materials


def build(depth, branching, td_points, n_materials):
    import bw2data as bd
    from bw_timex import TemporalDistribution, easy_timedelta_distribution
    from bw_timex import set_database_metadata

    bd.projects.set_current(PROJECT)
    for db in list(bd.databases):
        del bd.databases[db]

    biosphere = bd.Database("biosphere")
    biosphere.register()
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    node_co2 = biosphere.get("CO2")

    # -- background vintages ------------------------------------------------
    backgrounds = []
    for year in YEARS:
        db = bd.Database(f"background_{year}")
        db.register()
        db.write({})
        backgrounds.append(db)

    materials = make_materials(n_materials)
    for name, intensities in {**EV_PROCESSES, **materials}.items():
        for db, intensity in zip(backgrounds, intensities):
            db.new_node(name, name=name, location="somewhere").save()
            node = db.get(name)
            node["reference product"] = name
            node.save()
            production = -1 if "eol" in name else 1
            node.new_edge(input=node, amount=production, type="production").save()
            node.new_edge(input=node_co2, amount=intensity, type="biosphere").save()

    background_2020 = backgrounds[0]

    # -- foreground ---------------------------------------------------------
    foreground = bd.Database("foreground")
    foreground.register()
    foreground.write({})

    def new_process(code, name, unit="kg"):
        foreground.new_node(code, name=name, unit=unit).save()
        node = foreground.get(code)
        node["reference product"] = code
        node.save()
        node.new_edge(input=node, amount=1, type="production").save()
        return node

    ev_production = new_process("ev_production", "production of an electric vehicle", "unit")
    driving = new_process("driving", "driving an electric vehicle", "transport")
    used_ev = new_process("used_ev", "used electric vehicle", "unit")

    # A balanced tree of sub-assemblies under `ev_production`. Leaves consume
    # background materials; inner nodes consume the level below.
    material_names = list(materials)
    tree_edges = []          # (consumer, producer, amount) inside the foreground
    material_edges = []      # (consumer, background node, amount)
    levels = [[ev_production]]

    counter = 0
    for level in range(depth):
        children = []
        for parent in levels[-1]:
            for _ in range(branching):
                child = new_process(f"asm_{level}_{counter}", f"sub-assembly {level}.{counter}")
                counter += 1
                # A little direct emission, so foreground nodes are not inert.
                child.new_edge(input=node_co2, amount=0.05, type="biosphere").save()
                tree_edges.append((parent, child, 1.0 / branching))
                children.append(child)
        levels.append(children)

    for index, leaf in enumerate(levels[-1]):
        material = background_2020.get(material_names[index % len(material_names)])
        material_edges.append((leaf, material, 1.0))

    # -- the original EV skeleton on top ------------------------------------
    glider = background_2020.get("glider")
    powertrain = background_2020.get("powertrain")
    battery = background_2020.get("battery")
    electricity = background_2020.get("electricity")

    ev_edges = [
        (ev_production, glider, MASS_GLIDER),
        (ev_production, powertrain, MASS_POWERTRAIN),
        (ev_production, battery, MASS_BATTERY),
    ]
    eol_edges = [
        (used_ev, background_2020.get("glider_eol"), -MASS_GLIDER),
        (used_ev, background_2020.get("powertrain_eol"), -MASS_POWERTRAIN),
        (used_ev, background_2020.get("battery_eol"), -MASS_BATTERY),
    ]
    # Waste treatment: the used vehicle is consumed, so its production is -1.
    for exchange in used_ev.production():
        exchange["amount"] = -1
        exchange.save()

    driving_to_used_ev = driving.new_edge(input=used_ev, amount=-1, type="technosphere")
    driving_to_used_ev.save()
    ev_to_driving = driving.new_edge(input=ev_production, amount=1, type="technosphere")
    ev_to_driving.save()
    electricity_to_driving = driving.new_edge(
        input=electricity, amount=ELECTRICITY_CONSUMPTION * MILEAGE, type="technosphere"
    )
    electricity_to_driving.save()

    # -- temporal distributions --------------------------------------------
    def spread(points, span_months):
        """`points` offsets spread over `span_months`, weights summing to 1."""
        offsets = np.linspace(-span_months, 0, points).round().astype(int)
        offsets = np.unique(offsets)
        weights = np.full(len(offsets), 1.0 / len(offsets))
        return TemporalDistribution(
            date=np.array(offsets, dtype="timedelta64[M]"), amount=weights
        )

    # Each level of the tree gets its own span, so the convolved offsets do
    # not collapse onto each other when the timeline is grouped.
    for parent, child, amount in tree_edges:
        exchange = parent.new_edge(input=child, amount=amount, type="technosphere")
        level = int(child["code"].split("_")[1])
        exchange["temporal_distribution"] = spread(td_points, 3 * (level + 1) + 1)
        exchange.save()

    for leaf, material, amount in material_edges:
        exchange = leaf.new_edge(input=material, amount=amount, type="technosphere")
        exchange["temporal_distribution"] = spread(td_points, 5)
        exchange.save()

    for consumer, producer, amount in ev_edges:
        exchange = consumer.new_edge(input=producer, amount=amount, type="technosphere")
        exchange["temporal_distribution"] = spread(3, 24)
        exchange.save()

    for consumer, producer, amount in eol_edges:
        exchange = consumer.new_edge(input=producer, amount=amount, type="technosphere")
        exchange["temporal_distribution"] = TemporalDistribution(
            date=np.array([3], dtype="timedelta64[M]"), amount=np.array([1])
        )
        exchange.save()

    ev_to_driving["temporal_distribution"] = TemporalDistribution(
        date=np.array([-3, -2], dtype="timedelta64[M]"), amount=np.array([0.2, 0.8])
    )
    ev_to_driving.save()
    driving_to_used_ev["temporal_distribution"] = TemporalDistribution(
        date=np.array([LIFETIME + 1], dtype="timedelta64[Y]"), amount=np.array([1])
    )
    driving_to_used_ev.save()
    electricity_to_driving["temporal_distribution"] = easy_timedelta_distribution(
        start=0, end=LIFETIME, resolution="Y", steps=LIFETIME + 1, kind="uniform"
    )
    electricity_to_driving.save()

    for db in bd.databases:
        bd.Database(db).process()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for year in YEARS:
        set_database_metadata(
            f"background_{year}", representative_time=datetime(year, 1, 1)
        )

    n_foreground = len(foreground)
    print(
        f"built {PROJECT}: {n_foreground} foreground processes "
        f"({len(tree_edges)} tree edges, {len(material_edges)} material edges), "
        f"{len(materials) + len(EV_PROCESSES)} background processes x {len(YEARS)} vintages"
    )
    return driving


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--branching", type=int, default=3)
    parser.add_argument("--td-points", type=int, default=3)
    parser.add_argument("--materials", type=int, default=12)
    parser.add_argument("--temporal-grouping", default="month")
    args = parser.parse_args()

    build(args.depth, args.branching, args.td_points, args.materials)

    import bw2data as bd
    from bw_timex import TimexLCA

    driving = bd.get_node(database="foreground", code="driving")
    tlca = TimexLCA({driving: 1}, ("GWP", "example"))
    tlca.build_timeline(
        temporal_grouping=args.temporal_grouping, graph_traversal="bfs"
    )
    print(f"timeline rows: {len(tlca.timeline)}")

    tlca.lci(
        expand_technosphere=False,
        build_dynamic_biosphere=True,
        keep_activity_dimension=False,
    )
    builder = tlca.dynamic_biosphere_builder
    solver = tlca._background_solver

    per_process = len(
        {
            solver.cache_key(act)
            for demand in builder.collect_background_demands().values()
            for act in demand
        }
    )
    grouped = len(
        {
            (time, solver.block_index_for(act))
            for time, demand in builder.collect_background_demands_by_time().items()
            for act in demand
        }
    )
    chosen = "per time step" if builder.group_background_by_time else "per process"

    print(f"background solves, per process:   {per_process}")
    print(f"background solves, per time step: {grouped}")
    print(f"strategy chosen:                  {chosen}")
    print(f"dynamic inventory sum:            {tlca.dynamic_inventory.sum():.6f}")


if __name__ == "__main__":
    main()
