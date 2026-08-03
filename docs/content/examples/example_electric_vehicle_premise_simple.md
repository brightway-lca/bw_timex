---
icon: lucide/car-front
tags:
  - example
  - premise
  - temporal distribution
---


<div hidden data-source-edit-path="docs/content/examples/example_electric_vehicle_premise_simple.ipynb" data-source-view-path="docs/content/examples/example_electric_vehicle_premise_simple.ipynb"></div>
# Life cycle of an electric vehicle

This notebook is a compact walk-through of a time-explicit LCA with `bw_timex`, using a cradle-to-grave case study of an electric vehicle (ev). The case study is simplified: it is not meant to reflect the complexity of electric mobility, but to show what you need to do to make an LCA time-explicit.

> **Note:** This notebook uses ecoinvent and premise data. It expects a brightway project with ecoinvent v3.12 (cutoff) and prospective databases for 2020, 2030 and 2040, created from it with [`premise`](https://github.com/polca/premise) (all sectors updated, REMIND-EU SSP2-NDC). If you don't have access to that, please check out the ["standalone" version](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/example_electric_vehicle_standalone.ipynb) of this notebook, which creates all its data from scratch. A more detailed version of this example, which explains all the modelling steps and the additional options of `bw_timex`, is [here](example_electric_vehicle_premise.md).


```python
import bw2data as bd

bd.projects.set_current("ei312_REMIND_EU")
```

## The product system

Our ev is built from a glider, a powertrain and a battery, it is driven for 150,000 km over 15 years, and its parts are treated at the end of life. Purple boxes are foreground, cyan boxes come from the background databases (ecoinvent/premise).

```mermaid
flowchart LR
    glider_production(glider production):::ei-->ev_production
    powertrain_production(powertrain production):::ei-->ev_production
    battery_production(battery production):::ei-->ev_production
    ev_production(ev production):::fg-->driving
    electricity_generation(electricity generation):::ei-->driving
    driving(driving):::fg-->used_ev
    used_ev(used ev):::fg-->glider_eol(glider eol):::ei
    used_ev-->powertrain_eol(powertrain eol):::ei
    used_ev-->battery_eol(battery eol):::ei

    classDef ei color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#9c5ffd, stroke:none;
```

Building this system is standard brightway modelling, without anything `bw_timex`-specific, so the cell below just does it in one go (unfold it if you are curious).


??? note "Show the code that builds the ev system"

    ```python
    # Standard brightway modelling of the ev system - nothing time-explicit yet.

    ELECTRICITY_CONSUMPTION = 0.2  # kWh/km
    MILEAGE = 150_000  # km
    LIFETIME = 15  # years
    MASS_GLIDER = 840  # kg
    MASS_POWERTRAIN = 80  # kg
    MASS_BATTERY = 280  # kg

    db_2020 = bd.Database("ei312_REMIND-EU_SSP2_NDC_2020")
    db_2030 = bd.Database("ei312_REMIND-EU_SSP2_NDC_2030")
    db_2040 = bd.Database("ei312_REMIND-EU_SSP2_NDC_2040")

    # The ecoinvent processes for the ev parts already contain their end-of-life treatment.
    # We want to model the end of life separately, so we create copies without it.
    for db in [db_2020, db_2030, db_2040]:
        for name, code_, eol_name in [
            (
                "glider production, passenger car",
                "glider_production_without_eol",
                "market for used glider, passenger car",
            ),
            (
                "powertrain production, for electric passenger car",
                "powertrain_production_without_eol",
                "market for used powertrain from electric passenger car, manual dismantling",
            ),
            # For the battery, some waste treatment is buried in the cell production.
            # For simplicity, we just leave it in there.
            ("battery production, Li-ion, LiMn2O4, rechargeable", "battery_production_without_eol", None),
        ]:
            try:
                db.get(code=code_).delete()
            except Exception:
                pass
            without_eol = db.get(name=name).copy(code=code_, database=db.name)
            without_eol["name"] = f"{name}, without EOL"
            without_eol.save()
            if eol_name:
                for exc in without_eol.exchanges():
                    if exc.input["name"] == eol_name:
                        exc.delete()

    # Background processes our foreground links to
    glider_production = db_2020.get(code="glider_production_without_eol")
    powertrain_production = db_2020.get(code="powertrain_production_without_eol")
    battery_production = db_2020.get(code="battery_production_without_eol")
    glider_eol = db_2020.get(name="treatment of used glider, passenger car, shredding")
    powertrain_eol = db_2020.get(
        name="treatment of used powertrain for electric passenger car, manual dismantling"
    )
    battery_eol = db_2020.get(name="market for used Li-ion battery")
    electricity_production = db_2020.get(
        name="market group for electricity, low voltage", location="DEU"
    )

    # Foreground
    if "foreground" in bd.databases:
        del bd.databases["foreground"]
    foreground = bd.Database("foreground")
    foreground.register()

    ev_production = foreground.new_node(
        "ev_production", name="production of an electric vehicle", unit="unit"
    )
    ev_production["reference product"] = "electric vehicle"
    ev_production.save()

    driving = foreground.new_node(
        "driving", name="driving an electric vehicle", unit="transport over an ev lifetime"
    )
    driving["reference product"] = "transport"
    driving.save()

    used_ev = foreground.new_node("used_ev", name="used electric vehicle", unit="unit")
    used_ev["reference product"] = "used electric vehicle"
    used_ev.save()

    ev_production.new_edge(input=ev_production, amount=1, type="production").save()
    ev_production.new_edge(input=glider_production, amount=MASS_GLIDER, type="technosphere").save()
    ev_production.new_edge(input=powertrain_production, amount=MASS_POWERTRAIN, type="technosphere").save()
    ev_production.new_edge(input=battery_production, amount=MASS_BATTERY, type="technosphere").save()

    used_ev.new_edge(input=used_ev, amount=-1, type="production").save()  # -1: gets rid of a used car
    used_ev.new_edge(input=glider_eol, amount=-MASS_GLIDER, type="technosphere").save()
    used_ev.new_edge(input=powertrain_eol, amount=-MASS_POWERTRAIN, type="technosphere").save()
    used_ev.new_edge(input=battery_eol, amount=-MASS_BATTERY, type="technosphere").save()

    driving.new_edge(input=driving, amount=1, type="production").save()
    driving.new_edge(input=ev_production, amount=1, type="technosphere").save()
    driving.new_edge(input=used_ev, amount=-1, type="technosphere").save()
    driving.new_edge(
        input=electricity_production,
        amount=ELECTRICITY_CONSUMPTION * MILEAGE,
        type="technosphere",
    ).save()

    foreground.process()
    ```

## Adding temporal information

This is where `bw_timex` comes in. We tell the model *when* the exchanges of our product system happen, relative to the process that consumes them:

```mermaid
flowchart LR
    glider_production(glider production):::ei-->|0-2 years prior|ev_production
    powertrain_production(powertrain production):::ei-->|1 year prior|ev_production
    battery_production(battery production):::ei-->|1 year prior|ev_production
    ev_production(ev production):::fg-->|2-3 months prior|driving
    electricity_generation(electricity generation):::ei-->|uniformly distributed <br/> over lifetime|driving
    driving(driving):::fg-->|after ev lifetime|used_ev
    used_ev(used ev):::fg-->|3 months after <br/> ev lifetime|glider_eol(glider eol):::ei
    used_ev-->|3 months after <br/> ev lifetime|powertrain_eol(powertrain eol):::ei
    used_ev-->|3 months after <br/> ev lifetime|battery_eol(battery eol):::ei

    classDef ei color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#9c5ffd, stroke:none;
```

Such timing information is stored as a `TemporalDistribution` from [`bw_temporalis`](https://github.com/brightway-lca/bw_temporalis): a set of points in time (here relative to the consuming process), and the share of the exchange amount that occurs at each of them.


```python
import numpy as np
from bw_temporalis import TemporalDistribution, easy_timedelta_distribution

td_glider_production = TemporalDistribution(
    date=np.array([-2, -1, 0], dtype="timedelta64[Y]"), amount=np.array([0.7, 0.1, 0.2])
)

td_powertrain_and_battery_production = TemporalDistribution(
    date=np.array([-1], dtype="timedelta64[Y]"), amount=np.array([1])
)

td_assembly_and_delivery = TemporalDistribution(
    date=np.array([-3, -2], dtype="timedelta64[M]"), amount=np.array([0.2, 0.8])
)

td_use_phase = easy_timedelta_distribution(
    start=0,
    end=LIFETIME,
    resolution="Y",
    steps=(LIFETIME + 1),
    kind="uniform",
)

td_disassemble_used_ev = TemporalDistribution(
    date=np.array([LIFETIME + 1], dtype="timedelta64[Y]"), amount=np.array([1])
)

td_treating_waste = TemporalDistribution(
    date=np.array([3], dtype="timedelta64[M]"), amount=np.array([1])
)
```

Let's look at one of them:


```python
td_glider_production.graph(resolution="M")
```




    <Axes: xlabel='Time (Months)', ylabel='Amount'>




    
![png](example_electric_vehicle_premise_simple_files/output_8_1.png)
    


Now we attach the temporal distributions to the exchanges they belong to. The helper function `add_temporal_distribution_to_exchange` from `bw_timex.utils` finds an exchange for you, based on the process it comes from (`input_...`) and the process it goes to (`output_...`). We identify the processes by their name and database here, adding the location where the name alone is not unique.


```python
from bw_timex.utils import add_temporal_distribution_to_exchange

add_temporal_distribution_to_exchange(
    td_glider_production,
    input_name="glider production, passenger car, without EOL",
    input_database=db_2020.name,
    output_name="production of an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_powertrain_and_battery_production,
    input_name="powertrain production, for electric passenger car, without EOL",
    input_database=db_2020.name,
    output_name="production of an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_powertrain_and_battery_production,
    input_name="battery production, Li-ion, LiMn2O4, rechargeable, without EOL",
    input_database=db_2020.name,
    output_name="production of an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_assembly_and_delivery,
    input_name="production of an electric vehicle",
    input_database="foreground",
    output_name="driving an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_use_phase,
    input_name="market group for electricity, low voltage",
    input_database=db_2020.name,
    input_location="DEU",
    output_name="driving an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_disassemble_used_ev,
    input_name="used electric vehicle",
    input_database="foreground",
    output_name="driving an electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_treating_waste,
    input_name="treatment of used glider, passenger car, shredding",
    input_database=db_2020.name,
    output_name="used electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_treating_waste,
    input_name="treatment of used powertrain for electric passenger car, manual dismantling",
    input_database=db_2020.name,
    output_name="used electric vehicle",
    output_database="foreground",
)
add_temporal_distribution_to_exchange(
    td_treating_waste,
    input_name="market for used Li-ion battery",
    input_database=db_2020.name,
    output_name="used electric vehicle",
    output_database="foreground",
)
```

    2026-08-03 14:15:54.925 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: 840 kilogram 'glider production, passenger car, without EOL' (kilogram, GLO, None) to 'production of an electric vehicle' (unit, GLO, None).
    2026-08-03 14:15:55.047 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: 80 kilogram 'powertrain production, for electric passenger car, without EOL' (kilogram, GLO, None) to 'production of an electric vehicle' (unit, GLO, None).
    2026-08-03 14:15:55.172 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: 280 kilogram 'battery production, Li-ion, LiMn2O4, rechargeable, without EOL' (kilogram, GLO, None) to 'production of an electric vehicle' (unit, GLO, None).
    2026-08-03 14:15:55.177 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: 1 unit 'production of an electric vehicle' (unit, GLO, None) to 'driving an electric vehicle' (transport over an ev lifetime, GLO, None).
    2026-08-03 14:15:55.303 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: 30000.0 kilowatt hour 'market group for electricity, low voltage' (kilowatt hour, DEU, None) to 'driving an electric vehicle' (transport over an ev lifetime, GLO, None).
    2026-08-03 14:15:55.307 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: -1 unit 'used electric vehicle' (unit, GLO, None) to 'driving an electric vehicle' (transport over an ev lifetime, GLO, None).
    2026-08-03 14:15:55.432 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: -840 kilogram 'treatment of used glider, passenger car, shredding' (kilogram, GLO, None) to 'used electric vehicle' (unit, GLO, None).
    2026-08-03 14:15:55.553 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: -80 kilogram 'treatment of used powertrain for electric passenger car, manual dismantling' (kilogram, GLO, None) to 'used electric vehicle' (unit, GLO, None).
    2026-08-03 14:15:55.685 | INFO     | bw_timex.utils:add_temporal_distribution_to_exchange:670 - Added temporal distribution to exchange Exchange: -280 kilogram 'market for used Li-ion battery' (kilogram, GLO, None) to 'used electric vehicle' (unit, GLO, None).


## Time-explicit LCA

Besides the functional unit and the impact assessment method, `bw_timex` needs to know which point in time each background database represents. Databases that carry temporal distributions - here our foreground - are flagged as `"dynamic"`.


```python
from datetime import datetime

functional_unit = {driving: 1}  # transport over 1 ev lifetime

method = ("ecoinvent-3.12", "EF v3.1", "climate change", "global warming potential (GWP100)")

database_dates = {
    db_2020.name: datetime.strptime("2020", "%Y"),
    db_2030.name: datetime.strptime("2030", "%Y"),
    db_2040.name: datetime.strptime("2040", "%Y"),
    "foreground": "dynamic",
}
```

With that, we can set up a `TimexLCA`. It works like a normal `bw2calc.LCA`, with `database_dates` as the extra argument:


```python
from bw_timex import TimexLCA

tlca = TimexLCA(functional_unit, method, database_dates)
```

    2026-08-03 14:15:55.695 | INFO     | bw_timex.timex_lca:__init__:136 - Initializing TimexLCA object...
    2026-08-03 14:15:55.696 | INFO     | bw_timex.timex_lca:__init__:153 - Calculating base LCA...
    /Users/timodiepers/Documents/Coding/bw_timex/.venv/lib/python3.12/site-packages/scikits/umfpack/umfpack.py:737: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 1.65e+13)
      warnings.warn(msg, UmfpackWarning)
    2026-08-03 14:17:22.072 | INFO     | bw_timex.timex_lca:__init__:170 - Collecting node infos...


First, `bw_timex` traverses the supply chain and collects when each process occurs in a timeline:


```python
tlca.build_timeline(
    temporal_grouping="month", # aggregate timeline to next month
    graph_traversal="bfs", # breadth first traversal because foreground is relatively small
    )
```

    2026-08-03 14:17:29.332 | INFO     | bw_timex.timex_lca:build_timeline:342 - No edge filter function provided. Skipping all edges in background databases.
    2026-08-03 14:17:34.986 | INFO     | bw_timex.timex_lca:build_timeline:363 - Creating activity time mapping...
    2026-08-03 14:17:35.127 | INFO     | bw_timex.timeline_builder:__init__:112 - Traversing supply chain graph...
    2026-08-03 14:17:35.178 | INFO     | bw_timex.timeline_builder:build_timeline:186 - Building timeline...
    2026-08-03 14:17:35.234 | INFO     | bw_timex.timeline_builder:get_weights_for_interpolation_between_nearest_years:659 - Reference date 2040-08-01 00:00:00 is higher than all provided dates. Data will be taken from the closest lower year.





<table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date_producer</th>
      <th>producer_name</th>
      <th>date_consumer</th>
      <th>consumer_name</th>
      <th>amount</th>
      <th>temporal_market_shares</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2024-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>588.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.567, 'ei31...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2024-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>588.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.558, 'ei31...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2025-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>84.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.467, 'ei31...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2025-05-01</td>
      <td>powertrain production, for electric passenger ...</td>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>80.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.467, 'ei31...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2025-05-01</td>
      <td>battery production, Li-ion, LiMn2O4, rechargea...</td>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>280.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.467, 'ei31...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2025-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>84.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.459, 'ei31...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2025-06-01</td>
      <td>powertrain production, for electric passenger ...</td>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>80.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.459, 'ei31...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2025-06-01</td>
      <td>battery production, Li-ion, LiMn2O4, rechargea...</td>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>280.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.459, 'ei31...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2026-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>168.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.367, 'ei31...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2026-05-01</td>
      <td>production of an electric vehicle</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>0.2</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2026-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>168.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.359, 'ei31...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2026-06-01</td>
      <td>production of an electric vehicle</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>0.8</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2026-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.342, 'ei31...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>2026-08-01</td>
      <td>-1</td>
      <td>1.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2027-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.242, 'ei31...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2028-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.142, 'ei31...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2029-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2020': 0.042, 'ei31...</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2030-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.942, 'ei31...</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2031-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.842, 'ei31...</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2032-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.742, 'ei31...</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2033-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.642, 'ei31...</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2034-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.542, 'ei31...</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2035-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.442, 'ei31...</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2036-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.342, 'ei31...</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2037-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.242, 'ei31...</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2038-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.142, 'ei31...</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2039-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2030': 0.042, 'ei31...</td>
    </tr>
    <tr>
      <th>27</th>
      <td>2040-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2040': 1}</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2041-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2040': 1}</td>
    </tr>
    <tr>
      <th>29</th>
      <td>2042-08-01</td>
      <td>used electric vehicle</td>
      <td>2026-08-01</td>
      <td>driving an electric vehicle</td>
      <td>-1.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>30</th>
      <td>2042-11-01</td>
      <td>treatment of used glider, passenger car, shred...</td>
      <td>2042-08-01</td>
      <td>used electric vehicle</td>
      <td>-840.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2040': 1}</td>
    </tr>
    <tr>
      <th>31</th>
      <td>2042-11-01</td>
      <td>market for used Li-ion battery</td>
      <td>2042-08-01</td>
      <td>used electric vehicle</td>
      <td>-280.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2040': 1}</td>
    </tr>
    <tr>
      <th>32</th>
      <td>2042-11-01</td>
      <td>treatment of used powertrain for electric pass...</td>
      <td>2042-08-01</td>
      <td>used electric vehicle</td>
      <td>-80.0</td>
      <td>{'ei312_REMIND-EU_SSP2_NDC_2040': 1}</td>
    </tr>
  </tbody>
</table>




Next, we calculate the time-explicit inventory. This relinks all processes to the background databases matching their timing:


```python
tlca.lci()
```

    2026-08-03 14:17:35.876 | INFO     | bw_timex.timex_lca:lci:513 - Expanding matrices...
    2026-08-03 14:17:35.893 | INFO     | bw_timex.timex_lca:lci:532 - Calculating dynamic inventory...
    /Users/timodiepers/Documents/Coding/bw_timex/.venv/lib/python3.12/site-packages/scikits/umfpack/umfpack.py:737: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 4.97e+12)
      warnings.warn(msg, UmfpackWarning)
    /Users/timodiepers/Documents/Coding/bw_timex/.venv/lib/python3.12/site-packages/scikits/umfpack/umfpack.py:737: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 4.97e+12)
      warnings.warn(msg, UmfpackWarning)


Now we can get the time-explicit score, using the static characterization method we chose above:


```python
tlca.static_lcia()
tlca.static_score  # kg CO2-eq
```




    10744.473494732374



For comparison, a conventional LCA with a fixed background database (here: 2020) - `bw_timex` calculated this along the way:


```python
tlca.base_lca.score  # kg CO2-eq
```




    23247.15112358249



## Dynamic characterization

Since we know *when* each emission occurs, we can also characterize them dynamically, instead of lumping everything together at one point in time. `bw_timex` automatically maps the parameterized characterization functions from [`dynamic_characterization`](https://dynamic-characterization.readthedocs.io/en/latest/) (based on IPCC AR6) to the ecoinvent biosphere flows:


```python
tlca.dynamic_lcia(metric="GWP")
tlca.dynamic_score  # kg CO2-eq (GWP100)
```

    2026-08-03 14:17:49.679 | INFO     | dynamic_characterization.dynamic_characterization:characterize:126 - No custom dynamic characterization functions provided. Using default dynamic             characterization functions. The flows that are characterized are based on the selection                of the initially chosen impact category.





    np.float64(10655.763775235002)



And plot how that impact is distributed over time, stacked by the processes causing it:


```python
from bw_timex.utils import plot_characterized_inventory_as_waterfall

plot_characterized_inventory_as_waterfall(tlca)
```


    
![png](example_electric_vehicle_premise_simple_files/output_26_0.png)
    


That's it: same modelling as a normal LCA, plus temporal distributions on the exchanges, and the background databases are chosen according to when things actually happen.

If you want to dig deeper - other temporal resolutions, custom graph traversal, radiative forcing as a metric, or a comparison against fully prospective results - have a look at the [detailed version](example_electric_vehicle_premise.md) of this example.
