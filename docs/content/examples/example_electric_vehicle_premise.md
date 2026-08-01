---
icon: lucide/car-front
tags:
  - example
  - premise
---


<div hidden data-source-edit-path="docs/content/examples/example_electric_vehicle_premise.ipynb" data-source-view-path="docs/content/examples/example_electric_vehicle_premise.ipynb"></div>
# Time-explicit LCA of an electric vehicle


This notebook shows how to use `bw_timex` with a cradle-to-grave case study of an electric vehicle. The case study is simplified, not meant to reflect the complexity of electric mobility but to demonstrate hot to use `bw_timex`. 

More information on the inner workings of `bw_timex` can be found [here](https://timex.readthedocs.io/en/latest/content/theory.html).


> **Note:** This is the "premise" version of this notebook that works with ecoinvent and premise data. If you don't have access to that, please check out the ["standalone" version](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/example_electric_vehicle_standalone.ipynb) of this notebook.


```python { .notebook-cell }
import bw2data as bd

bd.projects.set_current("timex")
```

## Prospective background databases

The `bw_timex` package itself does not provide any data - specifying prospective and dynamic information is up to the user. In this example, we use data from [ecoinvent v3.10](https://ecoinvent.org/), and create a set of prospective databases with [`premise`](https://github.com/polca/premise). We applied projections for the future electricity sectors using the SSP2-RCP19 pathway from the IAM IMAGE. We selected this pathway to simply demonstrate some future development in this case study, and many other models and pathways are available. 
In the [premise documentation](https://premise.readthedocs.io/en/latest/) you can find instructions for the creation of prospective background databases. 



```python { .notebook-cell }
db_2020 = bd.Database("ei310_IMAGE_SSP2_RCP19_2020_electricity")
db_2030 = bd.Database("ei310_IMAGE_SSP2_RCP19_2030_electricity")
db_2040 = bd.Database("ei310_IMAGE_SSP2_RCP19_2040_electricity")
```

## Case study setup


In this study, we consider the following production system for our ev. Purple boxes are foreground, cyan boxes are background (i.e., ecoinvent/premise).

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

### Modeling the production system

Now, we need to build this with brightway. If you are not interested in the modeling details, feel free to skip this section.

For our ev model we make the following assumptions:


```python { .notebook-cell }
ELECTRICITY_CONSUMPTION = 0.2 # kWh/km
MILEAGE = 150_000 # km
LIFETIME = 15 # years

# Overall mass: 1200 kg
MASS_GLIDER = 840 # kg
MASS_POWERTRAIN = 80 # kg
MASS_BATTERY = 280 # kg
```

First, we create a new foreground database:


```python { .notebook-cell }
if "foreground" in bd.databases:
    del bd.databases["foreground"] # to make sure we create the foreground from scratch
foreground = bd.Database("foreground")
foreground.register()
```

Now, let's creating the foreground activities:



```python { .notebook-cell }
ev_production = foreground.new_node("ev_production", name="production of an electric vehicle", unit="unit")
ev_production['reference product'] = "electric vehicle"
ev_production.save()

driving = foreground.new_node("driving", name="driving an electric vehicle", unit="transport over an ev lifetime")
driving['reference product'] = "transport"
driving.save()

used_ev = foreground.new_node("used_ev", name="used electric vehicle", unit="unit")
used_ev['reference product'] = "used electric vehicle"
used_ev.save()
```

We take the actual process data from ecoinvent. However, the ecoinvent processes for the ev part production contain exchanges for the end of life treatment in the production processes already, which we want to separate. So let's fix that first by creating new activities without the eol processes:


```python { .notebook-cell }
for db in [db_2020, db_2030, db_2040]:
    for code in ["glider_production_without_eol", "powertrain_production_without_eol", "battery_production_without_eol"]:
        try:
            act = db.get(code=code)
            act.delete()
        except:
            pass
    
    glider_production = db.get(name="glider production, passenger car")
    glider_production_without_eol = glider_production.copy(code="glider_production_without_eol", database=db.name)
    glider_production_without_eol["name"] = "glider production, passenger car, without EOL"
    # glider_production_without_eol["reference product"] = "glider"
    glider_production_without_eol.save()
    for exc in glider_production_without_eol.exchanges():
        if exc.input["name"] == "market for used glider, passenger car":
            exc.delete()
    
    powertrain_production = db.get(name="powertrain production, for electric passenger car")
    powertrain_production_without_eol = powertrain_production.copy(code="powertrain_production_without_eol", database=db.name)
    powertrain_production_without_eol["name"] = "powertrain production, for electric passenger car, without EOL"
    # powertrain_production_without_eol["reference product"] = "powertrain"
    powertrain_production_without_eol.save()
    for exc in powertrain_production_without_eol.exchanges():
        if exc.input["name"] == "market for used powertrain from electric passenger car, manual dismantling":
            exc.delete()
    
    battery_production = db.get(name="battery production, Li-ion, LiMn2O4, rechargeable, prismatic")
    battery_production_without_eol = battery_production.copy(code="battery_production_without_eol", database=db.name)
    battery_production_without_eol["name"] = "battery production, Li-ion, LiMn2O4, rechargeable, prismatic, without EOL"
    # battery_production_without_eol["reference product"] = "battery"
    battery_production_without_eol.save()
    # For the battery, some waste treatment is buried in the process "battery cell production, Li-ion, 
    # LiMn2O4" - but not for the whole mass of the battery(?). For simplicity, we just leave it in there.
```

Now, let's build the exchanges, starting with the ev production:


```python { .notebook-cell }
glider_production = db_2020.get(code="glider_production_without_eol")
powertrain_production = db_2020.get(code="powertrain_production_without_eol")
battery_production = db_2020.get(code="battery_production_without_eol")

ev_production.new_edge(input=ev_production, amount=1, type="production").save()

glider_to_ev = ev_production.new_edge(
    input=glider_production,
    amount=MASS_GLIDER, 
    type="technosphere"
)
powertrain_to_ev = ev_production.new_edge(
    input=powertrain_production, 
    amount=MASS_POWERTRAIN, 
    type="technosphere"
)
battery_to_ev = ev_production.new_edge(
    input=battery_production, 
    amount=MASS_BATTERY, 
    type="technosphere"
)
```

... the end of life:


```python { .notebook-cell }
glider_eol = db_2020.get(name="treatment of used glider, passenger car, shredding")
powertrain_eol = db_2020.get(name="treatment of used powertrain for electric passenger car, manual dismantling")
battery_eol = db_2020.get(name="market for used Li-ion battery")

used_ev.new_edge(input=used_ev, amount=-1, type="production").save()  # -1 as this gets rid of a used car

used_ev_to_glider_eol = used_ev.new_edge(
    input=glider_eol,
    amount=-MASS_GLIDER,
    type="technosphere",
)
used_ev_to_powertrain_eol = used_ev.new_edge(
    input=powertrain_eol,
    amount=-MASS_POWERTRAIN,
    type="technosphere",
)
used_ev_to_battery_eol = used_ev.new_edge(
    input=battery_eol,
    amount=-MASS_BATTERY,
    type="technosphere",
)
```

...and, finally, driving:


```python { .notebook-cell }
electricity_production = db_2020.get(name="market group for electricity, low voltage", location="WEU")

driving.new_edge(input=driving, amount=1, type="production").save()

driving_to_used_ev = driving.new_edge(input=used_ev, amount=-1, type="technosphere")
ev_to_driving = driving.new_edge(
    input=ev_production, 
    amount=1, 
    type="technosphere"
)
electricity_to_driving = driving.new_edge(
    input=electricity_production,
    amount=ELECTRICITY_CONSUMPTION * MILEAGE,
    type="technosphere",
)
```

### Adding temporal information

Now that the production system is modelled, we can add temporal distributions at the exchange level. The temporal information we want to embed in our product system looks somewhat like this:

```mermaid
flowchart LR
    glider_production(glider production):::ei-->|"0-2 years prior \n &nbsp;"|ev_production
    powertrain_production(powertrain production):::ei-->|"1 year prior \n &nbsp;"|ev_production
    battery_production(battery production):::ei-->|"&nbsp; \n 1 year prior"|ev_production
    ev_production(ev production):::fg-->|"0-3 months prior \n &nbsp;"|driving
    electricity_generation(electricity generation):::ei-->|uniformly distributed \n over lifetime|driving
    driving(driving):::fg-->|"after ev lifetime \n &nbsp;"|used_ev
    used_ev(used ev):::fg-->|3 months after \n ev lifetime|glider_eol(glider eol):::ei
    used_ev-->|3 months after \n ev lifetime|powertrain_eol(powertrain eol):::ei
    used_ev-->|3 months after \n ev lifetime|battery_eol(battery eol):::ei

    classDef ei color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#9c5ffd, stroke:none;
```

To include this temopral information, we use the `TemporalDistribution` class from `bw_temporalis`. For more info, take a look at the [bw_temporalis documentation](https://github.com/brightway-lca/bw_temporalis).

Notably, in addition to the timestamp of the occurence of the process (which is shown in the flowchart above), we also need to specify the amount share of the exchange that happens at that time to fully define a `TemporalDistribution`.



```python { .notebook-cell }
from bw_temporalis import TemporalDistribution, easy_timedelta_distribution
import numpy as np

td_assembly_and_delivery = TemporalDistribution(
    date=np.array([-3, -2], dtype="timedelta64[M]"), amount=np.array([0.2, 0.8])
)

td_glider_production = TemporalDistribution(
    date=np.array([-2, -1, 0], dtype="timedelta64[Y]"), amount=np.array([0.7, 0.1, 0.2])
)

td_produce_powertrain_and_battery = TemporalDistribution(
    date=np.array([-1], dtype="timedelta64[Y]"), amount=np.array([1])
)

td_use_phase = easy_timedelta_distribution(
    start=0,
    end=LIFETIME,
    resolution="Y",
    steps=(LIFETIME + 1),
    kind="uniform", # you can also do "normal" or "triangular" distributions
)

td_disassemble_used_ev = TemporalDistribution(
    date=np.array([LIFETIME + 1], dtype="timedelta64[Y]"), amount=np.array([1])
)

td_treating_waste = TemporalDistribution(
    date=np.array([3], dtype="timedelta64[M]"), amount=np.array([1])
)
```

Let's explore what a `TemporalDistribution` looks like:


```python { .notebook-cell }
td_assembly_and_delivery.graph(resolution="M")
```




    <AxesSubplot:xlabel='Time (Months)', ylabel='Amount'>




    
![png](example_electric_vehicle_premise_files/output_25_1.png)
    



```python { .notebook-cell }
td_glider_production.graph(resolution="M")
```




    <AxesSubplot:xlabel='Time (Months)', ylabel='Amount'>




    
![png](example_electric_vehicle_premise_files/output_26_1.png)
    


Starting from the functional unit in our supply chain graph, the temporal distributions of consecutive edges get "multiplied", or more specifically, convolved. Let's look at an example to clarify this. The assembly and delivery of our ev happens either 2 or 3 months before we can start using it. Each of these occurences of this process demands a glider, which also has a temporal distribution that then gets convolved "back in time". Also pay attention to how the amounts get scaled.


```python { .notebook-cell }
(td_assembly_and_delivery * td_glider_production).graph(resolution="M")
```




    <AxesSubplot:xlabel='Time (Months)', ylabel='Amount'>




    
![png](example_electric_vehicle_premise_files/output_28_1.png)
    


We now add the temporal information to the exchanges of our EV. We add temporal distributions to all (technosphere) exchanges, but you don't have to.



```python { .notebook-cell }
glider_to_ev["temporal_distribution"] = td_glider_production
glider_to_ev.save()

powertrain_to_ev["temporal_distribution"] = td_produce_powertrain_and_battery
powertrain_to_ev.save()

battery_to_ev["temporal_distribution"] = td_produce_powertrain_and_battery
battery_to_ev.save()

ev_to_driving["temporal_distribution"] = td_assembly_and_delivery
ev_to_driving.save()

electricity_to_driving["temporal_distribution"] = td_use_phase
electricity_to_driving.save()

driving_to_used_ev["temporal_distribution"] = td_disassemble_used_ev
driving_to_used_ev.save()

used_ev_to_glider_eol["temporal_distribution"] = td_treating_waste
used_ev_to_glider_eol.save()

used_ev_to_powertrain_eol["temporal_distribution"] = td_treating_waste
used_ev_to_powertrain_eol.save()

used_ev_to_battery_eol["temporal_distribution"] = td_treating_waste
used_ev_to_battery_eol.save()
```

## LCA using `bw_timex`


As usual, we need to select an impact assessment method:


```python { .notebook-cell }
method = ('EF v3.1', 'climate change', 'global warming potential (GWP100)')
```

`bw_timex` also needs to know the representative time of the databases:


```python { .notebook-cell }
from datetime import datetime

database_dates = {
    db_2020.name: datetime.strptime("2020", "%Y"),
    db_2030.name: datetime.strptime("2030", "%Y"),
    db_2040.name: datetime.strptime("2040", "%Y"),
    "foreground": "dynamic", # flag databases that should be temporally distributed with "dynamic"
}
```

Now, we can instantiate a `TimexLCA`. It's structure is similar to a normal `bw2calc.LCA`, but with the additional argument `database_dates`.

Not sure about the required inputs? Check the documentation using `?`. All our classes and methods have docstrings!


```python { .notebook-cell }
from bw_timex import TimexLCA
TimexLCA?
```

    Init signature: TimexLCA(demand: dict, method: tuple, database_dates: dict = None) -> None
    Docstring:     
    Class to perform time-explicit LCA calculations.
    
    A TimexLCA retrieves the LCI of processes occuring at explicit points in time and relinks their technosphere
    exchanges to match the technology landscape at that point in time, while keeping track of the timing of the
    resulting emissions. As such, it combines prospective and dynamic LCA approaches.
    
    TimexLCA first calculates a static LCA, which informs a priority-first graph traversal. From the graph traversal,
    temporal relationships between exchanges and processes are derived. Based on the timing of the processes, bw_timex
    matches the processes at the intersection between foreground and background to the best available background
    databases. This temporal relinking is achieved by using datapackages to add new time-specific processes. The new
    processes and their exchanges to other technosphere processes or biosphere flows extent the technopshere and
    biosphere matrices.
    
    Temporal information of both processes and biosphere flows are retained, allowing for dynamic LCIA.
    
    Currently absolute Temporal Distributions for biosphere exchanges are dealt with as a look up function:
    If an activity happens at timestamp X then and the biosphere exchange has an absolute temporal
    distribution (ATD), it looks up the amount from from the ATD correspnding to timestamp X.
    E.g.: X = 2024, TD=(data=[2020,2021,2022,2023,2024,.....,2120 ], amount=[3,4,4,5,6,......,3]),
    it will look up the value 6 corresponding 2024. If timestamp X does not exist it find the nearest
    timestamp available (if two timestamps are equally close, it will take the first in order of
    apearance (see numpy.argmin() for this behabiour).
    
    
    TimexLCA calculates:
     1) a static LCA score (`TimexLCA.base_lca.score`, same as `bw2calc.lca.score`),
     2) a static time-explicit LCA score (`TimexLCA.static_score`), which links LCIs to the respective background databases but without additional temporal dynamics of the biosphere flows,
     3) a dynamic time-explicit LCA score (`TimexLCA.dynamic_score`), with dynamic inventory and dynamic charaterization factors. These are provided for radiative forcing and GWP but can also be user-defined.
    
    Example
    -------
    >>> demand = {('my_foreground_database', 'my_process'): 1}
    >>> method = ("some_method_family", "some_category", "some_method")
    >>> database_dates = {
            'my_background_database_one': datetime.strptime("2020", "%Y"),
            'my_background_database_two': datetime.strptime("2030", "%Y"),
            'my_foreground_database':'dynamic'
        }
    >>> bw_timex = TimexLCA(demand, method, database_dates)
    >>> bw_timex.build_timeline() # you can pass many optional arguments here, also for the graph traversal
    >>> bw_timex.lci()
    >>> bw_timex.static_lcia()
    >>> print(bw_timex.static_score)
    >>> bw_timex.dynamic_lcia(metric="radiative_forcing") # different metrics can be used, e.g. "GWP", "radiative_forcing"
    >>> print(bw_timex.dynamic_score)
    Init docstring:
    Instantiating a `TimexLCA` object calculates a static LCA, initializes time mapping dicts for activities and biosphere flows, and stores useful subsets of ids in the node_collections.
    
    Parameters
    ----------
    demand : dict[object: float]
            The demand for which the LCA will be calculated. The keys can be Brightway `Node`
            instances, `(database, code)` tuples, or integer ids.
    method : tuple
            Tuple defining the LCIA method, such as `('foo', 'bar')` or default methods, such as `("EF v3.1", "climate change", "global warming potential (GWP100)")`
    database_dates : dict, optional
            Dictionary mapping database names to dates.
    File:           ~/Documents/Coding/bw_timex/bw_timex/timex_lca.py
    Type:           type
    Subclasses:     

Let's instantiate a `TimexLCA` object for our "driving" activity:


```python { .notebook-cell }
tlca = TimexLCA({driving: 1}, method, database_dates)
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/scikits/umfpack/umfpack.py:736: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 1.21e+13)
      warnings.warn(msg, UmfpackWarning)


Next, we build a timeline of the exchanges. To do this, we can call the `build_timeline()` method, which does the graph traversal and creates a timeline dataframe from the results. The exchanges (rows of the dataframe) are aggregated to the resolution specified in the argument `temporal_grouping`. There are also many more options to specify the timeline creation and graph traversal process. Here are the most important ones:
- `temporal_grouping`: temporal resolution to which processes will be aggregated,"year" (default), "month", "day" or "hour"
- `interpolation_type`: How the best fitting background database is selected: "linear"(default), "closest"
- `edge_filter_function`: Custom filter function specifying when to stop the graph traversal.
- `cutoff`: stops graph traversal for nodes below this contribution to the static impact score.
- `max_calc`: stops graph traversal if this number of nodes has been traversed

For all these options, we provide sensible default values. Of course you can always just check the docstrings to see all your options and our assumptions for default values. 

So, let's build the timeline. We choose a monthly temporal grouping here because we use that resolution in our temporal distributions.



```python { .notebook-cell }
tlca.build_timeline(temporal_grouping="month")
```

    /Users/timodiepers/Documents/Coding/bw_timex/bw_timex/timex_lca.py:194: UserWarning: No edge filter function provided. Skipping all edges within background databases.
      warnings.warn(


    Starting graph traversal
    Calculation count: 9


    /Users/timodiepers/Documents/Coding/bw_timex/bw_timex/timeline_builder.py:527: Warning: Reference date 2040-08-01 00:00:00 is higher than all provided dates. Data will be taken from the closest lower year.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/bw_timex/bw_timex/timeline_builder.py:527: Warning: Reference date 2040-11-01 00:00:00 is higher than all provided dates. Data will be taken from the closest lower year.
      warnings.warn(





<table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date_producer</th>
      <th>producer_name</th>
      <th>date_consumer</th>
      <th>consumer_name</th>
      <th>amount</th>
      <th>interpolation_weights</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2022-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>588.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2022-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>588.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>84.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-05-01</td>
      <td>powertrain production, for electric passenger ...</td>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>80.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-05-01</td>
      <td>battery production, Li-ion, LiMn2O4, rechargea...</td>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>280.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2023-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>84.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2023-06-01</td>
      <td>powertrain production, for electric passenger ...</td>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>80.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2023-06-01</td>
      <td>battery production, Li-ion, LiMn2O4, rechargea...</td>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>280.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2024-05-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>168.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2024-05-01</td>
      <td>production of an electric vehicle</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>0.2</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2024-06-01</td>
      <td>glider production, passenger car, without EOL</td>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>168.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2024-06-01</td>
      <td>production of an electric vehicle</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>0.8</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2024-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>2024-08-01</td>
      <td>-1</td>
      <td>1.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2025-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2026-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2027-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2028-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2029-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2020_electricity': 0....</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2030-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2031-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2032-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2033-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2034-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2035-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2036-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2037-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>27</th>
      <td>2038-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2039-08-01</td>
      <td>market group for electricity, low voltage</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>1875.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2030_electricity': 0....</td>
    </tr>
    <tr>
      <th>29</th>
      <td>2040-08-01</td>
      <td>used electric vehicle</td>
      <td>2024-08-01</td>
      <td>driving an electric vehicle</td>
      <td>-1.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>30</th>
      <td>2040-11-01</td>
      <td>market for used Li-ion battery</td>
      <td>2040-08-01</td>
      <td>used electric vehicle</td>
      <td>-280.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2040_electricity': 1}</td>
    </tr>
    <tr>
      <th>31</th>
      <td>2040-11-01</td>
      <td>treatment of used powertrain for electric pass...</td>
      <td>2040-08-01</td>
      <td>used electric vehicle</td>
      <td>-80.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2040_electricity': 1}</td>
    </tr>
    <tr>
      <th>32</th>
      <td>2040-11-01</td>
      <td>treatment of used glider, passenger car, shred...</td>
      <td>2040-08-01</td>
      <td>used electric vehicle</td>
      <td>-840.0</td>
      <td>{'ei310_IMAGE_SSP2_RCP19_2040_electricity': 1}</td>
    </tr>
  </tbody>
</table>



The temporal market shares in the timeline (right most column above) specify the share of the amount of an exchange to be sourced from the respective database. 
`None` means that the exchange is in the foreground supply chain, and not at the intersection with the background system.  

Next, we calculate the time-explicit LCI. The `TimexLCA.lci()` function takes care of all the relinking, based on the information from the timeline. 


```python { .notebook-cell }
tlca.lci()
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/bw2calc/lca_base.py:127: SparseEfficiencyWarning: splu converted its input to CSC format
      self.solver = factorized(self.technosphere_matrix)
    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/scikits/umfpack/umfpack.py:736: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 5.78e+12)
      warnings.warn(msg, UmfpackWarning)
    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/scikits/umfpack/umfpack.py:736: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 5.78e+12)
      warnings.warn(msg, UmfpackWarning)


Taking a look at the `dynamic_inventory` that was now created, we can see that it has more rows (emissions) than our usual biosphere3 flows. Instead of one row for each emission in the biosphere database we now get one row for each emission at each point in time.


```python { .notebook-cell }
tlca.dynamic_inventory
```




    <Compressed Sparse Row sparse matrix of dtype 'float64'
    	with 65859 stored elements and shape (61709, 80718)>



The standard, non-dynamic inventory has far less rows because the temporal resolution is missing. Looking at the timeline again, we see that we have processes at 23 different points in time (only counting the ones that actually directly procude emissions), which exactly matches the ratio of the dimensions of our two inventories:


```python { .notebook-cell }
tlca.inventory.shape # (#rows, #cols)
```




    (2683, 80718)




```python { .notebook-cell }
tlca.dynamic_inventory.shape[0]/tlca.inventory.shape[0]
```




    23.0



While under the hood, the dynamic inventory is calculated as a sparse matrix, there is also a more human-friendly version as a pandas DataFrame:


```python { .notebook-cell }
tlca.dynamic_inventory_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>amount</th>
      <th>flow</th>
      <th>activity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>876</th>
      <td>2022-05-01</td>
      <td>3.281494e+04</td>
      <td>1584</td>
      <td>108727</td>
    </tr>
    <tr>
      <th>824</th>
      <td>2022-05-01</td>
      <td>8.636928e+03</td>
      <td>1472</td>
      <td>108727</td>
    </tr>
    <tr>
      <th>1576</th>
      <td>2022-05-01</td>
      <td>4.231631e+03</td>
      <td>3193</td>
      <td>108727</td>
    </tr>
    <tr>
      <th>578</th>
      <td>2022-05-01</td>
      <td>4.201366e+03</td>
      <td>842</td>
      <td>108727</td>
    </tr>
    <tr>
      <th>875</th>
      <td>2022-05-01</td>
      <td>9.198843e+02</td>
      <td>1583</td>
      <td>108727</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>62048</th>
      <td>2040-11-01</td>
      <td>-4.535713e-08</td>
      <td>1819</td>
      <td>108757</td>
    </tr>
    <tr>
      <th>62282</th>
      <td>2040-11-01</td>
      <td>-1.478520e-07</td>
      <td>1922</td>
      <td>108757</td>
    </tr>
    <tr>
      <th>64549</th>
      <td>2040-11-01</td>
      <td>-3.534886e-02</td>
      <td>3721</td>
      <td>108758</td>
    </tr>
    <tr>
      <th>61017</th>
      <td>2040-11-01</td>
      <td>-4.156468e-01</td>
      <td>1030</td>
      <td>108759</td>
    </tr>
    <tr>
      <th>61019</th>
      <td>2040-11-01</td>
      <td>-3.893688e+00</td>
      <td>1030</td>
      <td>108757</td>
    </tr>
  </tbody>
</table>
<p>65859 rows × 4 columns</p>
</div>



If we are only interested in the new overall time-explicit scores and don't care about the timing of the emissions, we can set `build_dynamic_biosphere=False` (default is `True`), which saves time and memory. In that case, you only get the `TimexLCA.inventory`, but not the `TimexLCA.dynamic_inventory`.

In case the timing of emissions is not important, one can directly calculate the LCIA the "standard way" using static characterization factors. Per default, the following calculates the static lcia score based on the impact method chosen in the very beginning:


```python { .notebook-cell }
tlca.static_lcia()
tlca.static_score   #kg CO2-eq
```




    11821.850158724601



At this point, we can already compare these time-explicit results to the results of an "ordinary", completely static LCA. These already exist within the TimexLCA class, originally to set the priorities for the graph traversal:


```python { .notebook-cell }
tlca.base_lca.score
```




    20858.470012031627



## Dynamic Characterization
In addition to the standard static characterization, the time-explicit, dynamic inventory generated by a `TimexLCA` allows for dynamic characterization. Users can provide their own dynamic characterization functions and link them to corresponding biosphere flows (see example on [dynamic characterization](https://github.com/TimoDiepers/timex/blob/main/notebooks/example_simple_dynamic_characterization.ipynb)). 

Alternatively, you can use the functions from our separate (but fully compatible) package [dynamic_characterization](https://dynamic-characterization.readthedocs.io/en/latest/). We provide two different metrics for dynamic LCIA of Climate Change: Radiative forcing [W/m2] and Global Warming Potential (GWP) [kg CO2-eq]. For both of these metrics, we have parameterized dynamic characterization functions for all GHG's that [IPCC AR6](https://www.ipcc.ch/report/ar6/wg1/chapter/chapter-7/) provides data for.

For the dynamic characterization, users can also choose the length of the considered time horizon (`time_horizon`) and whether it is a fixed time horizon (`fixed_time_horizon`). Fixed means that the time horizon for all emissions (no matter when they occur) starts counting at the time of the functional unit, resulting in shorter time horizons for emissions occuring later. If the time horizon is not fixed (this is what conventional impact assessment factors assume), it starts counting from the timing of the emission.


### Radiative forcing


Let's characterize our dynamic inventory, regarding radiative forcing with a fixed time horizon and the default time horizon length of 100 years:


```python { .notebook-cell }
tlca.dynamic_lcia(metric="radiative_forcing", fixed_time_horizon=True)
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/dynamic_characterization/dynamic_characterization.py:80: UserWarning: No custom dynamic characterization functions provided. Using default dynamic characterization functions from `dynamic_characterization` meant to work with biosphere3 flows. The flows that are characterized are based on the selection of the initially chosen impact category. You can look up the mapping in the bw_timex.dynamic_characterizer.characterization_functions.
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>amount</th>
      <th>flow</th>
      <th>activity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-05-01 05:49:12</td>
      <td>5.049646e-13</td>
      <td>1031</td>
      <td>108703</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-05-01 05:49:12</td>
      <td>2.470251e-18</td>
      <td>3792</td>
      <td>108703</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-05-01 05:49:12</td>
      <td>6.890517e-19</td>
      <td>4217</td>
      <td>108703</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-05-01 05:49:12</td>
      <td>3.461317e-18</td>
      <td>1366</td>
      <td>108703</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-05-01 05:49:12</td>
      <td>1.463020e-18</td>
      <td>1374</td>
      <td>108703</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>146565</th>
      <td>2123-11-02 03:03:36</td>
      <td>2.669520e-16</td>
      <td>1034</td>
      <td>108734</td>
    </tr>
    <tr>
      <th>146566</th>
      <td>2123-11-02 03:03:36</td>
      <td>1.487574e-21</td>
      <td>1369</td>
      <td>108733</td>
    </tr>
    <tr>
      <th>146567</th>
      <td>2123-11-02 03:03:36</td>
      <td>2.640643e-21</td>
      <td>226</td>
      <td>108733</td>
    </tr>
    <tr>
      <th>146568</th>
      <td>2123-11-02 03:03:36</td>
      <td>7.511111e-17</td>
      <td>1390</td>
      <td>108733</td>
    </tr>
    <tr>
      <th>146569</th>
      <td>2123-11-02 03:03:36</td>
      <td>3.126878e-48</td>
      <td>1152</td>
      <td>108734</td>
    </tr>
  </tbody>
</table>
<p>146570 rows × 4 columns</p>
</div>



The method call returns a dataframe of all the individual emissions at their respective timesteps (tlca.characterized_inventory), but we can also just look at the overall score:


```python { .notebook-cell }
tlca.dynamic_score #W/m2 (radiative forcing)
```




    1.0214828094580987e-09



To visualize the results, we provide a simple plotting functions:


```python { .notebook-cell }
tlca.plot_dynamic_characterized_inventory()  
```


    
![png](example_electric_vehicle_premise_files/output_64_0.png)
    


This can be a bit messy, though, because all the individual impacts caused by individual emissions (e.g., CO2, CH4, N2O, ...) appear. Luckily, there is also an option to sum the emissions within each activity:


```python { .notebook-cell }

```


```python { .notebook-cell }
tlca.plot_dynamic_characterized_inventory(sum_emissions_within_activity=True)
```


    
![png](example_electric_vehicle_premise_files/output_67_0.png)
    


There is also a flag to plot the cumulative score over time:


```python { .notebook-cell }
tlca.plot_dynamic_characterized_inventory(sum_activities=True, cumsum=True)
```


    
![png](example_electric_vehicle_premise_files/output_69_0.png)
    


### GWP


Similar options are available for the metric GWP, which compares the radiative forcing of a GHG to that of CO2 over a certain time horizon (commonly 100 years, but it can be set flexibly in `time_horizon`).


```python { .notebook-cell }
tlca.dynamic_lcia(metric="GWP", fixed_time_horizon=False, time_horizon = 70)
tlca.dynamic_score #kg CO2-eq (GWP)
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/dynamic_characterization/dynamic_characterization.py:80: UserWarning: No custom dynamic characterization functions provided. Using default dynamic characterization functions from `dynamic_characterization` meant to work with biosphere3 flows. The flows that are characterized are based on the selection of the initially chosen impact category. You can look up the mapping in the bw_timex.dynamic_characterizer.characterization_functions.
      warnings.warn(
    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/dynamic_characterization/dynamic_characterization.py:262: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(





    11996.445963730823



Plotting the GWP results over time:


```python { .notebook-cell }
tlca.plot_dynamic_characterized_inventory(sum_emissions_within_activity=True)
```


    
![png](example_electric_vehicle_premise_files/output_74_0.png)
    


Cumulative:


```python { .notebook-cell }
tlca.plot_dynamic_characterized_inventory(sum_emissions_within_activity=True, cumsum=True)
```


    
![png](example_electric_vehicle_premise_files/output_76_0.png)
    


### Comparison of time-explicit results to static results
It's helpful to understand how the time-explicit results differ from those using static assessments. 

We compare the time-explicit results with those of an LCA for the year 2020 and 2040 for the standard GWP100 metric (time horizon=100 and no fixed time horizon). This means we neglect the additional differences of the time-explicit results that would arise from using dynamic LCIA. 

Time-explicit scores:


```python { .notebook-cell }
tlca.dynamic_lcia(metric="GWP", fixed_time_horizon=False, time_horizon=100)
tlca.dynamic_score
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/dynamic_characterization/dynamic_characterization.py:80: UserWarning: No custom dynamic characterization functions provided. Using default dynamic characterization functions from `dynamic_characterization` meant to work with biosphere3 flows. The flows that are characterized are based on the selection of the initially chosen impact category. You can look up the mapping in the bw_timex.dynamic_characterizer.characterization_functions.
      warnings.warn(
    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/dynamic_characterization/dynamic_characterization.py:262: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(





    11653.498778351013



The 2020 (static) score has already been calculated by TimexLCA in the beginning, but we can still access the score:


```python { .notebook-cell }
tlca.base_lca.score
```




    20858.470012031627



However, further down we also want to look at what part of the life cycle has what contribution. To get this info, we need some more calculations:


```python { .notebook-cell }
static_scores = {}
for exc in driving.technosphere():
    if exc.input == ev_production:
        for subexc in exc.input.technosphere():
            tlca.base_lca.lcia(demand={subexc.input.id: exc.amount * subexc.amount * subexc.input.rp_exchange().amount})
            static_scores[subexc.input["name"]] = tlca.base_lca.score
    elif exc.input == used_ev:
        for subexc in exc.input.technosphere():
            tlca.base_lca.lcia(demand={subexc.input.id: exc.amount * subexc.amount * subexc.input.rp_exchange().amount})
            static_scores[subexc.input["name"]] = tlca.base_lca.score
    else:
        tlca.base_lca.lcia(demand={exc.input.id: exc.amount})
        static_scores[exc.input["name"]] = tlca.base_lca.score
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/scikits/umfpack/umfpack.py:736: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 1.21e+13)
      warnings.warn(msg, UmfpackWarning)


Similarly, we calculate the 2040 (prospective) scores by just changing the database the exchanges point to:


```python { .notebook-cell }
import bw2calc as bc

# first create a copy of the system and relink to processes from 2040 database
try:
    prospective_driving = driving.copy(code="prospective_driving", name="driving an electric vehi0cle in 2040")
except:
    foreground.get(code="prospective_driving").delete()
    prospective_driving = driving.copy(code="prospective_driving", name="driving an electric vehicle in 2040")
    

for exc in prospective_driving.technosphere():
    if exc.input == ev_production:
        prospective_ev_production = ev_production.copy(name="production of an electric vehicle in 2040")
        exc.input = prospective_ev_production
        exc.save()
        for subexc in prospective_ev_production.technosphere():
            subexc.input = bd.get_node(
                database=db_2040.name,
                name=subexc.input["name"],
                product=subexc.input["reference product"],
                location=subexc.input["location"],
            )
            subexc.save()
    elif exc.input == used_ev:
        prospective_used_ev = used_ev.copy(name="used electric vehicle in 2040")
        exc.input = prospective_used_ev
        exc.save()
        for subexc in prospective_used_ev.technosphere():
            subexc.input = bd.get_node(
                database=db_2040.name,
                name=subexc.input["name"],
                product=subexc.input["reference product"],
                location=subexc.input["location"],
            )
            subexc.save()
    else:
        exc.input = bd.get_node(
            database=db_2040.name,
            name=exc.input["name"],
            product=exc.input["reference product"],
            location=exc.input["location"],
        )
    exc.save()

prospective_scores = {}
lca = bc.LCA({prospective_driving.key: 1}, method)
lca.lci(factorize=True)
for exc in prospective_driving.technosphere():
    if exc.input["name"] in (prospective_ev_production["name"], prospective_used_ev["name"]):
        for subexc in exc.input.technosphere():
            lca.lcia(demand={subexc.input.id: exc.amount * subexc.amount * subexc.input.rp_exchange().amount})
            prospective_scores[subexc.input["name"]] = lca.score
    else:
        lca.lcia(demand={exc.input.id: exc.amount})
        prospective_scores[exc.input["name"]] = lca.score
```

    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/bw2calc/lca_base.py:127: SparseEfficiencyWarning: splu converted its input to CSC format
      self.solver = factorized(self.technosphere_matrix)
    /Users/timodiepers/anaconda3/envs/timex/lib/python3.10/site-packages/scikits/umfpack/umfpack.py:736: UmfpackWarning: (almost) singular matrix! (estimated cond. number: 2.48e+12)
      warnings.warn(msg, UmfpackWarning)


Lets compare the overall scores:


```python { .notebook-cell }
print("Static score: ", sum(static_scores.values())) # should be the same as tlca.base_lca.score
print("Prospective score: ", sum(prospective_scores.values()))
print("Time-explicit score: ", tlca.dynamic_score)
```

    Static score:  20858.470012031674
    Prospective score:  6522.389036408176
    Time-explicit score:  11653.498778351013


To better understand what's going on, let's plot the scores as a waterfall chart  based on timing of emission. Also, we can look at the "first-level contributions":


```python { .notebook-cell }
from bw_timex.utils import plot_characterized_inventory_as_waterfall

order_stacked_activities = (
    [ 
        glider_production_without_eol["name"],
        battery_production_without_eol["name"],
        powertrain_production_without_eol["name"],
        electricity_production["name"],
        glider_eol["name"],
        battery_eol["name"],
        powertrain_eol["name"],
    ]
)

plot_characterized_inventory_as_waterfall(
    tlca,
    static_scores=static_scores,
    prospective_scores=prospective_scores,
    order_stacked_activities=order_stacked_activities,
)
```


    
![png](example_electric_vehicle_premise_files/output_89_0.png)
    


One can see that the time-explicit results (in the middle) are somewhere in between the static and the prospective results. This makes sense as at each timestep, the underlying processes are sourced from progressively "cleaner" background databases, reaching a lower impact than if they are only sourced from the current database, but not so low as the prospective results, which are fully sourced from the most decarbonized database. Notably, the electricity consumption in the use-phase, modelled uniformly over the lifetime of the EV, contributes less and less to the score in the later years, since the electricity becomes cleaner in the future databases.
