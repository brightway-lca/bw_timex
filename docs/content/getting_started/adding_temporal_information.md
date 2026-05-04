# Step 1 - Adding temporal information

To get you started with time-explicit LCA, we'll investigate this very simple production system with two "technosphere" nodes A and B and a "biosphere" node representing some CO<sub>2</sub> emissions. For the sake of this example, we'll assume that we demand Process A to run exactly once.
```{mermaid}
:caption: Example production system
flowchart LR
subgraph background[<i>background</i>]
    B(Process B):::bg
end

subgraph foreground[<i>foreground</i>]
    A(Process A):::fg
end

subgraph biosphere[<i>biosphere</i>]
    CO2(CO<sub>2</sub>):::bio
end

B-->|"3 kg \n &nbsp;"|A
A-.->|"5 kg \n  &nbsp;"|CO2
B-.->|"11 kg \n &nbsp;"|CO2

classDef fg color:#222832, fill:#3fb1c5, stroke:none;
classDef bg color:#222832, fill:#3fb1c5, stroke:none;
classDef bio color:#222832, fill:#9c5ffd, stroke:none;
style background fill:none, stroke:none;
style foreground fill:none, stroke:none;
style biosphere fill:none, stroke:none;
```

:::{dropdown} <span style="font-weight: normal; font-style: italic;">Here's the code to set this up with brightway - but this is not essential here</style>
:icon: codescan

```python
import bw2data as bd

bd.projects.set_current("getting_started_with_timex")

bd.Database("biosphere").write(
    {
        ("biosphere", "CO2"): {
            "type": "emission",
            "name": "CO2",
        },
    }
)

bd.Database("background").write(
    {
        ("background", "B"): {
            "name": "B",
            "location": "somewhere",
            "reference product": "B",
            "exchanges": [
                {
                    "amount": 1,
                    "type": "production",
                    "input": ("background", "B"),
                },
                {
                    "amount": 11,
                    "type": "biosphere",
                    "input": ("biosphere", "CO2"),
                },
            ],
        },
    }
)

bd.Database("foreground").write(
    {
        ("foreground", "A"): {
            "name": "A",
            "location": "somewhere",
            "reference product": "A",
            "exchanges": [
                {
                    "amount": 1,
                    "type": "production",
                    "input": ("foreground", "A"),
                },
                {
                    "amount": 3,
                    "type": "technosphere",
                    "input": ("background", "B"),
                },
                {
                    "amount": 5,
                    "type": "biosphere",
                    "input": ("biosphere", "CO2"),
                }
            ],
        },
    }
)

bd.Method(("our", "method")).write(
    [
        (("biosphere", "CO2"), 1),
    ]
)
```
:::

Now, if you want to consider time in your LCA, you need to somehow add temporal information. For time-explicit LCA, we consider two kinds of temporal information, that will be discussed in the following.

:::{note}
Brightway can represent inventory data either with separate process and product nodes or with
chimaera process+product nodes. See the Brightway inventory overview on
[processes, products, and something in between](https://docs.brightway.dev/en/latest/content/overview/inventory.html#processes-products-and-something-in-between).

This getting-started page uses the common chimaera style, where Process A is also its reference
product. The temporal concepts below also apply to explicit process/product models; the main
difference is where output-side timing can be attached. If you want to represent several
production-time groups of the same product, you can do this in either paradigm. In a chimaera
model, this timing is often represented with an intermediary foreground edge. In an explicit
model, it can live directly on the process→product production edge.
:::

## Temporal distributions
To determine the timing of the exchanges within the production system, we add the `temporal_distribution` attribute to the respective exchanges. To carry the temporal information, we use the [`TemporalDistribution`](https://docs.brightway.dev/projects/bw-temporalis/en/stable/content/api/bw_temporalis/temporal_distribution/index.html#bw_temporalis.temporal_distribution.TemporalDistribution) class from [`bw_temporalis`](https://github.com/brightway-lca/bw_temporalis). This class is a *container for a series of amount spread over time*, so it tells you what share of an exchange happens at what point in time. So, let's include this information in our production system - first visually:
```{mermaid}
:caption: Temporalized example production system
flowchart LR
subgraph background[" "]
    B_2020(Process B):::bg
end

subgraph foreground[" "]
    A(Process A):::fg
end

subgraph biosphere[" "]
    CO2(CO<sub>2</sub>):::b
end

    B_2020-->|"dates:[-2,0,+4] years \n shares: [30%,50%,20%] * 3 kg "|A
    A-.->|"dates: [0,+1] years\n  shares: [60%,40%] * 5 kg"|CO2
    B_2020-.->|"dates:[0] years\n  shares: [100%] * 11 kg"|CO2

    classDef bg color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#3fb1c5, stroke:none;
    classDef b color:#222832, fill:#9c5ffd, stroke:none;
    style foreground fill:none, stroke:none;
    style background fill:none, stroke:none;
    style biosphere fill:none, stroke:none;

```
:::{dropdown} <span style="font-weight: normal; font-style: italic;">Here's the code to add this information to our modeled production system in Brightway</style>
:icon: codescan

```python
import numpy as np
from bw_temporalis import TemporalDistribution
from bw_timex.utils import add_temporal_distribution_to_exchange

# Starting with the exchange between A and B
# First, create a TemporalDistribution with the time information from above
td_b_to_a = TemporalDistribution(
    date=np.array([-2, 0, 4], dtype="timedelta64[Y]"),
    amount=np.array([0.3, 0.5, 0.2]),
)

# Now add the temporal distribution to the corresponding exchange. In
# principle, you just have to do the following:
# exchange_object["temporal_distribution"] = TemporalDistribution
# We currently don't have the exchange_object at hand here, but we can
# use the utility function add_temporal_distribution_to_exchange to help.
add_temporal_distribution_to_exchange(
    temporal_distribution=td_b_to_a,
    input_code="B",
    input_database="background",
    output_code="A",
    output_database="foreground"
)

# Now we do the same for our other temporalized exchange between A and CO2
td_a_to_co2 = TemporalDistribution(
    date=np.array([0, 1], dtype="timedelta64[Y]"),
    amount=np.array([0.6, 0.4]),
)

# We actually only have to define enough fields to uniquely identify the
# exchange here
add_temporal_distribution_to_exchange(
    temporal_distribution=td_a_to_co2,
    input_code="CO2",
    output_code="A"
)
```
:::

## Time-specific process data

While the temporal information above tells us when the processes occur, we also need information on how our processes change over time. So, for our simple example, let's say our background process B somehow evolves, so that it emits less CO<sub>2</sub> in the future. To make it precise, we assume that the original process we modeled above represents the process state in the year 2020, emitting 11 kg CO<sub>2</sub>, which reduces to 7 kg CO<sub>2</sub> by 2030:


```{mermaid}
:caption: Temporalized example production system with two time-specific background processes B
flowchart LR
subgraph background[" "]
    B_2020(Process B \n 2020):::bg
    B_2030(Process B \n 2030):::bg
end

subgraph foreground[" "]
    A(Process A):::fg
end

subgraph biosphere[" "]
    CO2(CO<sub>2</sub>):::b
end
    B_2020-->|"dates:[-2,0,+4] years \n shares: [30%,50%,20%] * 3 kg"|A
    A-.->|"dates: [0,+1] years\n  shares: [60%,40%] * 5 kg"|CO2   
    B_2020-.->|"dates:[0] years\n shares: [100%] * <span style='color:#9c5ffd'><b>11 kg</b></span>"|CO2
    B_2030-.->|"dates:[0] years\n shares: [100%] * <span style='color:#9c5ffd'><b>7 kg</b></span>"|CO2

    classDef bg color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#3fb1c5, stroke:none;
    classDef b color:#222832, fill:#9c5ffd, stroke:none;
    style foreground fill:none, stroke:none;
    style background fill:none, stroke:none;
    style biosphere fill:none, stroke:none;

```

:::{dropdown} <span style="font-weight: normal; font-style: italic;">Again, here's the code in case you're interested</span>
:icon: codescan

```python
bd.Database("background_2030").write(
    {
        ("background_2030", "B"): {
            "name": "B",
            "location": "somewhere",
            "reference product": "B",
            "exchanges": [
                {
                    "amount": 1,
                    "type": "production",
                    "input": ("background_2030", "B"),
                },
                {
                    "amount": 7,
                    "type": "biosphere",
                    "input": ("biosphere", "CO2"),
                },
            ],
        },
    }
)
```
:::

So, as you can see, the processes at specific time steps reside within a separate normal Brightway database. To hand them to `bw_timex`, we just need to define a dictionary that maps the names of time-specific databases to the point in time that they represent:

```python
from datetime import datetime

# Note: The foreground does not represent a specific point in time, but should
# later be dynamically distributed over time
database_dates = {
    "background": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
```

:::{note}
You can use whatever data source you want for the time-specific process data. A nice package from the Brightway cosmos that can help you is [premise](https://premise.readthedocs.io/en/latest/introduction.html).
:::

## Temporal evolution of foreground exchanges (`bw_timex>0.3.4`)

The approaches above handle temporal variation in the *background* system — different database snapshots for different points in time. But what if a *foreground* exchange itself changes over time? For example, an industrial process might become more energy-efficient over the years, so its electricity consumption per unit of output decreases.

`bw_timex` supports this via **temporal evolution** attributes on exchanges. These are optional — if you don't add them, exchange amounts remain constant over time as before.

There are two ways to specify temporal evolution:

**Scaling factors** — multiply the base exchange amount by a time-dependent factor:

```python
from datetime import datetime

exchange["temporal_evolution_factors"] = {
    datetime(2020, 1, 1): 1.0,   # 100% of base amount in 2020
    datetime(2030, 1, 1): 0.75,  # 75% of base amount in 2030
    datetime(2040, 1, 1): 0.6,   # 60% of base amount in 2040
}
```

**Absolute amounts** — directly specify the exchange amount at each point in time:

```python
exchange["temporal_evolution_amounts"] = {
    datetime(2020, 1, 1): 60,   # 60 MJ in 2020
    datetime(2030, 1, 1): 45,   # 45 MJ in 2030
    datetime(2040, 1, 1): 36,   # 36 MJ in 2040
}
```

For dates between the specified points, values are linearly interpolated. For dates outside the range, the nearest boundary value is used. You can specify either `temporal_evolution_factors` or `temporal_evolution_amounts` for the same exchange, but not both.

This mechanism can represent **production-version-specific efficiency** if the exchange is
evaluated at the timestamp of the process/product version. Here, "version" means the production or
design date that fixes a foreground exchange amount. This fixed production/design date is often
called a **vintage**. For example, with factors `{2025: 1.0, 2030: 1.1}`, a unit produced in
2025 uses the 2025 vintage factor and a unit produced in 2030 uses the 2030 vintage factor (with
interpolation in-between). If a single foreground exchange represents a mixed fleet over multiple
production years, that one exchange still has just one amount at each event time; to model distinct
production-time groups explicitly, create separate exchanges or processes for each group (e.g.,
EV_2025, EV_2030) and assign their temporal distributions accordingly.

### Choosing `temporal_evolution_reference`

Temporal evolution factors need a timestamp. In a time-explicit foreground exchange, `bw_timex` can carry two relevant timestamps:

- `date_consumer`: when the consuming foreground process instance exists. If the model splits a
  product into production-time groups, read this as the **process/product version date** of the
  process using the exchange. In fleet and stock models, this is usually the **vintage**.
- `date_producer`: when the exchanged input/output event actually happens. Read this as the **calendar event date** of the exchange.

Use `temporal_evolution_reference="consumer"` when the exchange amount is a property of the
consuming process or product version. The factor is locked to `date_consumer`.

Examples:

- A vehicle built in 2025 keeps its 2025 electricity consumption per km when it drives in 2035.
- A building built in 2020 keeps its 2020 insulation standard during later operation.
- A 2030 production line needs less material because of its design, and keeps that efficiency for all later maintenance or use-phase exchanges.

Use `temporal_evolution_reference="producer"` when the exchange amount is a property of the calendar year in which the exchange happens. The factor follows `date_producer`.

Examples:

- A maintenance operation uses less solvent in 2035 because maintenance practice improved by then, regardless of when the serviced asset was built.
- A foreground repair process becomes more efficient over calendar time.
- A foreground input is reduced by a retrofit or operational learning that applies to all active product/process versions in that year.

Rule of thumb:

```text
Is the change a property of the foreground process/product version date?
-> use consumer

Is the change a property of the calendar year when the exchange happens?
-> use producer
```

This choice is independent of whether you model with chimaera nodes or explicit process/product
nodes. Explicit process/product models can make the distinction easier to see because a product can
have a production-edge temporal distribution, creating clear product/process version dates. Chimaera
models can represent the same idea by adding a foreground intermediary activity whose exchange
creates those version dates.

A convenience function is available to add temporal evolution to an existing exchange:

```python
from bw_timex.utils import add_temporal_evolution_to_exchange

add_temporal_evolution_to_exchange(
    temporal_evolution_factors={
        datetime(2020, 1, 1): 1.0,
        datetime(2030, 1, 1): 0.75,
    },
    temporal_evolution_reference="consumer",
    input_code="B",
    input_database="background",
    output_code="A",
    output_database="foreground",
)
```

:::{note}
Temporal evolution only applies to foreground exchanges. Background process evolution is handled by the database interpolation mechanism described above.
:::
