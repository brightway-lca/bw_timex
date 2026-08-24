---
icon: lucide/zap
tags:
  - tutorial
---

# Quick Start

Condensed reference for `bw_timex`. For a step-by-step introduction, see the [Walkthrough](index.md). For the underlying framework, see the [Theory](../theory.md) page. For more, see the [Examples](../examples/index.md) or the [API Reference](../../api/index.md).

---

## Install `bw_timex`

```bash
pip install bw_timex
```

For other installation methods (uv, conda) and platform-specific notes, see the [Installation guide](../installation.md).

---

## Minimal Working Example

```
Example Project
├── biosphere           # elementary flows
├── foreground          # process A
├── background          # process B, as of 2020
└── background_2030     # process B, as of 2030
```

```python
from datetime import datetime
import bw2data as bd
import numpy as np
from bw_timex import (
    TemporalDistribution,
    TimexLCA,
    add_temporal_distribution_to_exchange,
    set_database_metadata,
)

# 1. Set up Brightway project
bd.projects.set_current("my_project")

# 2. Add temporal information to exchange A -> B
add_temporal_distribution_to_exchange(
    temporal_distribution=TemporalDistribution(
        date=np.array([-2, 0, 4], dtype="timedelta64[Y]"),
        amount=np.array([0.3, 0.5, 0.2]),
    ),
    input_code="B",
    input_database="background",
    output_code="A",
    output_database="foreground",
)

# 3. Say what your time-specific background databases represent
#    (databases from premise >= 2.4.9.2 already know - skip this for them)
set_database_metadata("background", representative_time=datetime(2020, 1, 1))
set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))

# 4. Create the TimexLCA object
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
)

# 5. Build the process timeline
tlca.build_timeline(starting_datetime="2024-01-01")

# 6. Calculate the time-explicit inventory
tlca.lci()

# 7. Characterize: static and/or dynamic
tlca.static_lcia()
print(tlca.static_score)

tlca.dynamic_lcia(metric="radiative_forcing", time_horizon=100)
print(tlca.dynamic_score)

# 8. Analyze results
tlca.plot_dynamic_characterized_inventory()
```

As an alternative to running `.build_timeline()`, `.lci()`and `.static_lcia()`/`.dynamic_lcia()`, you can also do:

```python
from bw_timex import TimexLCA, TimexLCASettings

settings = TimexLCASettings(
    demand={("foreground", "A"): 1}, 
    method=("our", "method"), 
    time_horizon=100,
    )
tlca = TimexLCA.(settings).run()
print(tlca.dynamic_score)

tlca.run(time_horizon=20) # re-run, but with different time horizon
print(tlca.dynamic_score)

```

For comparing different settings conveniently, you can simply run:

```python
comparison = tlca.compare(settings, other_settings)
print(comparison.summary)
```



---

## Temporal Information Cheat Sheet

| What you want to express | How | Where |
|---|---|---|
| *When* an exchange happens | `temporal_distribution` on the exchange | any foreground exchange (and background ones, if you traverse the background) |
| *How the background changes* over time | one database per point in time, each with `representative_time` metadata | background databases |
| *How a foreground exchange changes* over time | `temporal_evolution_factors` / `temporal_evolution_amounts` on the exchange (`bw_timex>0.3.4`) | foreground exchanges |

```python
from bw_timex.utils import (
    add_temporal_distribution_to_exchange,
    add_temporal_evolution_to_exchange,
)

# Timing: 30% two years earlier, 50% now, 20% four years later
add_temporal_distribution_to_exchange(
    temporal_distribution=TemporalDistribution(
        date=np.array([-2, 0, 4], dtype="timedelta64[Y]"),
        amount=np.array([0.3, 0.5, 0.2]),
    ),
    input_code="B",
    output_code="A",
)

# Evolution: the exchange needs less input in the future
add_temporal_evolution_to_exchange(
    temporal_evolution_factors={
        datetime(2020, 1, 1): 1.0,
        datetime(2030, 1, 1): 0.75,
        datetime(2040, 1, 1): 0.6,
    },
    temporal_evolution_reference="consumer",  # or "producer"
    input_code="B",
    output_code="A",
)
```

Absolute dates (`dtype="datetime64[s]"`) are also allowed in a `TemporalDistribution`,
e.g. for the timing of the functional unit itself. Relative dates
(`dtype="timedelta64[Y]"`) are relative to the consuming process. Several databases may
represent the same point in time, e.g. if you keep modified copies of background
processes in their own database instead of writing them into the shared vintage. See
[Step 1](adding_temporal_information.md) for the database metadata, and
[Step 2](build_process_timeline.md) for scenario selection and for mapping databases
explicitly with `database_dates`.

---

## TimexLCA Quick Reference

### Creating the object

```python
TimexLCA(
    demand={("foreground", "A"): 1},  # Node, (database, code) tuple, or int id
    method=("our", "method"),
    database_dates=None,              # fallback: map the databases yourself, overriding metadata
    scenario=None,                    # pick one scenario, when the project holds several
)
```

Both are optional. `scenario` narrows down what `bw_timex` reads from the database
metadata (written by premise >= 2.4.9.2, or by you with `set_database_metadata`);
`database_dates` is the fallback for when you'd rather write the mapping out yourself,
and it overrides the metadata entirely.

If the project does not hold the scenario's databases yet, `bw_timex` can build them
with premise instead of raising. Add the years to the scenario and pass
`create_missing=True`, alongside the premise key and ecoinvent credentials:

```python
tlca = TimexLCA(
    demand={("foreground", "ev"): 1},
    method=("EF v3.1", "climate change", "global warming potential (GWP100)"),
    scenario={
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.10.1",
        "years": [2020, 2030, 2040],
    },
    create_missing=True,
    premise_key="dummy_premise_decryption_key",              # or $PREMISE_KEY
    ecoinvent_credentials=("dummy_user", "dummy_password"),  # or $ECOINVENT_USERNAME / _PASSWORD
)
```

### `build_timeline()`

| Argument | Default | Description |
|---|---|---|
| `starting_datetime` | `"now"` | When the demand occurs, e.g. `"2024-01-01"` |
| `temporal_grouping` | `"year"` | Time resolution: `"year"`, `"month"`, `"day"`, `"hour"` |
| `interpolation_type` | `"linear"` | Sourcing between background vintages: `"linear"` or `"nearest"` |
| `edge_filter_function` | `None` | Skip edges during graph traversal (default: skip background-internal edges) |
| `cutoff` | `1e-9` | Traversal cutoff |
| `max_calc` | `2000` | Max number of traversal calculations |
| `traverse_background` | `False` | Also follow temporal distributions inside background databases |

Result is stored in `tlca.timeline` (a DataFrame with `date_producer`, `producer_name`,
`date_consumer`, `consumer_name`, `amount`, `temporal_market_shares`).

### `lci()`

| Argument | Default | Description |
|---|---|---|
| `build_dynamic_biosphere` | `True` | Keep the timing of emissions. Set `False` for scores only (faster, less memory) |
| `expand_technosphere` | `True` | Build expanded time-explicit matrices. `False` builds the dynamic inventory directly from the timeline |

### `dynamic_lcia()`

| Argument | Default | Description |
|---|---|---|
| `metric` | `"radiative_forcing"` | `"radiative_forcing"` or `"GWP"` |
| `time_horizon` | `100` | Time horizon in years |
| `fixed_time_horizon` | `False` | `True` = Levasseur approach (horizon from the functional unit), `False` = conventional (horizon from each emission) |
| `time_horizon_start` | `None` | Start of the fixed time horizon, defaults to now |
| `characterization_functions` | `None` | `{biosphere_flow_id: function}`. Not needed for ecoinvent / `biosphere3`, where flows are mapped automatically |

### `run()` and `compare()`

See [Repeated Runs & Scenario Comparison](configured_runs.md).

| Call | Description |
|---|---|
| `TimexLCA(settings)` | Build the object from a `TimexLCASettings` |
| `tlca.run()` | The four steps above, in order, with that object's settings |
| `tlca.run(**overrides)` | The same, with settings overridden for this call only. Refuses a changed `scenario` / `database_dates` |
| `TimexLCA.compare([settings, ...])` | Run several, into `ComparisonResult.summary` |

`TimexLCASettings` takes every argument listed above - `demand`, `method`,
`database_dates`, `scenario`, plus all of `build_timeline()`, `lci()` and
`dynamic_lcia()` - and a `label` naming its row in a comparison. Enable or skip
the LCIA steps with `static_lcia_enabled` / `dynamic_lcia_enabled`.

---

## Results

| Attribute / Method | Returns | Description |
|---|---|---|
| `tlca.timeline` | DataFrame | Exchanges with their timing and background shares |
| `tlca.base_score` | float | Score of the original, non-time-explicit LCA |
| `tlca.static_score` | float | Time-explicit inventory, static characterization |
| `tlca.dynamic_score` | float | Time-explicit inventory, dynamic characterization |
| `tlca.dynamic_inventory_df` | DataFrame | Emissions with date, amount, flow, activity |
| `tlca.characterized_inventory` | DataFrame | Characterized emissions over time |
| `tlca.plot_dynamic_inventory(bio_flows)` | Figure | Emissions over time |
| `tlca.plot_dynamic_characterized_inventory()` | Figure | Impacts over time |
| `plot_characterized_inventory_as_waterfall(tlca)` | Figure | Waterfall of contributions (from `bw_timex.utils`, GWP only) |