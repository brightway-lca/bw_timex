---
name: bw-timex-analysis
description: Use when writing, debugging, or reviewing time-explicit LCA analyses with bw_timex — setting up temporal distributions (TDs/rTDs) on exchanges, prospective/time-specific background databases, building a TimexLCA timeline, and running static or dynamic LCIA. Symptoms this applies to - "temporal distribution", "TimexLCA", "database_dates", "dynamic_lcia", "build_timeline", premise-based prospective LCA, or any Brightway LCA where process timing/vintage matters.
---

# bw_timex Analysis

## Overview

`bw_timex` runs Brightway LCAs where each process/emission gets data from the
point in time it actually occurs, instead of one static snapshot. Core object:
`bw_timex.TimexLCA`. Two ingredients on top of a normal Brightway model:

1. **Temporal distributions (TDs)** on exchanges — *when* an exchange happens
   relative to its consuming process.
2. **Time-specific databases** — background (and optionally foreground) data
   for different points in time, mapped via `database_dates`.

Workflow: write system → add TDs to exchanges → `TimexLCA(...)` →
`build_timeline()` → `lci()` → `static_lcia()` / `dynamic_lcia()`.

## System setup

Write the Brightway model as usual (`bd.Database(name).write({...})`), but:

- **Every background vintage is its own database** (e.g. `background_2020`,
  `background_2030`), each internally consistent and dated.
- The **foreground database is `"dynamic"`** — no single date, timing is
  derived from the graph traversal.
- After writing any database, **`db.process()` it** (or loop
  `for db in bd.databases: bd.Database(db).process()`) — TimexLCA reads
  processed data.
- Map every database used by the demand to a date:

```python
from datetime import datetime
database_dates = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
```

**Cross-vintage matching is by `(name, reference product, location)`, not by
code or database.** Foreground exchanges only need to link into *one* vintage
database (e.g. `background_2020`); at `build_timeline()` time, bw_timex finds
the matching activity in every *other* static vintage database by that
triplet and interpolates/relinks based on timing. So every vintage's version
of "the same" process must share identical `name`, `"reference product"`,
and `location` values (the `code` can differ). Minimal activity dict:

```python
("background_2020", "B"): {
    "name": "B",
    "reference product": "B",   # required for cross-vintage matching
    "location": "somewhere",
    "exchanges": [
        {"amount": 1, "type": "production", "input": ("background_2020", "B")},
        {"amount": 11, "type": "biosphere", "input": ("biosphere", "CO2")},
    ],
},
```

**Prospective backgrounds from `premise`:** generate one Brightway database
per vintage year with `premise.NewDatabase(...).write_brightway25_database(...)`
(or equivalent), then use each resulting database name as a key in
`database_dates` with its representative `datetime`. Everything else is
identical to a hand-written multi-vintage setup above.

## Temporal distributions (TDs)

TDs live on **exchanges**, not activities. `date` is usually a `timedelta64`
array **relative to the consuming process** (negative = earlier, positive =
later) — but it can also hold `datetime64` values for **absolute** dates
instead, if you know exactly when an exchange happens regardless of the
consumer's timing. `amount` are shares that should sum to 1 unless you're
intentionally scaling.

```python
import numpy as np
from bw_temporalis import TemporalDistribution, easy_timedelta_distribution

# explicit shares: 30% two years before, 50% at t0, 20% four years after
td = TemporalDistribution(
    date=np.array([-2, 0, 4], dtype="timedelta64[Y]"),
    amount=np.array([0.3, 0.5, 0.2]),
)

# convenience for uniform/normal/triangular spreads, e.g. a use phase
td_use = easy_timedelta_distribution(
    start=0, end=15, resolution="Y", steps=16, kind="uniform",
)
```

Attach to an exchange by identifying producer + consumer (only enough fields
to uniquely resolve the exchange are required):

```python
from bw_timex.utils import add_temporal_distribution_to_exchange

add_temporal_distribution_to_exchange(
    temporal_distribution=td,
    input_code="B", input_database="background_2020",
    output_code="A", output_database="foreground",
)
```

**Foreground amount changing over time** (e.g. efficiency gains) is a
separate concept from *when* — use `add_temporal_evolution_to_exchange`:

```python
from bw_timex.utils import add_temporal_evolution_to_exchange

add_temporal_evolution_to_exchange(
    temporal_evolution_factors={   # fraction of the base exchange amount
        datetime(2020, 1, 1): 1.0,
        datetime(2030, 1, 1): 0.75,
    },
    # or temporal_evolution_amounts={...} for absolute values
    input_code="B", input_database="background_2020",
    output_code="A", output_database="foreground",
)
```

Drafting TDs interactively in a notebook: `bw_timex.utils.interactive_td_widget()`.

## Running a TimexLCA

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},   # or a product node
    method=("our", "method"),
    database_dates=database_dates,
)
tlca.build_timeline(starting_datetime="2024-01-01")   # default "now"
tlca.lci()
tlca.static_lcia()
tlca.static_score
```

`demand` activities' database **must** be marked `"dynamic"` in
`database_dates` — validated at construction, raises otherwise.

### `build_timeline()` — key params

| param | default | notes |
|---|---|---|
| `starting_datetime` | `"now"` | when the demand occurs; ISO string or `datetime` |
| `temporal_grouping` | `"year"` | `"month"`/`"day"`/`"hour"` for finer resolution |
| `interpolation_type` | `"linear"` | or `"nearest"`/`"closest"` between vintage databases |
| `cutoff` / `max_calc` | `1e-9` / `2000` | graph-traversal cutoffs |
| `graph_traversal` | `"priority"` | or `"bfs"` (no per-subgraph LCA overhead, needed for `traverse_background`) |
| `traverse_background` | `False` | if `True`, TDs *inside* background databases are honored too, not just at the foreground/background frontier |
| `edge_filter_function` | skip all background edges | pass a `Callable(node_id) -> bool` to customize which edges the traversal descends into |

Calling `build_timeline()` again with the same args reuses the cached
timeline; different args rebuild it.

### `lci()` — key params

- `build_dynamic_biosphere=True` (default): keeps per-emission timing, needed
  for `dynamic_lcia()`. Set `False` if you only need `static_score` — faster.
- `expand_technosphere=True` (default): builds an expanded time-explicit
  technosphere/biosphere via datapackages — required for `static_lcia()` and
  for background contribution analysis (`disaggregate_background_lci()`).
  `False` computes the dynamic inventory straight from the timeline (faster,
  no contribution analysis).

### Impact assessment

```python
tlca.static_lcia()      # requires expand_technosphere=True
tlca.static_score

from dynamic_characterization.ipcc_ar6 import characterize_co2
emission_id = bd.get_activity(("biosphere", "CO2")).id
tlca.dynamic_lcia(
    metric="radiative_forcing",       # or "GWP"
    time_horizon=100,
    characterization_functions={emission_id: characterize_co2},  # optional; IPCC AR6 defaults used otherwise
)
tlca.dynamic_score
```

- `fixed_time_horizon=True` + `time_horizon_start` gives the Levasseur
  approach (all emissions share one horizon from a fixed date); default
  (`False`) computes each emission's horizon from its own emission date —
  standard LCA convention.
- Three scores exist on the same object: `base_score` (plain static LCA,
  no time-explicit relinking), `static_score` (time-explicit LCI, static
  characterization), `dynamic_score` (time-explicit LCI + dynamic
  characterization). Compare all three to see how much timing matters.

### Contribution analysis on the background

```python
tlca.disaggregate_background_lci()   # needs expand_technosphere=True
tlca.dynamic_lcia(..., use_disaggregated_lci=True)
```
Breaks temporal-market emissions back down to their original background
producers instead of one aggregated "temporal market" node.

### Plotting / inspection

- `tlca.plot_dynamic_inventory(bio_flows=[...], cumulative=False)`
- `tlca.plot_dynamic_characterized_inventory(cumsum=, sum_emissions_within_activity=, sum_activities=)`
- `bw_timex.utils.plot_characterized_inventory_as_waterfall(tlca)`
- `tlca.timeline` — DataFrame of producer/consumer/date/amount per traversed edge
- `tlca.create_labelled_dynamic_inventory_dataframe()` — human-readable dynamic inventory
- `tlca.create_labelled_technosphere_dataframe()` / `create_labelled_biosphere_dataframe()`

## Common mistakes

| Symptom | Cause / fix |
|---|---|
| `ValueError: Demand activity ... not marked 'dynamic'` | The demand's database is missing from `database_dates`, or listed with a `datetime` instead of `"dynamic"`. |
| TD seems to shift the wrong process | `date` in a `TemporalDistribution` is (by default) relative to the **consumer**, not the producer. Negative = before the consuming process. Use `datetime64` values instead of `timedelta64` if you want absolute, consumer-independent dates. |
| Timeline / exchanges look unchanged after writing TDs | Forgot to `db.process()` after writing/modifying the database. |
| `AttributeError: Timeline not yet built` | Call `build_timeline()` before `lci()`; call `lci()` before `static_lcia()`/`dynamic_lcia()`. |
| `static_lcia` raises "expanded matrix" error | It requires `lci(expand_technosphere=True)` (the default) — you likely called `lci(expand_technosphere=False)`. |
| `Method ... not found` | `method` must already exist in `bw2data.methods` before constructing `TimexLCA`. |
| Background TDs silently ignored | Default `edge_filter_function` skips all background-database edges. Pass `traverse_background=True` (with `graph_traversal="bfs"` for large systems) if background timing matters. |
| Slow repeated runs in a notebook loop | Keep `use_global_lci_cache=True` (default) and reuse the same Python session — background unit LCIs are cached at module level across `TimexLCA` objects. |
| No interpolation between vintages happens | The matching activity in the other vintage database has a different `name`, `"reference product"`, or `location` than the one the exchange links to — cross-vintage matching keys on that triplet, not on `code`. |

## Reference

- Getting Started notebook: `notebooks/getting_started.ipynb` in the bw_timex
  repo — minimal end-to-end worked example this skill is distilled from.
- Full docs: https://docs.brightway.dev/projects/bw-timex/en/latest/
