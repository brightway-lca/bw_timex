---
icon: lucide/git-compare
tags:
  - scenario
  - background databases
---

# Repeated Runs & Scenario Comparison

The [Walkthrough](index.md) calls the four steps one at a time, which is what you
want while building a model: you look at `tlca.timeline` before calculating the
inventory, and re-run a single step with different arguments.

Once the model stands, you usually want the opposite - to run *the same
calculation* many times over: different purchase years, different time horizons,
different IAM scenarios. This page covers the two pieces for that. Neither
replaces the four steps; `run()` calls them for you, in the same order.

---

## One calculation as one object

`TimexLCASettings` holds everything a calculation needs - the demand, the method,
the background selection, and every timeline, LCI and LCIA option:

```python
from datetime import datetime
from bw_timex import TimexLCA, TimexLCASettings

settings = TimexLCASettings(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    starting_datetime=datetime(2024, 1, 1),
    metric="radiative_forcing",
    label="baseline",
)

tlca = TimexLCA.from_settings(settings).run()
print(tlca.static_score, tlca.dynamic_score)
```

`run()` executes `build_timeline()`, `lci()`, `static_lcia()`, and - unless
`dynamic_lcia_enabled=False` - `dynamic_lcia()`. Every argument of those four
methods is a field on the settings, so one object is also the record of what was
run: keep it, log it, or put a list of them into `compare()`.

---

## Re-running with different settings

Call `run()` again on the same object. Individual overrides apply to that call
only, leaving both the settings object and the object's own settings untouched:

```python
tlca.run(time_horizon=20)                       # one knob
tlca.run(starting_datetime=datetime(2030, 1, 1))
tlca.run(demand={("foreground", "B"): 1})       # a different demand
```

What that saves depends on what changed:

| Changed | What happens |
|---|---|
| a timeline, LCI or LCIA option | the base LCA, node proxies and background caches are reused; the timeline too, if its own parameters are unchanged |
| `demand` or `method` | the base LCA is recalculated, everything keyed on the background is still reused |
| `database_dates`, `scenario` | refused - see below |

The background databases fix the columns of the time-explicit matrices and the
caches keyed on them, so they cannot change between runs of one object.
`run()` raises a `ValueError` naming the field rather than returning a quietly
wrong number. That is what `compare()` is for.

!!! tip "Passing a whole settings object"

    `run()` also takes a replacement: `tlca.run(other_settings)`. Build it with
    `dataclasses.replace(settings, ...)` so the background fields carry over
    unchanged - if they differ from what the object was built with, `run()`
    raises.

---

## Comparing scenarios

`compare()` takes a list of settings and returns a `ComparisonResult`:

```python
from dataclasses import replace

comparison = TimexLCA.compare(
    [
        replace(settings, scenario={"pathway": "SSP2-Base"}, label="Base"),
        replace(settings, scenario={"pathway": "SSP2-PkBudg500"}, label="PkBudg500"),
    ]
)

comparison.summary  # one row per calculation
```

`summary` is a DataFrame carrying the scores next to every setting that produced
them, plus `scenario_*` columns, the timeline size and the runtime - so the table
is its own record of the comparison, and plots directly:

```python
comparison.summary.plot.bar(x="label", y="static_score")
```

Each distinct background gets its own `TimexLCA`, and every calculation sharing
that background runs on it - so a scenario × demand grid only pays for a new
object when the background actually changes.

| Argument | Default | Description |
|---|---|---|
| `keep_objects` | `False` | Keep each `TimexLCA` in `ComparisonResult.objects`, to dig into one result's timeline or inventory afterwards. Off by default because a large comparison holds a lot of memory this way |
| `on_error` | `"raise"` | `"record"` puts the failure in the row's `error` column and carries on, instead of aborting a long unattended sweep |

---

## Where the scenario databases come from

`scenario={...}` selects among the background databases the project holds, by
the metadata they carry (see [Step 1](adding_temporal_information.md) for
`representative_time`, and [Step 2](build_process_timeline.md) for the filter
itself).

If the project doesn't hold them yet, `bw_timex` can build them with premise -
see [`create_missing`](quickstart.md#creating-the-object) in the Quick Start. Do
that once, up front; afterwards the databases are there and the comparison above
runs against them unchanged:

```python
TimexLCA(demand, method, scenario={..., "years": [2020, 2030, 2040]}, create_missing=True)
```

---

For a worked example on real ecoinvent + premise data, see the
[Scenario Comparison notebook](../examples/advanced/scenario_comparison.md).
