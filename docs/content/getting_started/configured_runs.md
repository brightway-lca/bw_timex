---
icon: lucide/git-compare
tags:
  - configured runs
  - comparison
  - scenario
---

# Configured Runs & Scenario Comparisons

The main functions to call to run a `TimexLCA` are `build_timeline()`, `lci()`, `static_lcia()` and `dynamic_lcia()`, see Walkthrough Steps 2-4. To save you from typing this repeatedly, and capture all configurations in one place, we provide the `TimexLCA.run()` function.

## One calculation as one object

To specify the configuration of the run, we use the `TimexLCASettings` class:

```python
settings = TimexLCASettings(
    demand=demand,
    method=method,
    scenario={
        "iam_model": "remind",
        "pathway": "SSP2-NDC",
    },
    timeline={
        "starting_datetime": datetime(2020, 6, 1),
        "graph_traversal": "bfs", # breadth first search
        # other settings from .build_timeline()
    },
    lci={
        "build_dynamic_biosphere": False,
        # other settings from .lci()
    },
    lcia={
        "metric": "radiative_forcing",
        "time_horizon": 100,
        # other settings from .static_lcia() or .dynamic_lcia()
    },
    label="built 2020", # optional
)
```

A `TimexLCASettings` object holds everything a calculation needs, including demand, method, scenario, and every chosen option otherwise passed through `.build_timeline`, `.lci()`, `.static_lcia()` or `.dynamic_lcia()`.

The `timeline` / `lci` / `lcia` groups in the settings configuration keep a long settings block readable, but they are entirely
optional: `TimexLCASettings(..., starting_datetime=..., graph_traversal=..., metric=...)` builds the same
object.

## Running a configured time-explicit LCA

To run the whole time-explicit LCA pipeline based on `TimexLCASettings`, you can directly type:

```python
tlca = TimexLCA(settings).run()

print(tlca.static_score, tlca.dynamic_score)
```

`run()` executes `build_timeline()`, `lci()`, `static_lcia()` and `dynamic_lcia()`.
Every argument of those four methods is a field on the settings, so one object is
also the record of what was run: keep it, log it, or put a list of them into
[`compare()`](#comparing-scenarios).

## Re-running with different settings

Call `run()` again on the same object. Individual overrides apply to that call
only, leaving both the settings object and the object's own settings untouched:

```python
tlca.run(time_horizon=20)
tlca.run(starting_datetime=datetime(2030, 1, 1))
tlca.run(demand={("foreground", "B"): 1})
```

!!! tip "Passing a whole settings object"

    `run()` also takes a replacement: `tlca.run(other_settings)`. Build it with
    `dataclasses.replace(settings, starting_datetime=...)` so the other fields carry over
    unchanged.

## Comparing scenarios

`TimexLCA.compare()` takes a list of settings and returns a `ComparisonResult`:

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

!!! tip "Fetch scenarios on-the-fly"

    If you compare against a pathway that is not present in the current Brightway project, it can be fetched automatically. For details see [Creating scenario databases on-the-fly](../create_premise_dbs.md#creating-scenario-databases-on-the-fly)

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