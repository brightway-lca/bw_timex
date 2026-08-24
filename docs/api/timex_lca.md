---
icon: lucide/component
tags:
  - api
---

# TimexLCA

The main user-facing class of `bw_timex`. A `TimexLCA` takes a Brightway demand, an LCIA method and a set of time-specific databases, and produces a time-explicit inventory.

`TimexLCA.run()` runs the whole calculation - timeline, inventory, characterization - in one call, and is the recommended entry point; the stage methods it calls remain available for finer control.

`TimexLCASettings` holds everything one calculation needs, and can be passed straight to `TimexLCA()`; `TimexLCA.compare()` runs a list of them and returns a `ComparisonResult`. See [Repeated Runs & Scenario Comparison](../content/getting_started/configured_runs.md).

::: bw_timex.timex_lca
