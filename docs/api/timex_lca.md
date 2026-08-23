---
icon: lucide/component
tags:
  - api
---

# TimexLCA

The main user-facing class of `bw_timex`. A `TimexLCA` takes a Brightway demand, an LCIA method and a set of time-specific databases, and produces a time-explicit inventory.

`TimexLCASettings` holds everything one calculation needs, for `TimexLCA.from_settings()` and `TimexLCA.run()`; `TimexLCA.compare()` runs a list of them and returns a `ComparisonResult`. See [Repeated Runs & Scenario Comparison](../content/getting_started/scenarios.md).

::: bw_timex.timex_lca
