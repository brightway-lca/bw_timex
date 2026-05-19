# Redesign: getting_started_explicit_process_product.ipynb

**Date:** 2026-05-08
**Path:** `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb`

## Goal

Replace the abstract A/B/CO2 example with a concrete fleet-flavored case
(residential heat pumps) that motivates the explicit product/process paradigm
through a real-world research question, and demonstrates how a unit process
plus normalized RTDs aggregates into a fleet-level result.

Add a second part where the made-up RTDs are replaced with shapes derived from
a tiny `flodym` dynamic MFA, so readers see the bridge between dMFA fleet
thinking and `bw_timex` per-unit RTD thinking.

## Research question

> A municipality plans to subsidize installation of 100 residential heat pumps
> between 2025 and 2045 to replace gas boilers. What is the cumulative
> greenhouse gas footprint of that program when we account for (a) the
> electricity grid decarbonizing between 2025 and 2035 and (b) heat pump
> models installed in 2035 having ~30 % better COP than those installed in
> 2025?

## System

```
biosphere:
  CO2
  refrigerant_leak  (high GWP)

background (chimaera, one process+product per database):
  electricity_market_2025  ~0.40 kg CO2 / kWh
  electricity_market_2035  ~0.10 kg CO2 / kWh

foreground (explicit product/process):
  heat_pump            (product, type=product, unit="unit", no exchanges)
  heat_pump_lifecycle  (process, type=process, unit="unit")
    production:  1 heat_pump                    td=td_install
    input:    18 000 kWh electricity_market     td=td_use
                                                temporal_evolution_factors=cop_factors
                                                temporal_evolution_reference="consumer"
    biosphere:    2 kg refrigerant_leak         td=td_install
```

`heat_pump_lifecycle` is a per-unit LCA process — its exchange amounts
describe one heat pump's cradle-to-grave activity, with the use-phase
electricity bundled in.

## Demand

```python
demand = {heat_pump: 100}    # 100 HPs deployed across the horizon
```

## Part 1 — made-up RTDs

```python
td_install = TD([0, 5, 10] yr, [0.2, 0.5, 0.3])   # install-year shape
td_use     = TD([0, 6, 12] yr, [0.4, 0.4, 0.2])   # per-unit lifetime kWh shape
cop_factors = {datetime(2025,1,1): 1.0, datetime(2035,1,1): 0.7}

build_timeline(starting_datetime=datetime(2025,1,1), temporal_grouping="year")
```

Show:
1. Timeline with `date_producer` / `date_consumer` columns. Vintages are
   2025 / 2030 / 2035; electricity rows pile under each vintage with
   td_use offsets.
2. Aggregation by vintage: confirms 20 / 50 / 30 unit cohorts, raw kWh per
   cohort vs effective kWh after COP factor.
3. `static_score` vs `base_lca.score` — quantifies fleet-time effect.
4. Optional A/B run with COP factors removed → isolates vintage-efficiency
   contribution.

## Part 2 — RTDs from a tiny flodym dMFA

Tiny dMFA setup:

- Time horizon 2025–2060 (extends past horizon to capture last cohort's
  retirements).
- Exogenous stock trajectory `S(t)`: linear ramp from 0 to 100 units
  between 2025 and 2045, flat after.
- Lifetime model: Weibull, mean = 18 yr, shape ≈ 2.
- flodym yields: inflow `I(t)`, outflow `O(t)`, and the survival /
  age-cohort matrix.

Normalize:

- `td_install = TD(I_year_offsets, I(t) / sum(I(t)))`
- `td_use    = TD(age_year_offsets, age_use_profile / sum(age_use_profile))`

Where `age_use_profile[k]` is the fraction of one unit's total lifetime
electricity drawn `k` years after install. With constant annual draw and
Weibull survival `S(k)`, `age_use_profile[k] ∝ S(k)`. Total lifetime kWh
on the edge stays 18 000.

Rebuild `heat_pump_lifecycle` with derived `td_install` / `td_use`,
re-run TimexLCA, compare results vs Part 1.

Concluding plot: stacked bar of CO2 per calendar year from manufacturing
(refrigerant + install impacts) and operation (electricity), showing how
fleet impacts spread across 2025–2060.

## Why explicit product/process here

- Output-side RTD on `heat_pump_lifecycle → heat_pump` carries install-year
  distribution → `date_consumer` = install vintage.
- Input-side RTD on electricity edge carries operational profile relative to
  install → `date_producer` = calendar year of electricity draw.
- Vintage-specific COP improvements hang on the electricity edge with
  `temporal_evolution_reference="consumer"` so each vintage keeps its own
  efficiency across all later draw years.
- With chimaera (process == product), there is no separate production edge
  to host the install-year RTD; install timestamp and operational
  timestamps collapse onto one node.

## Notebook structure

1. Title + research question + small mermaid diagram of the system.
2. "Why explicit product/process for fleet modeling" markdown.
3. Project setup: clean project, biosphere, two background databases.
4. Part 1 setup: define `td_install`, `td_use`, `cop_factors`, write
   foreground product + process.
5. Walk-through of how unit process + RTDs + demand aggregates into fleet:
   small table or markdown breakdown of vintages × use-phase years.
6. `database_dates`, `TimexLCA`, `build_timeline`, timeline display.
7. Aggregation by vintage, effective kWh, scores.
8. A/B comparison with/without COP factors.
9. Part 2: flodym dMFA — stock trajectory, Weibull lifetime, derive
   `td_install` and `td_use`. Plot the dMFA inflow / stock / outflow.
10. Rebuild foreground exchanges with derived RTDs (use Brightway exchange
    edits, same pattern as A/B comparison cell). Re-run TimexLCA.
11. Compare Part 1 vs Part 2 scores; show year-by-year emissions plot.
12. Quick recap: unit process + normalized RTDs + scalar demand → fleet.

## Out of scope

- No ecoinvent / premise integration; backgrounds stay tiny chimaera.
- No regionalization, no characterization beyond the simple `(our, method)`
  CO2-eq method plus `dynamic_characterization` GWP100 in optional final
  cell.
- No multi-product foreground; one product, one process.
- No EOL recycling credit modelling (refrigerant leak is the only EOL-ish
  flow and it fires at install for simplicity).

## Risks / things to verify during implementation

- Confirm `temporal_evolution_factors` accepts datetime keys at the year
  granularity used here, and that `get_temporal_evolution_factor` returns
  the expected step values.
- Confirm flodym API for "stock-driven model with Weibull lifetime
  distribution" matches the tiny snippet assumed here. If flodym's
  current API differs, substitute equivalent numpy / scipy code that
  produces the same `I(t)`, `S(k)` arrays — preserve the conceptual story.
- Confirm `bd.labels.product_node_default` / `process_node_default` /
  `production_edge_default` / `consumption_edge_default` are the
  identifiers in the user's bw2data version (already used in the existing
  notebook, so safe to assume).
- Notebook must execute end-to-end in the project's `.venv` without
  manual setup beyond `pip install bw_timex flodym` (already present per
  the existing fleet notebook).
