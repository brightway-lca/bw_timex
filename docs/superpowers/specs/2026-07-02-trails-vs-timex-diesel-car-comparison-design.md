# Design: trails vs. timex diesel-car score comparison notebook

**Date:** 2026-07-02
**Branch:** feat/premise-temporal
**Author:** Timo Diepers

## Purpose

Validate that `bw_timex` + `add_premise_temporal_distributions` reproduces the
temporal scores produced by **trails** on the *same* premise background, using
trails' own worked example (`examples/2.2. premise and imported lci example.ipynb`):
a diesel passenger car (`transport, passenger, car, diesel`) assessed at
reference year **2050**.

The deliverable is a new notebook
`notebooks/example_premise_temporal_comparison_trails.ipynb` that runs both
engines end-to-end and compares their scores.

## What actually differs between the two engines

The premise background (fuel-market composition shifting diesel→biodiesel across
scenario years, electricity mix, etc.) is *identical* input to both engines, so
it is not a source of divergence:

- **trails** reads year-varying background amounts straight from the premise
  datapackage matrices (`temporal_amount_source=matrix`).
- **timex** interpolates the same premise background across `database_dates`
  (2020 → 2030 → 2040 → 2050 → 2075 → 2100).

The only genuine difference is **where the temporal distributions come from**:

1. **Foreground TDs** on the diesel car's own exchanges — trails reads these from
   the imported spreadsheet `lci-pass_cars.xlsx` (hand-authored):
   - use-phase / wear / maintenance / road-maintenance / direct biosphere:
     uniform ±8 y (stats_arrays code 4, `loc=0, min=-8, max=8`)
   - road construction: uniform `loc=-20, min=-40, max=-1`
   - `passenger car production, diesel`: triangular `loc=-8, min=-12, max=-1`
     (code 5) — the manufacturing pulse before the use phase
2. **Background TDs** deeper in the supply chain — trails applies premise's
   curated `temporal_distributions.csv` internally; timex applies the **same
   file** via `add_premise_temporal_distributions`. This equivalence is the
   thing the notebook is meant to demonstrate.

Confirmed same source file:
`premise/data/trails/temporal_distributions.csv` (9658 rows) is what both
`add_premise_temporal_distributions` and trails consume.

## Shared data

Both engines must sit on the same premise scenario so score differences are
attributable to temporal handling, not to different backgrounds.

- **Scenario:** model `remind-eu`, pathway `SSP2-NDC`, system model `cutoff`,
  ecoinvent `3.12`, years `[2020, 2030, 2040, 2050, 2075, 2100]`.
- **timex side:** the existing bw project `ei312_REMIND_EU` already holds
  `ei312_REMIND-EU_SSP2_NDC_{2020,2030,2040,2050,2075,2100}` plus source
  `ecoinvent-3.12-cutoff` and `ecoinvent-3.12-biosphere`.
- **trails side:** generate a premise `TrailsDataPackage` with the parameters
  above. **Requires `IAM_FILES_KEY`** (premise IAM decryption key) supplied by
  the user via environment variable, and network access. Build cost ≈ hours for
  6 years. Note: the regenerated datapackage will match the existing bw dbs
  *scenario-for-scenario*; if the premise version that built the bw dbs differs
  from the installed `premise==2.3.7`, small numeric drift is possible and will
  be reported, not hidden.

## Method

Primary: `('ecoinvent-3.12', 'IPCC 2021', 'climate change: total (excl. biogenic CO2)', 'global warming potential (GWP100)')`
— trails 2.2's excl-biogenic method (verified present in the project).
trails receives the equivalent dash-joined string with `ei_version="3.12"`.

## timex model (foreground approach A)

Rebuild the `lci-pass_cars.xlsx` diesel activity as a `foreground` bw database
activity so its **foreground TDs come from the sheet** (matching trails), while
the deeper background TDs come from `add_premise_temporal_distributions`:

1. Delete/rebuild a `foreground` database.
2. Create activity `transport, passenger, car, diesel` (RER, unit km,
   production amount 1).
3. Add each xlsx exchange, resolving its input to the matching background node in
   `ei312_REMIND-EU_SSP2_NDC_2050` (technosphere) or the biosphere db
   (biosphere), with the xlsx `amount`.
   - Fuel rows (`diesel production…`, `esterification of rape oil`) and the two
     CO2 rows use `temporal_amount_source=matrix` in the sheet → point the edge
     at the background market and let timex interpolate the amount over dates. Do
     **not** hard-code the year columns.
4. Attach the sheet's foreground TDs as `bw_temporalis.TemporalDistribution`
   objects on the corresponding edges (uniform ±8, road-construction uniform,
   production triangular). Map stats_arrays codes → discretised TD arrays at
   yearly resolution.
5. `add_premise_temporal_distributions(BG_DATABASES)` on all six variants for the
   deep background.

## Run + comparison

- **trails:** follow 2.2 — load datapackage, `import_excel_inventory`, select the
  diesel activity by metadata, `lca(...)` temporal + `static_lca(year=2050)`.
- **timex:** `TimexLCA({fg_diesel: 1}, method, database_dates)`,
  `build_timeline(starting_datetime="2050-01-01", temporal_grouping="year",
  traverse_background=True, graph_traversal="bfs")`, `lci()`, `dynamic_lcia()`.
  Align routing depth/cutoff to trails' adaptive default as closely as the timex
  API allows; document any knob that can't be matched.
- **Comparison output:**
  - total temporal score: trails vs timex, absolute + % difference
  - static/base score cross-check
  - per-year score series overlaid on one plot (both engines)
  - a short table of the largest per-year contributors to any gap

## Fidelity caveats (documented in the notebook)

- trails uses adaptive-depth routing with a relative cutoff; timex uses BFS
  background traversal with `max_calc`. These are not identical traversal
  strategies, so small differences in deep-chain capture are expected.
- Foreground TD discretisation: trails samples continuous distributions; the
  timex side builds discrete yearly TDs. Uniform/triangular are reproduced at
  yearly steps; exact bin edges may differ by fractions of a year.
- Temporal grouping is yearly on both sides for a like-for-like series.

## Scope / non-goals

- No FaIR climate-emulator step (trails 2.2 §12) — out of scope; the comparison
  is on characterized GWP100 scores.
- The electric-car block in the xlsx is ignored; only the diesel activity.
- Not a general bw→trails exporter; the trails datapackage comes from premise.

## Open runtime dependencies (resolved at implementation, not design)

- `IAM_FILES_KEY` from the user (blocks the trails datapackage build).
- `pip install -e` the local trails repo into the 3.12 `.venv`.
- Confirm premise accepts model string `remind-eu` / pathway `SSP2-NDC`.

## Risks

- Datapackage build is long and network/key dependent — highest-risk step; gate
  it early and cache the resulting zip.
- Scenario regeneration may not be byte-identical to the existing bw dbs; report
  the base-LCA cross-check so any background mismatch is visible before blaming
  temporal handling.
