# premise Temporal-Distribution Annotation — Design

**Date:** 2026-06-24
**Status:** Design — approved, pending spec review
**Branch:** `feat/premise-temporal` (off `main`)
**Scope:** Independent feature in the trails-learnings roadmap. Annotate
pre-existing premise-generated, year-specific bw2 databases with temporal
distributions, so `bw_timex` can run time-explicit LCA on a premise background
without the user hand-defining temporal data.

## Background

`bw_timex` reads temporal distributions off exchanges as
`bw_temporalis.TemporalDistribution` objects stored under
`exchange["temporal_distribution"]` (see
`bw_timex.utils.add_temporal_distribution_to_exchange`). Its
`traverse_background=True` path then honours temporal distributions defined on
background-database exchanges.

`premise` (the trails work, shipping in `premise >= 2.5.0`) curates background
temporal data in `premise/data/trails/temporal_distributions.csv`
(~9.7k rows), keyed by `(name, reference product)` plus ISIC/CPC
classification, each row carrying a `temporal_tag`, an age-distribution `type`,
`loc/scale/offsets/weights/min/max`, and `lifetime`. premise's
`TrailsDataPackage` loads that CSV into categorized buckets
(`_load_temporal_specs_from_csv`) and, in `add_temporal_distributions`, places
the temporal parameters on the correct exchanges using a fixed set of rules.

This feature reuses premise's curated data and its placement rules to annotate
the user's existing dated bw2 databases directly. It does **not** materialize,
unfold, or otherwise build databases — the user already has them (e.g. one
ecoinvent+premise database per scenario year, registered in their bw2 project).

## Goal

A function `add_premise_temporal_distributions(databases)` that, for each named
existing premise database, finds the exchanges premise would tag and writes the
corresponding `bw_temporalis.TemporalDistribution` onto them, returning a
summary plus a faulty/unmatched report. Idempotent; reuses premise's CSV loader
and placement rules.

Non-goals: building/unfolding databases, biosphere linking, constructing
`database_dates`, and any non-premise temporal source.

## premise placement rules (the behaviour we mirror)

From `premise/trails.py` `add_temporal_distributions` (verified against the
`trails_temporal_distributions_update` branch):

- **biomass_growth** — for a dataset whose `(name, reference product)` is in
  `biomass_growth_params`, set the temporal params on that dataset's
  **biosphere** exchange named exactly `"Carbon dioxide, in air"`.
- **stock_asset** — for a **technosphere** exchange whose **supplier**
  `(name, product)` is in `stock_asset_params`, set the supplier's params on
  that exchange.
- **maintenance** — for a technosphere exchange whose supplier `(name, product)`
  is in `maintenance_suppliers`, set a uniform distribution (premise code `4`)
  over `[0, lifetime]`, where `lifetime` is the **calling dataset's** lifetime
  from `dataset_lifetimes`.
- **end_of_life** — for a technosphere exchange whose supplier `(name, product)`
  is in `end_of_life_suppliers`, set a one-pulse distribution (premise code `6`)
  at the calling dataset's `lifetime`.
- **Ambiguity** — if a supplier matches more than one of
  stock_asset/maintenance/end_of_life, record a fault and skip.
- **Missing data** — technosphere exchange without a supplier product, or a
  maintenance/end_of_life match without a dataset lifetime, records a fault and
  skips.

premise temporal codes used: `1` discrete (mass at `loc`), `3` normal, `4`
uniform (`[min,max]`), `5` triangular, `6` discrete empirical (explicit
`offsets`/`weights`). All time values are in **years**.

## Architecture

New module `bw_timex/premise_temporal.py` (single responsibility: premise →
bw_timex temporal annotation). No changes to bw_timex's core engine. `premise`
is an **optional dependency** declared as the `premise` extra
(`pip install bw-timex[premise]`); the module imports premise lazily and raises
a clear, actionable error if it is missing or older than 2.5.0.

## Components

### 1. `load_temporal_specs() -> TemporalSpecs`

Reuses premise's own loader so the parsing/categorization (the part most likely
to evolve) stays in premise. Returns a small dataclass `TemporalSpecs` holding
the five premise buckets: `biomass_growth_params`, `stock_asset_params`
(both `dict[(name, ref), params]`), `maintenance_suppliers`,
`end_of_life_suppliers` (both `set[(name, ref)]`), and `dataset_lifetimes`
(`dict[(name, ref), float]`).

Implementation: call premise's
`TrailsDataPackage._load_temporal_specs_from_csv` against the CSV bundled in the
installed premise package (`premise.trails.FILEPATH_TEMPORAL_PARAMETERS`). If
premise exposes these only as instance methods, instantiate the minimal object
needed or call the underlying static parsing; the CSV path constant is public
enough to locate the file. A thin adapter isolates this coupling so a premise
API change touches one function.

### 2. `premise_params_to_td(params, *, lifetime=None) -> TemporalDistribution`

Pure converter from premise's `(code, loc, scale, min, max, offsets, weights)`
(+ optional `lifetime` for the maintenance/end_of_life synthetic forms) to a
`bw_temporalis.TemporalDistribution`, time unit years:
- code 3 normal → `easy_timedelta_distribution(..., kind="normal", loc, scale)`
- code 4 uniform → `easy_timedelta_distribution(..., kind="uniform", min, max)`
  (maintenance uses `min=0, max=lifetime`)
- code 5 triangular → `easy_timedelta_distribution(..., kind="triangular", ...)`
- code 1 discrete → single pulse at `loc`
- code 6 discrete empirical → explicit `offsets`/`weights` arrays
  (end_of_life uses a single pulse at `lifetime`)
Returns a `TemporalDistribution` with `date` as `timedelta64[Y]`. Independently
unit-testable with no bw2data.

### 3. `annotate_database(db_name, specs, *, overwrite=False) -> AnnotationReport`

Iterates the activities and exchanges of the existing bw2 database `db_name`,
applies the premise placement rules above, converts matched params via
`premise_params_to_td`, and writes the TD with the existing
`exchange["temporal_distribution"] = td; exchange.save()` path. Skips an
exchange that already has a temporal distribution unless `overwrite=True`.
Collects counts and a list of faulty/unmatched exchanges into an
`AnnotationReport`.

### 4. `add_premise_temporal_distributions(databases, *, overwrite=False) -> AnnotationReport`

Public entry point. `databases` is an iterable of database names (or a mapping
whose keys are database names — values, e.g. years, are ignored here). Loads
specs once, annotates each database, aggregates the reports. Exported from
`bw_timex/__init__.py`.

## Error handling

- premise missing or `< 2.5.0`: raise `ImportError`/`RuntimeError` with
  "install bw-timex[premise] (needs premise >= 2.5.0)".
- A named database not present in the project: raise a clear `KeyError`-style
  error naming the database before any writes.
- Unmatched rows / ambiguous tags / missing lifetimes: recorded in the report
  (mirroring premise's `temporal_distribution_faulty_exchanges` behaviour), not
  fatal.
- Annotation never raises out of a single bad exchange; it records and
  continues.

## Drift guard

The CSV parsing/categorization is reused from premise (not copied). The
placement loop is a faithful port of premise's `add_temporal_distributions`
rules; a reference test compares this module's placement decisions to premise's
own output on a small synthetic dataset so the port cannot silently drift.

## Testing

- `premise_params_to_td` (unit, no bw2data): each premise code → expected TD
  shape (normal/uniform/triangular/discrete/empirical), years resolution,
  maintenance `[0,lifetime]`, end_of_life pulse at lifetime.
- `annotate_database` (bw2 fixture): a synthetic database with
  (a) a dataset carrying a `"Carbon dioxide, in air"` biosphere exchange whose
  `(name,ref)` is in `biomass_growth_params`,
  (b) a technosphere exchange whose supplier is a stock_asset,
  (c) suppliers tagged maintenance and end_of_life with a dataset lifetime,
  (d) an ambiguous supplier (two tags),
  (e) an exchange already carrying a TD.
  Assert TDs land only on the right exchanges with the right shapes; ambiguous
  and missing-lifetime cases land in the report; idempotency (no overwrite by
  default; overwrite when requested).
- Reference/drift test: build matching premise spec buckets and assert this
  module tags the same exchanges premise's rules would.
- Error path: premise-missing import error message; unknown database error.
- Full suite passes; no new warnings.

## Deliverables

1. `bw_timex/premise_temporal.py`: `TemporalSpecs`, `AnnotationReport`,
   `load_temporal_specs`, `premise_params_to_td`, `annotate_database`,
   `add_premise_temporal_distributions`.
2. `premise` optional extra in `pyproject.toml`; lazy import + version guard.
3. Export `add_premise_temporal_distributions` from `bw_timex/__init__.py`.
4. Tests above.

## Follow-on (not this cycle)

Convenience helpers for building `database_dates` from premise database naming
conventions, if desired later.
