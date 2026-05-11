# Design: `TemporalDistribution` as demand value in `TimexLCA`

**Date:** 2026-05-11
**Scope:** `bw_timex` core (`TimexLCA`, `EdgeExtractor`, validation).

## Goal

Let users pass a `TemporalDistribution` (TD) as the value of a `demand` dict
entry to `TimexLCA`, in addition to the existing scalar form. The TD
describes when, in calendar time (or as offsets from
`starting_datetime`), the functional unit is demanded and in what
quantities — producing install-vintage cohorts at the FU boundary
without needing to mount an output-side `temporal_distribution` on the
demanded product's production edge.

## Motivation

Today, the only way to spread the functional unit across calendar time
is to put an output-side RTD on the production edge of the demanded
product. That is awkward when the temporal pattern belongs to the
*demand* (e.g. a fleet rollout schedule, a service contract that runs
over years, a delivery cadence) rather than to the production process
itself.

## API

```python
from bw_temporalis import TemporalDistribution

# Existing — still supported.
TimexLCA(demand={product: 1.0}, ...)

# New — absolute calendar dates.
td_abs = TemporalDistribution(
    date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
    amount=np.array([60.0, 40.0]),
)
TimexLCA(demand={product: td_abs}, ...)

# New — relative offsets from starting_datetime.
td_rel = TemporalDistribution(
    date=np.array([0, 5], dtype="timedelta64[Y]"),
    amount=np.array([60.0, 40.0]),
)
tlca = TimexLCA(demand={product: td_rel}, ...)
tlca.build_timeline(starting_datetime=datetime(2025, 1, 1))   # mandatory
```

Mixing forms across demand entries is supported:
`demand={p1: 5.0, p2: td_abs, p3: td_rel}`.

## Semantics

- A TD demand value declares "demand `amount[i]` units on `date[i]`".
  Total demand magnitude per node = `sum(amount)`.
- **Static base LCA** (`tlca.base_lca`): each node receives a scalar
  equal to `sum(amount)`. Static base score is therefore independent of
  the TD's date axis and matches what a plain `bw2calc.LCA(demand=
  {p: sum(amount)})` would produce.
- **Time-explicit timeline**: each `(date, amount)` entry seeds the FU
  for that node. The resulting timeline contains one or more FU rows
  per TD entry. If the demanded product's production edge has an
  output-side `temporal_distribution`, the demand TD and the
  production RTD are **convolved** (cross product over their
  `(date, amount)` pairs) and a warning is logged. If no production
  RTD is present, the demand TD's entries map 1-to-1 to FU rows.
- **Relative TD dates** (`timedelta64[*]`) are interpreted as offsets
  from `starting_datetime` (the existing `build_timeline` argument).
  If any demand value is a relative TD and `starting_datetime` is not
  supplied to `build_timeline`, raise.
- **Absolute TD dates** (`datetime64[*]`) are used as given. They do
  not require `starting_datetime` for that entry (other parts of the
  timeline builder still receive `starting_datetime` for foreground
  RTDs).

## Boundary cases (explicit)

| Case | Behavior |
|---|---|
| TD with 1 entry, absolute date | Equivalent to scalar demand anchored at that date. Backward compatible with existing single-cohort behavior. |
| TD `amount` contains negative values | Reject with `ValidationError`. |
| TD `amount` contains zeros | Filtered out before seeding (no zero-amount FU rows). |
| TD with no entries | Reject with `ValidationError`. |
| TD with non-`datetime64` and non-`timedelta64` date dtype | Reject with `ValidationError`. |
| Demand node's production edge has output-side RTD | Convolve + emit `UserWarning` `"Both demand TD and output-side production RTD present on <product>; convolving them. ..."`. |
| Mixed scalar + TD across demand entries | Each entry handled independently. |
| Demand keyed by a product node (explicit paradigm) | Resolves to producing process as today; TD applied at FU seeding. |
| Demand keyed by a process node (chimaera paradigm) | TD applied at FU seeding. Same convolution rule with output-side RTD on the chimaera's reference-product edge. |
| Relative TD without `starting_datetime` | Raise in `build_timeline`. |
| Mixing relative and absolute TDs in same `TimexLCA` call | Allowed; relative ones use `starting_datetime`, absolute ones use their own dates. |

## Implementation touchpoints

1. **`validation.py / TimexLCAInputs`**
   Accept `Union[int, float, TemporalDistribution]` for demand values.
   - If TD: date dtype must be `datetime64[*]` or `timedelta64[*]`.
   - All TD amounts must be non-negative; at least one must be > 0.

2. **`TimexLCA.__init__`**
   Store original `self.demand` (dict that may contain TDs). Derive
   `self._scalar_demand` (dict of node → `sum(amount)` or the scalar)
   for use by `base_lca` and any external static comparisons.

3. **`prepare_base_lca_inputs`**
   Use `self._scalar_demand` rather than `self.demand`.

4. **`build_timeline`**
   - If any demand entry has a relative-dtype TD and
     `starting_datetime` is missing → raise.
   - Pass `self.demand` and `starting_datetime` into the
     `TimelineBuilder` / `EdgeExtractor` so the FU seeding can use the
     per-entry TD.

5. **`EdgeExtractor.build_edge_timeline`**
   Replace the current single-`t0` FU seeding with a per-FU-edge TD:
   - If `demand[node]` is scalar: behave as today (single `t0` × scalar).
   - If `demand[node]` is TD: build an absolute-date TD (convert from
     timedelta via `starting_datetime` if needed); use it instead of
     `self.t0` when seeding the FU edge. The existing
     `td_producer * t0` mechanism naturally handles the convolution
     with the output-side production RTD; the only change is
     substituting `t0` with the demand TD.
   - When both demand TD and production RTD on the demanded product
     are present, log the warning before the seed.

6. **`prepare_bw_timex_inputs._build_indexed_demand`**
   No change. Reads FU rows from the (now correctly populated)
   timeline.

7. **Static-vs-time-explicit cross-check** (`tlca.base_lca.score`)
   `prepare_base_lca_inputs` already constructs the scalar LCA; with
   the scalar-demand extraction in step 2, base score stays equal to
   `bw2calc.LCA({p: sum(amount)})`.

## Tests (TDD; written before implementation)

All tests live in `tests/test_demand_temporal_distribution.py` (new
file). Each test sets up a tiny project with one chimaera background
and one foreground product/process so the assertions are exact.

1. **`test_scalar_demand_unchanged`** — passing
   `{p: 1.0}` produces same timeline + score as today (regression).
2. **`test_absolute_td_demand_seeds_fu_rows`** — `td_abs` with two
   absolute dates and two amounts; no production RTD on the demanded
   product. FU rows match the TD entries.
3. **`test_relative_td_demand_requires_starting_datetime`** — TD with
   `timedelta64[Y]` dates and no `starting_datetime` raises.
4. **`test_relative_td_demand_with_starting_datetime`** — TD with
   relative dates resolves to the same FU rows as the absolute
   equivalent.
5. **`test_demand_td_convolves_with_production_rtd`** — both a demand
   TD and an output-side `temporal_distribution` on the production
   edge are present; FU rows are the cross product; a `UserWarning` is
   emitted.
6. **`test_mixed_scalar_and_td_demand_entries`** — `{p1: 5.0,
   p2: td_abs}` produces the union of FU rows; static base score
   matches the equivalent scalar demand sum.
7. **`test_demand_td_static_base_score_matches_scalar_sum`** — for
   any TD demand, `tlca.base_lca.score` equals
   `bw2calc.LCA({p: sum(amount)})`.
8. **`test_demand_td_multi_vintage_relinking`** — extension of the
   existing `test_multi_vintage_demand_splits_across_cohorts`: two
   background databases at distinct dates, demand TD with two dates
   each falling in a different background. Score equals the
   demand-weighted background mix.
9. **`test_demand_td_negative_amount_rejected`** — TD with negative
   amount entry raises `ValidationError`.
10. **`test_demand_td_wrong_dtype_rejected`** — TD with e.g. `float64`
    date dtype raises `ValidationError`.

## Out of scope

- No characterization-side TDs on demand (dynamic LCIA already
  handles emission timing).
- No simultaneous TDs on multiple demand entries that share a
  production edge (the FU seeding still treats each demand entry
  independently).
- No automatic resampling/grouping of demand TD entries onto coarser
  temporal_grouping; if user wants yearly resolution they pre-collapse
  their TD.

## Risks

- The `EdgeExtractor`'s `_get_temporal_distribution_amount_and_date`
  path is tangled; the substitution of `t0` happens inside a hot loop
  in `build_edge_timeline`. Implementation must verify the absolute-
  -date conversion (timedelta → datetime64 via `starting_datetime`)
  preserves dtype precision (`datetime64[s]` throughout).
- Convolution warning must fire **once per demand entry**, not once
  per timeline row, to avoid log spam in fleets.
- `bw_temporalis`'s `TemporalDistribution.__mul__` semantics: a TD with
  multi-entry date axis multiplied by a scalar yields a TD scaled
  element-wise. Multiplying a TD by another TD yields a convolved TD.
  Implementation relies on this; pin the expected behavior with a
  unit test.
