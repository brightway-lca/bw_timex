# Design: conserve impact for background production-edge TDs in `traverse_background`

**Status:** approved (design), not yet implemented.
**Scope:** `bw_timex` variant-aware background descent (`traverse_background=True`).

## Problem

When `traverse_background=True` descends into a background node that carries a
**production-edge temporal distribution** (a TD on the node's own production
exchange), the descent mishandles it:

- The production TD is convolved into the node's **child** (its expansion /
  consumer-side date band) but **not** into the edge that **produces** the node.
- Result 1 — **`KeyError`**: the node is *consumed* at cohort years it was never
  *registered as a producer* at, so `TimelineBuilder.get_time_mapping_key` misses
  (`timeline_builder.py`).
- Result 2 — **N× over-count**: the production-TD cohort weights are lost
  (`_join_datetime_and_timedelta_distributions` tiles the producer TD's amounts
  and discards the consumer-side cohort weights), so each spread cohort emits the
  full exchange coefficient instead of its weighted share. A 3-cohort production
  TD inflates the score ~3×.

Minimal reproduction (both `priority` and `bfs` engines):
`fu -> bg_A -> bg_B -> bg_C -> CO2`, two dated variants, `bg_A->bg_B` carries a
technosphere TD (triggers the variant-split descent), and `bg_B` carries a
production-edge TD spread over several years. `base_lca.score == 1.0` but the
time-explicit score comes out `= number of production-TD cohorts`.

This is distinct from the premise `KeyError` originally reported (documented in
`docs/superpowers/bug-traverse-background-out-of-range-variant-mismatch.md`),
whose real mechanism is a **dual-path** (referenced-variant matrix traversal vs
proxy descent) date-rounding divergence — premise backgrounds carry no
production-edge TDs. That facet is **out of scope** here (see below).

## Chosen semantics: cohort split (FU-seed style)

A production-edge TD on a descended background node `bg_B` (weights
`[0.5, 0.3, 0.2]` at `+0/+3/+6y`, demanded by `bg_A` at 2020) splits `bg_B` into
weighted **produced** cohorts, each of which is both produced and consumes its
inputs at its own date:

```
bg_B@2020 (w=0.5) -> bg_C@2020 -> CO2@2020
bg_B@2023 (w=0.3) -> bg_C@2023 -> CO2@2023
bg_B@2026 (w=0.2) -> bg_C@2026 -> CO2@2026

bg_B PRODUCED at {2020, 2023, 2026}
bg_B CONSUMED at {2020, 2023, 2026}   (bands match by construction)
```

This mirrors the existing, correct FU-seed handling in `build_edge_timeline`
(`edge_extractor.py:1187-1209`), where a functional unit's production TD spreads
it into weighted cohorts that are each registered.

## Approach: fold the production TD into the effective producer TD (background only)

Today each descent site emits the producer edge at the **unshifted**
`abs_td_producer`, then applies the producer's own production-edge TD only to the
child:

```python
child_td, child_abs_td = distribution, abs_td_producer
producer_production_td = self._normalized_production_edge_td_from_proxy(producer_process)
if producer_production_td is not None:
    child_td = (distribution * producer_production_td).simplify()
    child_abs_td = _join_datetime_and_timedelta_distributions(producer_production_td, abs_td_producer)
```

Change: when `producer_production_td` is present, fold it into the **effective
producer TD of the edge itself** so the producer is registered at the same
spread, weighted cohorts it is later consumed at:

- `td_producer_eff = (td_producer * producer_production_td)` — a proper
  `TemporalDistribution` convolution, which **multiplies** weights correctly
  (unlike `_join`, which tiles the producer amounts and drops the rest).
- Emit the producer edge from `td_producer_eff` (its `td_producer`,
  `distribution`, and `abs_td_producer`).
- Queue the child from those same cohorts — **no** separate re-application of the
  production TD.

Each emitted cohort then carries `exchange_weight × prodTD_weight`; the producer
is registered at exactly the years it is consumed at.

### Sites (both background-only, shared by both engines)

- `_emit_variant_split_for_consumer_date` — `edge_extractor.py:334` (prodTD
  applied at `:401`)
- `_descend_variant_subtree` — `edge_extractor.py:426` (prodTD applied at `:540`)

Both live in `VariantBackgroundMixin` and run **only** on variant-locked
background nodes read from proxies. The priority `EdgeExtractor` (`:554`) and
`EdgeExtractorBFS` (`:919`) both reach them through the shared `_emit_variant_split`
(`:250`). The foreground/matrix path and the explicit product/process modeling
are **not** touched.

### Array-alignment constraint

`extract_edge_data` (`timeline_builder.py`) explodes `abs_td_producer.date` /
`abs_td_producer.amount` against `len(td_producer)` to tile consumer dates. The
fix keeps this convention, substituting `td_producer_eff`. If `.simplify()`
merging causes the distribution and absolute-date arrays to diverge in length,
derive the emitted amount from the same array used for the dates (the same
consistency the FU-seed maintains between `seed_td` and `seed_abs_td`). This is
the main implementation risk and is guarded by the conservation tests below.

## Snap fallback: kept

The nearest-registered-year fallback added to
`TimelineBuilder.get_time_mapping_key` is **retained** as defense-in-depth for the
still-unreproduced premise dual-path `KeyError`. After this root fix it should no
longer fire on the production-TD path (bands match), but it stays as a safety net
and is already full-suite-green.

## Testing (TDD)

RED (write/confirm failing first):
- Single production-TD chain conserves: `fu -> bg_A -> bg_B[prodTD] -> bg_C -> CO2`,
  time-explicit score `== base_lca.score`, both `priority` and `bfs`.
- Convergent + production-TD conserves (a node reached by two parents, one path
  carrying the production TD).

GREEN / regression:
- Full existing suite stays green (currently 244 passed).
- Premise diesel smoke (integration): `build_timeline` + `lci` + `static_lcia`
  runs without `KeyError` and yields a stable score. Kept out of the unit suite
  if it needs the premise project; run manually otherwise.

Housekeeping:
- Promote the throwaway `tests/test_repro_variant_mismatch.py` into a proper,
  named test module; replace the currently-failing over-count test with the
  conservation assertions above.

## Out of scope

- Premise dual-path `KeyError` root cause (matrix-vs-proxy date rounding). The
  snap covers it for now; reproduce and fix separately.
- Foreground explicit product/process modeling — untouched.
- Global semantics of `_join_datetime_and_timedelta_distributions` — untouched.
