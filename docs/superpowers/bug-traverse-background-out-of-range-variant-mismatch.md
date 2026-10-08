# Bug: `traverse_background` — out-of-range dates source different variants on producer vs consumer side

**Status:** open (diagnosed, not fixed). Separate from the already-fixed
`max_calc`-truncation bug (`ed1ae5e`, *cap max_calc-truncated background frontier
as static leaf*).

**Affects:** `TimexLCA.build_timeline(traverse_background=True)` on real premise
backgrounds with deeply-stacked long-lifetime temporal distributions.

## Symptom

`build_timeline` raises

```
KeyError: (('dp312_SSP2_NDC_2020', '<code>'), 1970)
```

in `timeline_builder.get_time_mapping_key`, or (if the missing key is forced in)
`NonsquareTechnosphere` in `lci()`. Small in-range graphs never hit it; it needs
a real premise chain whose stacked negative TDs push a node's date **outside**
`database_dates`.

## Reproduction

Reliably reproduced by the diesel-car premise case
(`notebooks/example_premise_temporal_comparison_trails.ipynb`): a
`transport, passenger, car, diesel` foreground on the `dp312_SSP2_NDC_*`
(REMIND-EU SSP2-NDC) variants, `traverse_background=True`,
`cutoff=1e-3`, `max_calc=2000`, `graph_traversal="bfs"`, `starting_datetime=2050`.

**A minimal fixture reproduction was NOT found** despite ~8 attempts
(single-path and dual-role, in-range and out-of-range, 2–3 variants). The trigger
needs the specific convergence / dual-role structure of the premise graph (a
common infrastructure activity reached both as a first-level market and deep as a
variant-resolved producer, at an out-of-range date). This is the main thing to
crack to get a TDD failing test.

## Root cause (evidenced)

The failing node is **"road construction" (`road`, RoW)**, consumed at year
**1970** — *below* the earliest database date (2020), i.e. **out of range**.

Instrumenting the raw (pre-rounding) dates shows road construction's **producer**
date-spread and **consumer** date-spread **match** (both `1970–2009`, plus a
separate `2010–2048` foreground cohort). So this is **not** a date/rounding
bug — the years agree.

The mismatch is the **variant (database)**:

- The **consumer** side resolves road@1970 to the **nearest database by date** →
  `background_2020` (the normal, always-working interpolation path). Lookup key:
  `(('dp312_SSP2_NDC_2020', code), 1970)`.
- The **producer** side registers road@1970 under whatever **variant the descent
  routed it to** (path/cohort-dependent — e.g. the 2040 or 2050 variant), because
  the registration uses `db_key = producer_node["database"]`.

Same logical activity, same year, but **two different variant node-ids** →
registered under one `(variant, 1970)`, looked up under another → `KeyError`.
(`road_ids` has one id per variant, confirming distinct per-variant nodes.)

Intended behaviour (per maintainer): a node at an out-of-range date should just
**take data from the nearest database on both sides**, as the normal
interpolation already does everywhere else. The producer/registration side is the
one not doing so.

## Where to look

- `bw_timex/timeline_builder.py`
  - producer registration loop: `db_key = producer_node["database"]` (the
    variant-resolved producer is keyed under its routed variant).
  - `get_time_mapping_key` (consumer lookup): uses `self.nodes[node_id].key`
    (the nearest-DB-resolved variant on the consumer side).
  - `_leaf_background_producers` / `add_column_temporal_market_shares_to_timeline`
    (the nearest-DB market path the consumer side follows).
- `bw_timex/edge_extractor.py`
  - `_descend_variant_subtree`: variant routing during descent (cohort/date that
    determines the producer's variant id), and `variant_resolved_producers`.

## Suggested fix direction

Reconcile the variant assignment for out-of-range dates so the producer side uses
the **nearest database by the node's own date**, matching the consumer/market
interpolation — instead of the descent-routed variant. Equivalently: a
variant-resolved producer whose date falls outside `database_dates` should be
treated like the nearest-DB market (the same treatment the consumer side already
applies), not temporalized under its routed variant.

## TDD note

Write the failing test first. The open problem is a **minimal** reproduction:
construct a 2–3 variant fixture where the same background activity is reached both
as a first-level nearest-DB market and as a deep variant-resolved producer at a
date pushed below the earliest DB by stacked negative TDs, and assert
`build_timeline` + `lci()` + `static_lcia()` succeed and conserve impact. If a
minimal fixture stays elusive, the diesel case is the reliable (integration)
reproduction.
