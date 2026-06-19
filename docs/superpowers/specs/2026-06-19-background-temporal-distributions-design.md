# Background temporal distributions (`traverse_background`)

**Date:** 2026-06-19
**Branch:** `background-temporal-distributions`
**Status:** Design approved, pending implementation

## Goal

Today `bw_timex` only honors temporal distributions (`temporal_distribution`) and
`temporal_evolution` on exchanges in the **foreground** system. Background databases
are treated as static time-snapshots: traversal stops at the first-level background
frontier, and each background flow is mapped to the temporally-appropriate background
db variant(s) via the "temporal market" interpolation.

This feature lets temporal distributions defined **inside background databases** take
effect. The traversal descends into the background, propagates those TDs by
convolution exactly like foreground TDs, and the resulting time-spread flows are
sourced from the temporally-appropriate variant of each background database. Because
the TD is read from the *respective* variant being traversed, background temporal
behavior can change over time (a 2030 variant may define a different TD than the 2020
variant of the same exchange).

The feature is gated behind a new opt-in flag; default behavior is unchanged.

## Public API

New keyword argument on `TimexLCA.build_timeline(...)`:

```python
traverse_background: bool = False
```

- `False` (default): exact current behavior. Background databases are excluded from
  traversal; the first-level background frontier is the stop.
- `True`: traversal descends into background databases, bounded only by `cutoff` and
  `max_calc`. Background TDs are honored. Variant-aware descent (below) applies.

The flag is plumbed through `TimexLCA.build_timeline` → `TimelineBuilder.__init__` →
both `EdgeExtractor` (priority) and `EdgeExtractorBFS` (bfs).

## Core mechanism: variant-aware descent

### Removing the static exclusion

Two knobs currently halt the traversal at the background:

1. `edge_filter_function` / `edge_ff`: the default marks every node in a static
   background db as a **leaf** (recorded as an edge, not descended into).
2. `static_activity_indices`: in BFS, inputs in this set are skipped entirely; in
   priority it is passed to `TemporalisLCA`.

When `traverse_background=True`:

- `static_activity_indices` for background nodes is **empty** (background is no longer
  pre-excluded).
- The **default** `edge_filter_function` becomes "nothing is a leaf"
  (`lambda _: False`) instead of "all background nodes are leaves". A user-supplied
  `edge_filter_function` is still respected.
- Traversal is bounded purely by `cutoff` (and `max_calc`). BFS gains a `max_calc`
  guard for parity and safety, since unbounded background descent is dangerous.

### The split rule (replaces the fixed first-level frontier)

There is no longer a single static frontier. Instead, a producer is **split into
interpolated variants by its resolved date** whenever that date selects different
variant(s) than the **consumer's current variant context**:

- **Background entry** — consumer is in the foreground (no variant context), producer
  is in a background db: the producer date `T` selects interpolated variants
  (e.g. `{V_2020: 0.6, V_2030: 0.4}`). Descend into each branch, weighted by its share.
- **Inside a variant, no-TD edge** — the producer date equals the consumer date, so it
  resolves to the **same** variant. **Sticky**: stay in that single variant, no split.
  This is numerically identical to today's static background inventory.
- **Inside a variant, TD edge** — the TD shifts the producer date. Re-evaluate the
  variant selection for the new date and split again among the variants appropriate for
  it. Descend into each branch.

Consequences:

- Branching happens only at background entry and at TD-carrying edges. Because TDs are
  rare in the background, the branch count per path is bounded (≈ `2^(#TD edges on the
  path)`); `cutoff`/`max_calc` prune the rest.
- **Backward compatibility:** with zero background TDs, the producer splits once at
  entry and stays sticky forever — reproducing today's behavior exactly.

### Reading the respective variant

Each traversal branch carries the **variant db** it is currently in. When descending a
branch in, say, `V_2030`, the node's technosphere/substitution exchanges, their
amounts, the reference-product production amount, **and their TDs** are read from
`V_2030`'s activity proxy. The mapping from a node to its counterpart in another variant
db reuses `interdatabase_activity_mapping` (matched on name / reference product /
location) plus direct bw2data exchange reads.

Each terminal leaf is a node-in-`V` mapped to `V`'s static inventory with share
`{V: 1}`. The full inter-variant distribution is carried by the **branch shares**
accumulated during descent, not by a per-leaf interpolation.

### Priority-mode caveat (accepted)

`EdgeExtractor` inherits from `TemporalisLCA`, whose heap ordering uses per-subgraph
**LCA scores computed from `base_lca`**. The other variants' subgraphs are not in
`base_lca` (the foreground links only one variant), so when descent hops into another
variant there is no exact subgraph score available. For hopped-variant nodes the heap
score is **approximated from the nominal node's `base_lca` score**.

This can slightly misorder the priority heap, which only affects *which* edges are
explored first under `max_calc`/`cutoff` — not the correctness of explored amounts.
This is an accepted limitation. It is documented and a **one-time warning** is emitted
when `traverse_background=True` with `graph_traversal="priority"`. BFS has no score
dependency and is unaffected.

## Downstream changes

### `build_timeline` (TimelineBuilder)

- `temporal_market_shares` is assigned per **leaf** producer as `{resolved_variant: 1}`.
  Inter-variant distribution is already carried by the branch shares from descent.
- The hardcoded reliance on `node_collections["first_level_background_static"]` for
  deciding which producers get `temporal_market_shares` is replaced by the dynamic
  leaf/split logic. A background producer is a temporal-market leaf iff it is **not
  traversed into** (never appears as a consumer in the edge timeline).
- In `False` mode these reduce to the existing behavior, so the current code path is
  preserved.

### `matrix_modifier`

The temporal-market-vs-temporalized branch currently keys on
`previous_producer_node["database"] in database_dates_static`. Change it to key on the
**presence of `temporal_market_shares`** on the row:

- Row has `temporal_market_shares` ⇒ leaf ⇒ wire as a temporal market to the variant's
  static inventory.
- Row has no `temporal_market_shares` ⇒ traversed-into ⇒ rebuild as a temporalized
  process from its explicit child edges (its full static inventory is **not** pulled in).

This prevents double-counting a background process that was descended into (it would
otherwise contribute both its full static inventory *and* its explicit child edges).
In `False` mode "has shares" ⇔ "is a static background producer", so behavior is
identical.

### Untouched

`interdatabase_activity_mapping` population (keyed on
`temporal_market_shares.notnull()`), dynamic biosphere construction, and `lci()` are all
generic over the leaf set and require no changes.

## Testing (TDD — failing test first for each)

New fixture: `foreground → bg_A → bg_B`, with two dated variants of the background
databases and a `temporal_distribution` on the `bg_A → bg_B` exchange.

1. **Equivalence / regression** — `traverse_background=True` with **no** background TDs
   produces a timeline and LCA score identical to `traverse_background=False`.
2. **Spread** — with the `bg_A → bg_B` TD, `traverse_background=True` spreads `bg_B`
   across the variants per the TD; assert against a hand-computed expected
   inventory/score.
3. **No double-count** — the traversed `bg_A` contributes only its explicit child edges,
   not its full static inventory (assert the inventory matches the explicit-edge sum,
   not double).
4. **Per-variant TD** — a TD defined differently in `V_2020` vs `V_2030` produces
   different spreads depending on which variant the branch is in.
5. **Both modes** — parametrize tests across `graph_traversal in {"priority", "bfs"}`.

Match existing fixture conventions in `tests/fixtures` + `tests/conftest.py`
(see `tests/test_process_at_base_database_time.py` as a template).

## Out of scope (v1)

- Exact priority-mode subgraph scoring for hopped variants (approximation accepted).
- Co-production / multi-output background processes along descended paths beyond what
  existing code already supports.
