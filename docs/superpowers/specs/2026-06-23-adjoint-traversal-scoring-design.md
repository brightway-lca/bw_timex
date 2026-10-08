# Adjoint Static-Score Intensities for Traversal Scoring (P1)

**Date:** 2026-06-23
**Status:** Design — approved, pending spec review
**Scope:** First spec of a multi-cycle effort to bring `trails` performance/UX learnings
into `bw_timex` while keeping `bw_timex`'s existing logic intact.

## Background

`bw_timex` builds a time-explicit LCI by (1) a graph traversal that extracts a
timeline of temporally-distributed edges, then (2) expanding/solving matrices.
The **graph traversal is the dominant cost**, not the solve.

The priority traversal path is:

```
TimexLCA.build_timeline()
  -> TimelineBuilder -> EdgeExtractor (bw_timex)
       inherits bw_temporalis.TemporalisLCA
         uses bw_graph_tools.NewNodeEachVisitGraphTraversal
```

`NewNodeEachVisitGraphTraversal` scores each visited node with a **per-node-visit
linear solve** (a `CachingSolver`: the technosphere is factorized once, but a
back-substitution runs for every node visit; "new node each visit" means the same
activity reached via N paths is solved N times). For deep/wide supply chains this
per-visit solve count dominates traversal wall time. The heap priority and the
cutoff both derive from these solved `cumulative_score` values
(`edge_extractor.py` pushes `1 / node.cumulative_score`).

`trails` avoids this entirely. Its `StaticActivityScores._compute_static_activity_scores`
solves the **adjoint** system `A.T x = B.T c` — one sparse solve per LCIA method —
yielding the static score intensity for **every** activity at once. Routing then
prunes/orders branches with a pure lookup (`|intensity[act]| * demand`), doing
**zero linear solves during traversal**.

## Goal

Eliminate the per-node-visit linear solve in `bw_timex`'s priority traversal by
replacing the **source** of node scores with a precomputed adjoint intensity
vector. The traversal structure — priority heap, `cutoff`, `max_calc`, temporal
convolution, variant/background descent — is unchanged. Only how a node's score
is obtained changes: from "solve a linear system" to "look up `intensity[act]`".

Non-goals (deferred to later specs): relative score-potential cutoff (P2),
persistent disk cache (P3), public-API/UX changes, premise Frictionless
datapackage ingestion.

## Core math

For demand `d`: supply `s = A^-1 d`, inventory `g = B s`, score `= h^T g` where
`h` is the characterization (CF) vector for the method.

Define the adjoint vector `λ` by `A^T λ = B^T h`. Then `score = λ^T d`, and
`λ[a]` is the **static downstream score per unit of activity `a`'s reference
product**. This is exactly the quantity the priority heap needs for ordering and
the cutoff needs for pruning. One sparse solve per method computes `λ` for all
activities.

`base_lca` (already built in `TimexLCA.__init__`, includes the full background)
provides `A` (`technosphere_matrix`) and `B` (`biosphere_matrix`); the method
provides `h` (from `characterization_matrix` / CF data).

## Components

### 1. `StaticScoreIntensities` (new, isolated unit)

Mirrors `trails.static_activity_scores.StaticActivityScores`, adapted to
`bw_timex`/`bw2calc` index spaces.

- **Input:** a built `bw2calc.LCA` (the existing `base_lca`) and the method.
- **Compute:** `h` from the method's CFs; `λ = spsolve(A.T, B.T @ h)`.
- **Expose:**
  - `intensity[activity_matrix_index] -> float` (signed, retained for diagnostics)
  - an absolute-valued array for pruning/ordering (precomputed once;
    `nan_to_num` + `abs`, per the trails optimization for hot lookups)
  - a mapping helper from `bw_graph_tools`/`TemporalisLCA` node identity to the
    activity index used by `λ` (the index-space bridge is the main correctness
    detail — see Risks).
- **Properties:** pure and deterministic; no traversal state. Independently
  unit-testable: for any single-activity demand `d = e_a`, `λ[a]` must equal the
  full static LCA score of that demand to numerical tolerance.

### 2. Integration seam — Approach A (chosen): inject adjoint scoring into the priority engine

Provide a custom scoring object to `bw_graph_tools`'s traversal so that a node's
`cumulative_score` is computed as `λ[act] * supply_amount` (a lookup), instead of
a back-substitution. The seam:

- `TemporalisLCA` accepts a `graph_traversal` subclass; `NewNodeEachVisitGraphTraversal`
  uses a `caching_solver` and calls `set_score_row(...)`. We subclass/replace the
  solver (or the traversal's scoring step) so `scores(...)` returns adjoint-based
  potentials with **no per-node solve**.
- `EdgeExtractor` (bw_timex) wires the `StaticScoreIntensities` into this custom
  solver/subclass at construction. Everything downstream in
  `build_edge_timeline` (heap, `1 / node.cumulative_score`, cutoff vs
  `cutoff_score`, convolution, variant descent) is untouched.
- Gated/opt-in initially: a flag on `build_timeline` (e.g.
  `graph_traversal="priority"` keeps current behavior; a new value or a boolean
  selects adjoint scoring) so the old path remains available for comparison and
  fallback. Default-switch decision deferred until the validation gate passes.

`cumulative_score` semantics shift from "per-visit solved subtree score" to
"static adjoint intensity × supply amount". For ordering and cutoff this is the
correct potential and is what `trails` uses; the guardrails below ensure pruning
stays conservative.

### 3. Validation harness (acceptance gate)

Because upfront profiling was intentionally skipped, the design carries its own
measurement gate (a script/notebook, not a shipped feature):

- Run an existing real example model (from `notebooks/`) both ways: current
  priority engine vs P1 adjoint scoring.
- Record: wall time, node-visit / solve count, final scores, and the timeline
  DataFrame.
- Pass criteria are the Correctness guardrails plus a demonstrated reduction in
  traversal time / solve count.

## Correctness guardrails (numeric-tolerance gate, as chosen)

1. **Score equivalence:** `λ[a] · d` matches the full static LCA score for
   single-activity demands within a small `rtol` (e.g. `1e-9`), and the
   end-to-end time-explicit scores from a P1 run match the current priority-engine
   run within a small `rtol` on the example models.
2. **Conservative pruning:** with identical `cutoff` / `max_calc`, P1 must not
   silently drop a branch that the current engine retains above the cutoff. If
   adjoint potential and per-visit score diverge, prefer the more inclusive
   decision (do not under-explore). Documented and tested.
3. **Timeline consistency:** the resulting timeline (edges, amounts, dates)
   matches the current engine within tolerance on the example models when
   `cutoff` / `max_calc` are unchanged (numeric tolerance, not byte-equality).

## Risks / open details

- **Index-space bridge.** `λ` is indexed by technosphere matrix columns;
  `bw_graph_tools`/`TemporalisLCA` nodes carry their own ids. The mapping between
  node identity and the `λ` activity index is the primary correctness-sensitive
  piece and must be unit-tested directly. (`edge_extractor.py` already navigates
  `lca.dicts.product.reversed` and `activity_datapackage_id`; reuse those.)
- **`bw_graph_tools` coupling.** Approach A subclasses library internals
  (`caching_solver` / scoring). Pin behavior with tests; keep the override
  surface minimal and the old path selectable as fallback.
- **Sign / substitution edges.** `λ` must follow the same sign conventions the
  traversal already applies (production vs technosphere vs substitution). Verify
  against `adjust_sign_of_amount_based_on_edge_type` semantics.
- **Multiple methods.** Initial scope targets the single configured
  `TimexLCA.method`; one adjoint solve. Multi-method potential is a later concern.

## Deliverables

1. `StaticScoreIntensities` unit + tests (math/equivalence, index bridge, signs).
2. Custom adjoint-scoring solver/subclass wired into `EdgeExtractor`, behind an
   opt-in flag on `build_timeline`.
3. Validation harness + recorded before/after results on an example model.
4. Tests asserting the three correctness guardrails on example models.

## Follow-on specs (not this cycle)

- P2: relative score-potential cutoff (adaptive routing) built on these intensities.
- P3: persistent, fingerprinted disk cache for the intensities.
- UX: curated public API, adaptive-by-default routing, routed-graph Sankey.
- premise: Frictionless datapackage ingestion adapter.
