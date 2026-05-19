# Implementation plan: explicit `process` + `product` node support

| Field | Value |
| --- | --- |
| Status | Proposed (not yet scheduled) |
| Audience | `bw_timex` maintainers and contributors |
| Scope | Add full support for Brightway's explicit `process` / `product` node paradigm, including `temporal_distribution` on production-typed output edges |
| Related issues | TBD (file when prioritised) |
| Related notebook | `notebooks/example_electric_vehicle_fleet.ipynb` (motivating use case) |

This document captures the analysis behind that scope and the concrete changes needed in `bw_timex` (and one upstream dependency). It is intended to be read end-to-end before any of the changes below are picked up; the order, rationale and out-of-scope items matter.

## 1. Motivation

`bw_timex` distributes exchanges in time via `TemporalDistribution` (TD) attached to *technosphere* edges. When a model needs to inject a *cohort* distribution — i.e., a temporal pattern on the **output** of an activity rather than on any single input — the only working pattern today is to insert an aggregator node:

```text
FU = {fleet_service: 1}
fleet_service ──[TD = cohort distribution]──▶ fleet_driving
                                               ├──[TD = age survival]──▶ electricity
                                               └──[TD = age retirement PDF]──▶ used_ev
```

This works (it is the pattern used in `example_electric_vehicle_fleet.ipynb`), but the `fleet_service` node is purely structural: it exists to give the cohort TD a technosphere edge to live on. In Brightway's explicit paradigm — `process` and `product` as separate nodes connected by an off-diagonal output edge — the cohort TD has a natural home on the `process → product` edge, and the aggregator disappears:

```text
FU = {fleet_driving_product: 1}
fleet_driving_process ──[output edge, type="production", TD = cohort]──▶ fleet_driving_product
fleet_driving_process ←──[input edge,  type="technosphere",  TD = age]── electricity_product
fleet_driving_process ←──[input edge,  type="technosphere",  TD = retirement]── used_ev_product
```

Same math, three nodes instead of four, and the topology says what it means. The barrier today is that `bw_timex`'s graph traversal silently fails on this paradigm; the aim of this plan is to remove that barrier.

## 2. Background: how Brightway represents activities

`bw2data ≥ 4.0` defines two complementary node-type families (`bw2data/configuration.py`):

```python
process_node_types  = ["process", "processwithreferenceproduct"]
product_node_types  = ["product"]
chimaera_node_default = "processwithreferenceproduct"
```

Two relevant paradigms emerge:

1. **Chimaera (default).** A single `processwithreferenceproduct` node represents both the activity and its reference product. The technosphere matrix has one column per node and one row per node; each chimaera contributes one diagonal entry (the production self-loop) and zero or more off-diagonal entries (technosphere consumption from other chimaeras).
2. **Explicit.** Two distinct nodes — a `process` and a `product` — connected by a `type="production"` exchange. Process columns and product rows are decoupled; the production exchange is a real off-diagonal entry whose `amount` is the output coefficient. Multi-product processes (co-production) are expressible by adding more `process → product` output edges from the same process.

The chimaera paradigm is by far the most common in published Brightway practice; explicit nodes appear in input-output and SUT-style models, in some `bonsai`-style supply-chain models, and in any modelling style that wants to attach TDs (or other metadata) to the act of producing the reference product.

`bw2calc.LCA` accepts both paradigms: it requires the FU to be a **product** (`"LCA can only be performed on products, not activities"`), but the producer can be either a chimaera or an explicit process. Static LCAs over explicit graphs work today.

## 3. Current `bw_timex` behaviour on explicit graphs

A minimal repro (electricity-only model, FU = `{fleet_driving_product: 1}`, cohort TD on the `process → product` output edge, age TD on the electricity input):

```python
import bw2calc as bc
bc.LCA({fd_product: 1}, method).lci();  bc.LCA(...).lcia()
# → static_score = 5.0 (correct)

from bw_timex import TimexLCA
tlca = TimexLCA({fd_product: 1}, method, database_dates)
tlca.build_timeline(starting_datetime=datetime(2030,1,1), temporal_grouping="year")
# → timeline rows: 1
#    only the FU placeholder, no supply chain walk
tlca.lci()
# → KeyError on the product id (not in demand_timing / activity_time_mapping)
```

Three independent failures stack up:

1. **`bw_graph_tools.matrix_tools.gpe_second_heuristic` crashes on NumPy 2** because of a `np.in1d` call (removed in NumPy 2.0; replaced by `np.isin`). This heuristic only fires when the production-exchange diagonal is ambiguous, which is exactly the explicit-paradigm case. A `np.in1d = np.isin` shim unblocks this layer but is *not* a fix.
2. **The graph traversal terminates at the demanded product.** Both the `EdgeExtractor` (priority traversal, inheriting from `bw_temporalis.TemporalisLCA`) and the BFS variant (`EdgeExtractorBFS._get_technosphere_inputs` in `bw_timex/edge_extractor.py:390`) assume `dicts.product[activity_id]` resolves — i.e., that each tech-matrix column corresponds to a single product on the diagonal. With explicit nodes, `dicts.product.get(process_id)` is `None`, and:
   - The "skip self-loop" filter (`if row_idx == product_idx: continue`) becomes a no-op, so the *output* row is treated as just another input with the wrong sign.
   - With a `Product` as FU, `bw_graph_tools` does not perform the bipartite step "find producing process for this product, then descend into its inputs."
3. **`prepare_bw_timex_inputs` fails** because the demanded `Product` was never registered in `demand_timing` / `activity_time_mapping` (the traversal didn't reach it). This surfaces as a `KeyError` in `timex_lca.py:1133`.

In addition, the matrix-modifier layer assumes chimaera-shaped activities:

- `edge_extractor.py:178` — `node.reference_product_production_amount`
- `matrix_modifier.py:223` — `self.nodes[row.producer].rp_exchange().amount` (FU branch)
- `matrix_modifier.py:236` — `self.nodes[row.consumer].rp_exchange().amount` (general branch)
- `matrix_modifier.py:314-321` — already special-cases `IOTableActivity`; the explicit `process` case must be added alongside.

For a pure `process` node, `rp_exchange()` returns `None` or raises depending on the bw2data branch; either way the call site needs to look up the production-typed output edge to the reference product instead.

## 4. Theoretical model: TD on the production output edge

Notation: process column `P`, product row `Q`, output coefficient `α` (entry of the tech matrix at row `Q`, column `P`), input coefficient `β` from another product `R` (row `R`, column `P`).

Static LCA: scaling factor for `P` to satisfy `{Q: 1}` = `1/α`. Each unit of `P` consumes `β/α` of `R`.

Now decorate the output edge with a cohort TD `{c_i: w_i}` (offsets relative to the FU date, weights summing to 1) and the input edge with an age TD `{a_j: v_j}` (offsets relative to each cohort instance):

- Each cohort `c_i` invokes `P` at scale `w_i / α`.
- For each cohort instance, the input from `R` is consumed at age `a_j` with weight `v_j × β × w_i / α` at calendar year `c_i + a_j`.
- `date_consumer` of the input edge is the cohort year `c_i` (because the consumer-side date is the date of the producing process invocation).

This is exactly the cohort × age decomposition we want from the `fleet_service` intermediary today, with two architectural simplifications:

1. The cohort TD lives on a single edge of `fleet_driving_process`, not on a foreign edge from a structural placeholder. The model reads as the system that exists.
2. `temporal_evolution_factors` with `reference="consumer"` evaluates at `c_i` directly — vintage locking is the natural reading, not a thing one has to argue for.

The output-edge TD is a *first-class temporal modifier on the production decision*, not a workaround. This is the abstraction we want long-term.

## 5. Required changes

Ordered shallow → deep. Each phase is independently testable and ships value on its own.

### Phase 0 — unblock `bw_graph_tools` on NumPy 2

**Type:** upstream / dependency hygiene. Not strictly part of this plan, but blocks anyone trying explicit graphs today.

- Either pin `bw_graph_tools` to a version known to work, or upstream a one-line patch replacing `np.in1d` with `np.isin` in `bw_graph_tools/matrix_tools.py:132` (and any other call sites). The latter is preferable.
- No new test in `bw_timex` until phase 1 is in; the failure mode is masked by an `AttributeError` on import paths users don't normally hit.

### Phase 1 — replace chimaera-only production-amount lookups with a paradigm-aware helper

**Files:** `bw_timex/edge_extractor.py`, `bw_timex/matrix_modifier.py`.
**Effort:** ~half a day, mostly mechanical.
**User-visible behaviour:** static LCAs over explicit graphs (single-product processes only) start to produce sensible *base_lca* results inside `TimexLCA`, even before any traversal changes. Multi-output / cohort-TD use cases still won't work; that's later phases.

Add one helper:

```python
# bw_timex/utils.py (new function)
def get_reference_product_production_amount(node, *, reference_product=None):
    """Return the production amount for `node`'s reference product, paradigm-agnostic.

    - chimaera (`processwithreferenceproduct`): self-loop production exchange amount.
    - explicit `process`: amount of the production-typed output edge into the
      requested `reference_product` (or, if the process has exactly one
      production output, that one).
    - explicit `product`: production amount of the producing process for this
      product; raises if multiple producers exist (caller must disambiguate).
    - `IOTableActivity`: existing branch, kept.
    """
```

Replace these call sites with the helper (preserving the existing `IOTableActivity` special-case):

- `edge_extractor.py:178` (priority traversal) — `node.reference_product_production_amount`
- `edge_extractor.py:447` (BFS traversal, `_get_production_amount`) — `tech_matrix[product_idx, col_idx]`
- `matrix_modifier.py:223` (FU branch)
- `matrix_modifier.py:236, 324` (general branches)

`_get_technosphere_inputs` (`edge_extractor.py:390`) needs to learn about explicit nodes too: for an explicit `process` column, the *output* row carries a positive amount and is **not** an input. The current `if row_idx == product_idx: continue` heuristic must be generalised to "skip any row connected by a `type="production"` exchange to this column," not "skip the diagonal."

**Tests to add (`tests/test_explicit_paradigm.py`):**

- A four-node fixture: one explicit `process` + `product` pair in the foreground and one chimaera background process. Static `TimexLCA(...).base_lca.score` matches `bw2calc.LCA(...).score`.
- A multi-output process fixture (one process, two products with different output amounts) where the FU asks for product A and product B respectively. Each demand gives the correct static score; the helper picks the right `α` per product.

### Phase 2 — bipartite supply-chain traversal

**Files:** `bw_timex/edge_extractor.py` (priority + BFS variants); coordinated with `bw_temporalis` if the change is upstreamed.
**Effort:** several days. The substantive work in this plan.
**User-visible behaviour:** explicit graphs produce non-trivial timelines; supply chain is fully walked; demand-timing is populated.

The traversal needs three concrete behaviours added:

1. **From a Product node, find producing Process(es).** Look up the product's row in the tech matrix; for each negative entry (sign convention: production rows are negative in BW), the column index is a producing process. Most foregrounds will have exactly one producer per product; the API needs to handle (and warn? error? attribute-resolve?) the multi-producer case, mirroring how `bw2calc` does activity selection. *Open question — see §7.*
2. **From a Process node, walk both directions.** Output edges (`type="production"`, the off-diagonal positive entries in the process column) carry the production amount and may carry a `temporal_distribution`. Input edges (`type="technosphere"`, off-diagonal negative entries) are the existing case. The `Edge` dataclass already has `edge_type`; the convolution needs to fire correctly for *both* directions.
3. **Timeline rows for output edges.** Today only one synthetic FU edge has `edge_type="production"` (the placeholder created in `edge_extractor.py:431`). Real production output edges need their own rows in the timeline, with `producer = product_id`, `consumer = process_id`, and `td_producer` carrying the cohort TD. Downstream input rows then inherit `abs_td_consumer` from this row's `abs_td_producer`, exactly as they inherit from technosphere rows today.

Two variants need parallel changes (the priority `EdgeExtractor` inheriting from `TemporalisLCA`, and the BFS `EdgeExtractorBFS`). Either keep the variants in step manually (current convention) or refactor the shared logic into a paradigm-aware mixin first; we recommend the latter, in a small refactor PR before Phase 2's main work.

Recommended approach for *where* the bipartite walk lives:

- **Preferred — push it upstream into `bw_graph_tools`/`bw_temporalis`.** Both libraries already have to detect explicit production exchanges (`bw_graph_tools.matrix_tools.guess_production_exchanges`); extending that detection to drive the traversal is in-scope upstream and benefits other consumers. `bw_timex` then receives the correctly-walked edge graph and only needs Phase 3 work below.
- **Fallback — pre-process in `bw_timex`.** Before invoking `TemporalisLCA`, synthesise a chimaera-shaped view of the technosphere (one virtual chimaera per `(process, product)` pair, inputs union'd from the process's input edges, self-loop production amount equal to the output edge's `α`). The TD on the output edge becomes a TD on the synthetic self-loop — but production-self-loop TDs are silently ignored today (see §6, *known footgun*), so this fallback only buys backward-compat for *static* explicit graphs, not the cohort-injection use case. Therefore: viable as a stopgap for Phase 1 testing only.

**Tests to add:**

- The repro from §3: explicit `fleet_driving_process / fleet_driving_product` with cohort TD on the output edge and age TD on the electricity input. Timeline has the expected `cohort × age` row count; per-row dates and amounts match the `cohort_TD ⊛ age_TD` convolution; `date_consumer` of every input row is the cohort year.
- Same fixture as Phase 1 multi-output; assert that the timeline correctly attributes inputs across products by output coefficient.
- Direct port of the cohort+age fleet model (the `fleet_service` notebook), refactored to explicit paradigm. Total static and dynamic scores match the chimaera version within numerical tolerance.

### Phase 3 — honour TDs on production-typed output edges

**Files:** `bw_timex/edge_extractor.py` (convolution), `bw_timex/matrix_modifier.py`, `bw_timex/timeline_builder.py`.
**Effort:** smaller than Phase 2 if the traversal is correct; the math is a sign-flip away from the input case. Mostly tests + matrix-side correctness.

Once Phase 2 produces timeline rows for production output edges, the matrix-modifier layer needs to interpret `temporal_distribution` on them correctly:

- The output edge's role in the matrix is to set the production coefficient `α` at the time-mapped process instance. The time mapping must place one process column per cohort year, each with its own `α` (potentially equal, since the TD weights normalise to 1). The cohort-share scaling enters through the `td_producer.amount` on the output row.
- `temporal_evolution_factors` semantics: `reference="producer"` evaluates at the production calendar date (cohort year, since that's when the process is invoked). `reference="consumer"` evaluates at the consumer (the FU, in our use case) date. For most cohort use cases the relevant reference is `consumer`, applied via the *input* edge from the process to a downstream product — same as today. Output-edge `temporal_evolution_factors` would represent something like "how the act of producing changes over time" — semantically valid but rare; we should support it but it's a niche.

**Tests:** vintage-locked efficiency test on the explicit fleet model, mirroring the existing isolation test in the notebook. Verify the per-cohort-year electricity factor lands at the right rows.

### Phase 4 — surface the abstraction and clean up

**Files:** `bw_timex/utils.py` (`add_temporal_distribution_to_exchange`), `bw_timex/validation.py`, `docs/`.
**Effort:** half a day.

- Add explicit checks in `add_temporal_distribution_to_exchange` that warn if a TD is being attached to a chimaera self-production edge: under the current implementation it is *silently ignored* (this is the §6 footgun). The same TD on an explicit `process → product` output edge **does** fire after Phases 1-3 — message should explain the difference.
- Add a "Modeling paradigms and TD placement" section to the docs decision tree (`docs/content/decisiontree.md`) covering the three patterns explored in the fleet notebook (intermediary, self-loop, explicit) and which one to use when.
- Optional but valuable: support `TemporalDistribution` as an FU-dict value (`{product: TD(1)}`) by translating it into a synthetic intermediary at construction time. Closes the "inject a cohort TD without restructuring the foreground" gap. Validation currently rejects it (`validation.py`); change to accept and dispatch to the synthesizer.

## 6. Known footgun: silent ignore of TDs on chimaera production self-loops

Independent of explicit-node support, the chimaera paradigm has a quiet failure mode that we should fix in passing.

A `temporal_distribution` set on a chimaera's self-production exchange (`fleet_driving → fleet_driving`, `type="production"`) is silently dropped by the traversal — `edge_mapping[node.unique_id]` only includes consumption edges into the node, not the diagonal self-loop. Empirically:

| Variant | Outcome |
| --- | --- |
| Cohort TD on `fleet_service → fleet_driving` (intermediary) | Honoured. 13 timeline rows. |
| Cohort TD on `fleet_driving → fleet_driving` (self-loop) | Silently ignored. 4 timeline rows. Wrong dynamic timing; *correct static total* by accident. |
| Cohort TD as `{fleet_driving: TD(1)}` in FU dict | `ValidationError: demand values must be numeric`. |

The static total being correct masks the bug — a user doing development with `tlca.static_score` will see the expected number, only finding the problem later when `dynamic_lcia` or `temporal_evolution_reference="consumer"` reveals the missing cohort spread.

Recommended fix in Phase 4:

- `bw_timex/utils.py:add_temporal_distribution_to_exchange` — if the target is a chimaera's production self-loop, raise a `NotImplementedError` (or warn loudly) pointing the user at the intermediary or explicit pattern.
- `bw_timex/validation.py` — same check via the structured-input validator path.

Doing this without explicit-paradigm support is fine: the warning still applies once explicit support lands (the explicit *output* edge is a different beast and remains valid).

## 7. Open design questions

These need a decision before Phase 2 ships and should be collected into the issue when filed.

1. **Multi-producer products.** When a foreground product has more than one producing process (e.g., `electricity_product` produced by both gas and wind processes in the foreground), how does `bw_timex` resolve the producer for traversal? Options:
   - Replicate `bw2calc` behaviour (which uses `dicts.product` to pick by `(database, code)` mapping; in practice ambiguity is rare because foreground graphs are usually injective).
   - Aggregate via temporal-market-shares (analogous to the existing background interpolation code). This is the more powerful answer but requires a foreground-side market-share concept that doesn't exist today.
2. **Co-products.** A single process with multiple output edges (each carrying its own `α` and possibly its own TD). Each TD must be honoured per-product, which means per-product time-mapping of the *same* process column. Doable but it complicates `activity_time_mapping` (today: one entry per `(activity, time)`; needed: one per `(activity, product, time)`).
3. **TD type compatibility.** Should we constrain `temporal_distribution` on production output edges to *datetime* TDs (so the cohort dimension is calendar-time) versus *timedelta* TDs (relative to the consumer)? The current code accepts both forms; for cohort use cases datetime TDs are usually wanted, but enforcing it would complicate the FU-relative anchoring.
4. **Backward compatibility.** Phases 1-3 should be additive — chimaera workflows must continue to work exactly as they do today. The Phase 4 footgun fix changes one previously-silent path into a loud failure. Should it be opt-in via a config flag for one minor version, or hard-error immediately?

## 8. Out of scope

- **Substitution exchanges (`type="substitution"`)** beyond what's already supported. The convolution math is the same, but interaction with co-products + TDs deserves its own design pass.
- **Non-foreground explicit graphs.** Background databases can be in either paradigm independently of the foreground; this plan implicitly assumes the foreground is the explicit one. Phase 1's helper should still work on background nodes (it's paradigm-agnostic by design), but full traversal of explicit *backgrounds* is not a goal here — backgrounds are leaves and are not traversed beyond temporal-market-share lookup.
- **`prospective_*` waterfall plumbing.** The fleet notebook's prospective comparison (`example_electric_vehicle_fleet.ipynb`, cell 74) walks `fleet_driving.technosphere()` and copies activities. This walk needs to be paradigm-aware too, but is a notebook concern more than a `bw_timex` API concern — leave for a notebook update once Phases 1-3 are usable.

## 9. Estimated total effort

| Phase | Effort | Risk |
| --- | --- | --- |
| 0 — `np.in1d` upstream patch | <1h | None |
| 1 — paradigm-aware production-amount helper | 0.5d | Low |
| 2 — bipartite traversal | 3-5d | Medium-high (touches upstream `bw_graph_tools` / `bw_temporalis`) |
| 3 — honour TDs on output edges | 1-2d | Medium |
| 4 — surface + footgun fix + docs | 0.5d | Low |

Total: roughly one to two weeks of focused work, plus review cycles. Phase 1 is a sensible first PR; it ships small visible value (static scoring works on explicit graphs) and forces the helper abstraction that Phases 2-3 will lean on.

## 10. References

- `notebooks/example_electric_vehicle_fleet.ipynb` — current fleet model using the chimaera + intermediary pattern.
- `bw2data/configuration.py` — node-type taxonomy.
- `bw_graph_tools/matrix_tools.py` — production-exchange detection heuristics.
- `bw_temporalis/lca.py:115` — supply-chain traversal entry point.
- Empirical scripts used during this analysis (kept under `/tmp/` during exploration; reproduce by setting up a minimal explicit-paradigm foreground and demanding the product as FU).
