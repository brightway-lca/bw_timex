# Background Temporal Distributions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let temporal distributions defined on exchanges *inside* background databases take effect, by optionally descending into the background during traversal and sourcing time-spread flows from the temporally-appropriate background-db variants.

**Architecture:** A new opt-in flag `traverse_background` on `TimexLCA.build_timeline` removes the static-background traversal stop. When set, both the priority (`EdgeExtractor`) and BFS (`EdgeExtractorBFS`) traversals descend into background dbs, propagate background TDs by convolution, and at background-entry + TD-carrying edges split a producer into its date-interpolated variants — reading each branch's exchanges/amounts/TDs from the *respective* variant db (via `interdatabase_activity_mapping` + bw2data). Downstream, the temporal-market-vs-temporalized decision keys on the presence of `temporal_market_shares` (leaf ⇒ market) instead of database membership, so traversed background processes are temporalized rather than double-counted.

**Tech Stack:** Python, bw2data, bw2calc, bw_temporalis, numpy, pandas, pydantic, pytest.

## Global Constraints

- Default behavior MUST be unchanged: `traverse_background=False` is the default and every existing test must keep passing.
- TDD: write the failing test first, confirm it fails, then implement. (Project rule.)
- No Claude attribution in commit messages; no `Co-Authored-By` trailer. (Project rule.)
- Do not filter or mention the bw2calc scikit-umfpack `UserWarning`. (Project rule.)
- Both `graph_traversal="priority"` and `graph_traversal="bfs"` must support `traverse_background`.
- The `interpolation_type` values are `"linear"` and `"nearest"` (`"closest"` normalizes to `"nearest"`).
- Run a single test with: `pytest tests/<file>::<test> -v` (use `-p no:cacheprovider` not required).

---

### Task 1: Plumb the `traverse_background` flag (no behavior change)

Adds the flag to validation, `build_timeline`, `TimelineBuilder`, and both extractor constructors. The flag is accepted and stored but not yet used, so behavior is identical.

**Files:**
- Modify: `bw_timex/validation.py:89-100` (`BuildTimelineInputs`)
- Modify: `bw_timex/timex_lca.py:205-346` (`TimexLCA.build_timeline`)
- Modify: `bw_timex/timeline_builder.py:30-128` (`TimelineBuilder.__init__`)
- Modify: `bw_timex/edge_extractor.py:88-112` (`EdgeExtractor.__init__`)
- Modify: `bw_timex/edge_extractor.py:381-411` (`EdgeExtractorBFS.__init__`)
- Test: `tests/test_timeline_builder.py`

**Interfaces:**
- Produces: `TimexLCA.build_timeline(..., traverse_background: bool = False)`; `TimelineBuilder(..., traverse_background: bool = False)`; `EdgeExtractor(..., traverse_background: bool = False)`; `EdgeExtractorBFS(..., traverse_background: bool = False)`. Each stores `self.traverse_background`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_timeline_builder.py` (use the existing `process_at_base_database_time_db` fixture already imported via conftest):

```python
def test_traverse_background_flag_accepted_and_stored(process_at_base_database_time_db):
    from bw_timex import TimexLCA

    method = ("GWP", "example")
    database_dates = {
        "background_2020": datetime.strptime("2020", "%Y"),
        "background_2030": datetime.strptime("2030", "%Y"),
        "foreground": "dynamic",
    }
    tlca = TimexLCA({("foreground", "fu"): 1}, method, database_dates)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    assert tlca.timeline_builder.traverse_background is False
```

Ensure `from datetime import datetime` is present at the top of the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_timeline_builder.py::test_traverse_background_flag_accepted_and_stored -v`
Expected: FAIL — `build_timeline() got an unexpected keyword argument 'traverse_background'`.

- [ ] **Step 3: Implement the plumbing**

In `bw_timex/validation.py`, add to `BuildTimelineInputs` (after `graph_traversal`):

```python
    traverse_background: bool = False
```

In `bw_timex/timex_lca.py` `build_timeline` signature (after `graph_traversal: str = "priority",`):

```python
        traverse_background: bool = False,
```

Pass it into the validator call:

```python
        validated = BuildTimelineInputs(
            starting_datetime=starting_datetime,
            temporal_grouping=temporal_grouping,
            interpolation_type=interpolation_type,
            edge_filter_function=edge_filter_function,
            cutoff=cutoff,
            max_calc=max_calc,
            graph_traversal=graph_traversal,
            traverse_background=traverse_background,
        )
```

Add `traverse_background` to the `timeline_cache_key` tuple (so a changed flag invalidates the cache):

```python
        timeline_cache_key = (
            str(validated.starting_datetime),
            temporal_grouping,
            interpolation_type,
            cutoff,
            max_calc,
            graph_traversal,
            traverse_background,
        )
```

Pass it to the `TimelineBuilder(...)` construction (add as a keyword near `graph_traversal=graph_traversal,`):

```python
            graph_traversal=graph_traversal,
            traverse_background=traverse_background,
```

In `bw_timex/timeline_builder.py` `TimelineBuilder.__init__`, add `traverse_background: bool = False,` to the signature (before `*args,`) and store `self.traverse_background = traverse_background` near the other `self.* =` assignments. Pass it into BOTH extractor constructions:

```python
        if graph_traversal == "bfs":
            self.edge_extractor = EdgeExtractorBFS(
                lca_object=base_lca,
                starting_datetime=self.starting_datetime,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                static_activity_indices=set(static_background_activity_ids),
                nodes=self.nodes,
                traverse_background=self.traverse_background,
            )
        elif graph_traversal == "priority":
            self.edge_extractor = EdgeExtractor(
                base_lca,
                starting_datetime=self.starting_datetime,
                *args,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                max_calc=self.max_calc,
                static_activity_indices=set(static_background_activity_ids),
                traverse_background=self.traverse_background,
                **kwargs,
            )
```

In `bw_timex/edge_extractor.py` `EdgeExtractor.__init__`, accept and store the flag without passing it to `super().__init__` (TemporalisLCA does not know it):

```python
    def __init__(
        self, *args, edge_filter_function: Callable = None,
        traverse_background: bool = False, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.traverse_background = traverse_background
        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False
```

In `bw_timex/edge_extractor.py` `EdgeExtractorBFS.__init__`, add `traverse_background: bool = False,` to the signature and store `self.traverse_background = traverse_background`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_timeline_builder.py::test_traverse_background_flag_accepted_and_stored -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `pytest -q`
Expected: all pass (flag is unused).

- [ ] **Step 6: Commit**

```bash
git add bw_timex/validation.py bw_timex/timex_lca.py bw_timex/timeline_builder.py bw_timex/edge_extractor.py tests/test_timeline_builder.py
git commit -m "Add traverse_background flag plumbing"
```

---

### Task 2: Background-chain multi-variant test fixture

A fixture with a two-step background chain (`bg_A → bg_B`) in two dated variants, with a `temporal_distribution` on the `bg_A → bg_B` exchange. Used by all later behavior tests.

**Files:**
- Create: `tests/fixtures/background_td_db_fixture.py`
- Modify: `tests/conftest.py:1-19` (add import)

**Interfaces:**
- Produces: pytest fixture `background_td_db`. Databases: `foreground`, `background_2020`, `background_2030`, `biosphere`. Activities: `fu` (foreground) → `bg_A` (background_2020, "electricity"-like, amount 2) ; `bg_A` → `bg_B` ("fuel"-like, amount 5) with a TD; `bg_B` → 1 kg CO2. Both variants identical except `bg_A→bg_B` amount and TD may differ. Method `("GWP", "example")`, CF CO2 = 1.

- [ ] **Step 1: Write the fixture (this IS the deliverable; its "test" is being importable + usable)**

Create `tests/fixtures/background_td_db_fixture.py`:

```python
import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_td_db():
    """foreground.fu -> background.bg_A -> background.bg_B -> CO2.

    Two dated background variants (2020, 2030). The bg_A -> bg_B exchange
    carries a temporal_distribution so that, when traversed, bg_B is spread
    over time and sourced from the temporally-appropriate variant.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {
            ("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"},
        }
    )
    node_co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()

    background_2020 = bd.Database("background_2020")
    background_2020.register()
    background_2030 = bd.Database("background_2030")
    background_2030.register()

    # --- foreground ---
    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    # --- build both background variants identically (structure) ---
    # bg_A -> bg_B TD: spread 60% at +0y, 40% at +10y from bg_A's time.
    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    variant_nodes = {}
    for db, dbname in [(background_2020, "background_2020"), (background_2030, "background_2030")]:
        bg_a = db.new_node("bg_A", name="bg_A", unit="kWh")
        bg_a["reference product"] = "bg_A"
        bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="kg")
        bg_b["reference product"] = "bg_B"
        bg_b.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_b.new_edge(input=bg_b, amount=1, type="production").save()

        a_to_b = bg_a.new_edge(input=bg_b, amount=5, type="technosphere")
        a_to_b["temporal_distribution"] = td_a_to_b
        a_to_b.save()

        bg_b.new_edge(input=node_co2, amount=1, type="biosphere").save()
        variant_nodes[dbname] = (bg_a, bg_b)

    # --- foreground -> background_2020.bg_A ---
    bg_a_2020 = variant_nodes["background_2020"][0]
    fu.new_edge(input=bg_a_2020, amount=2, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for dbn in bd.databases:
        bd.Database(dbn).process()

    return variant_nodes
```

- [ ] **Step 2: Register the fixture**

In `tests/conftest.py`, add after the other fixture imports:

```python
from .fixtures.background_td_db_fixture import background_td_db
```

- [ ] **Step 3: Write a smoke test that the fixture loads and a static TimexLCA runs**

Create `tests/test_background_traversal.py`:

```python
from datetime import datetime

import bw2data as bd

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def test_fixture_loads(background_td_db):
    assert "background_2020" in bd.databases
    assert "background_2030" in bd.databases
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    tlca.lci()
    tlca.static_lca.lcia()
    assert tlca.static_lca.score > 0
```

- [ ] **Step 4: Run the smoke test**

Run: `pytest tests/test_background_traversal.py::test_fixture_loads -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/background_td_db_fixture.py tests/conftest.py tests/test_background_traversal.py
git commit -m "Add background-chain multi-variant test fixture"
```

---

### Task 3: Key temporal-market classification on `temporal_market_shares`, not db membership

Switch the three places that decide "temporal market vs temporalized" from database membership to the presence of `temporal_market_shares`. In `traverse_background=False` mode the two are equivalent, so this is behavior-preserving and unlocks correct handling of traversed background processes later.

**Files:**
- Modify: `bw_timex/matrix_modifier.py:271-315` (`process_edge` branch)
- Modify: `bw_timex/timex_lca.py:1452-1468` (`collect_temporalized_processes_from_timeline`)
- Test: `tests/test_background_traversal.py`

**Interfaces:**
- Consumes: timeline rows with column `temporal_market_shares` (dict or `None`), from Task 1.
- Produces: classification rule "row is a temporal market iff `row.temporal_market_shares` is truthy".

- [ ] **Step 1: Write the failing test**

Add to `tests/test_background_traversal.py`:

```python
def test_classification_keys_on_shares_not_db(background_td_db):
    """With traverse_background=False, results are unchanged by the refactor."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2024-01-01", traverse_background=False)
    tlca.lci()
    tlca.dynamic_lcia(metric="GWP")  # or whichever LCIA the suite uses
    # bg_A is the only first-level background producer -> exactly one temporal market.
    assert len(tlca.node_collections["temporal_markets"]) == 1
```

(If `dynamic_lcia` requires extra args in this codebase, replace with `tlca.static_lca.lcia()` after `tlca.lci()`; the assertion on `node_collections["temporal_markets"]` is the point.)

- [ ] **Step 2: Run test to verify current behavior**

Run: `pytest tests/test_background_traversal.py::test_classification_keys_on_shares_not_db -v`
Expected: PASS already (sanity baseline). If it errors on `dynamic_lcia` signature, fix the call, re-run until it passes. This pins the invariant the refactor must preserve.

- [ ] **Step 3: Refactor `matrix_modifier.process_edge`**

In `bw_timex/matrix_modifier.py`, change the branch condition (currently `if previous_producer_node["database"] in self.database_dates_static.keys():`) to:

```python
        # A row is a temporal market iff it carries temporal_market_shares.
        # Leaf producers (background frontier) carry shares; producers traversed
        # into (e.g. background processes descended into with traverse_background)
        # do not, so they are rebuilt as temporalized processes (no double-count).
        is_temporal_market = bool(getattr(row, "temporal_market_shares", None))
        if is_temporal_market:
```

Leave the `else:` temporalized branch as-is.

- [ ] **Step 4: Refactor `collect_temporalized_processes_from_timeline`**

In `bw_timex/timex_lca.py`, replace the db-membership classification loop. Build a set of `time_mapped_producer`s that carry shares from the timeline, then classify:

```python
        market_time_mapped = set(
            self.timeline.loc[
                self.timeline.temporal_market_shares.notnull(),
                "time_mapped_producer",
            ]
        )

        temporal_market_ids = set()
        temporalized_process_ids = set()
        for producer, time_mapped_producer in unique_producers:
            if time_mapped_producer in market_time_mapped:
                temporal_market_ids.add(time_mapped_producer)
            else:
                temporalized_process_ids.add(time_mapped_producer)
```

- [ ] **Step 5: Run the refactor test + full suite**

Run: `pytest tests/test_background_traversal.py::test_classification_keys_on_shares_not_db -v`
Expected: PASS.
Run: `pytest -q`
Expected: all pass (behavior-preserving in `False` mode).

- [ ] **Step 6: Commit**

```bash
git add bw_timex/matrix_modifier.py bw_timex/timex_lca.py tests/test_background_traversal.py
git commit -m "Key temporal-market classification on temporal_market_shares"
```

---

### Task 4: BFS descent into background (equivalence when no background TDs)

When `traverse_background=True`, the BFS extractor stops excluding background nodes and descends, bounded by `cutoff` and a new `max_calc` guard. `build_timeline` assigns `temporal_market_shares` to leaf producers (not in the consumer set) instead of the hardcoded `first_level_background_static`. This task establishes the descent and the invariant: with **no** background TDs, `traverse_background=True` equals `False`.

**Files:**
- Modify: `bw_timex/edge_extractor.py:381-714` (`EdgeExtractorBFS`)
- Modify: `bw_timex/timeline_builder.py:95-128` (static-index gating) and `:422-508` (`add_column_temporal_market_shares_to_timeline`)
- Test: `tests/test_background_traversal.py`

**Interfaces:**
- Consumes: `self.traverse_background` (Task 1), classification-on-shares (Task 3).
- Produces: BFS traversal that descends into background when the flag is set; `EdgeExtractorBFS.__init__` gains `max_calc: int = 1_000_000`. `build_timeline` assigns shares to leaf producers via a new helper `_leaf_background_producers(edges_df)` returning a `set[int]`.

- [ ] **Step 1: Write the failing equivalence test**

Add to `tests/test_background_traversal.py` a helper + test. Use a fixture variant with NO background TD: build it inline by deleting the TD before running.

```python
import numpy as np
from bw_temporalis import TemporalDistribution


def _strip_background_tds():
    """Remove the temporal_distribution on every background bg_A->bg_B exchange."""
    for dbname in ("background_2020", "background_2030"):
        for act in bd.Database(dbname):
            for exc in act.technosphere():
                if "temporal_distribution" in exc:
                    del exc["temporal_distribution"]
                    exc.save()
        bd.Database(dbname).process()


def _score(traverse_background):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=traverse_background,
    )
    tlca.lci()
    tlca.static_lca.lcia()
    return tlca.static_lca.score, tlca.timeline


def test_bfs_equivalence_without_background_tds(background_td_db):
    _strip_background_tds()
    score_static, tl_static = _score(False)
    score_traverse, tl_traverse = _score(True)
    assert score_traverse == pytest.approx(score_static, rel=1e-9)
```

Add `import pytest` at the top of the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_background_traversal.py::test_bfs_equivalence_without_background_tds -v`
Expected: FAIL — with `traverse_background=True` the background is still excluded (flag unused in BFS), so either an exception or a mismatched score. Capture the actual failure.

- [ ] **Step 3: Gate the static exclusion in `TimelineBuilder`**

In `bw_timex/timeline_builder.py`, where `static_background_activity_ids` is computed (lines ~97-101), short-circuit it when traversing:

```python
        if self.traverse_background:
            static_background_activity_ids = set()
        else:
            static_background_activity_ids = {
                node_id
                for node_id in self.node_collections["background"]
                if node_id not in self.node_collections["first_level_background_static"]
            }
```

(Move the assignment of `self.traverse_background` above this block in `__init__`.)

- [ ] **Step 4: Make BFS descend when the flag is set**

In `bw_timex/edge_extractor.py` `EdgeExtractorBFS.__init__`, add `max_calc: int = 1_000_000,` to the signature and `self.max_calc = max_calc`. Initialize a counter `self._calc_count = 0` and, when `traverse_background` is set, neutralize the default leaf filter so background nodes are descended into:

```python
        if self.traverse_background:
            # Background is no longer a hard stop; rely on cutoff / max_calc.
            # A user-supplied edge_filter_function is still respected.
            if edge_filter_function is None:
                self.edge_ff = lambda x: False
```

In `_get_technosphere_inputs`, the `static_activity_indices` set is already empty when traversing (Task 4 Step 3), so deeper background inputs are no longer skipped — no change needed there.

In `build_edge_timeline`, add a `max_calc` guard inside the `while queue:` loop, right after `node_id, td, ... = queue.popleft()`:

```python
            self._calc_count += 1
            if self._calc_count > self.max_calc:
                break
```

- [ ] **Step 5: Assign `temporal_market_shares` to leaf producers**

In `bw_timex/timeline_builder.py`, add a helper method to `TimelineBuilder`:

```python
    def _leaf_background_producers(self, edges_df: pd.DataFrame) -> set:
        """Producers that are leaves (never traversed into) and live in a static
        background db. These are the temporal-market frontier."""
        consumers = set(edges_df["consumer"].unique())
        static_dbs = set(self.database_dates_static.keys())
        leaves = set()
        for producer in edges_df["producer"].unique():
            if producer in consumers:
                continue  # traversed into -> temporalized, not a market
            node = self.nodes.get(producer)
            if node is not None and node["database"] in static_dbs:
                leaves.add(producer)
        return leaves
```

In `add_column_temporal_market_shares_to_timeline`, replace the use of
`self.node_collections["first_level_background_static"]` with the leaf set when
traversing. Compute once near the top of the method:

```python
        if self.traverse_background:
            market_producers = self._leaf_background_producers(tl_df)
        else:
            market_producers = self.node_collections["first_level_background_static"]
```

Then in the final assignment, use `market_producers` in place of
`first_level_background_static`:

```python
        tl_df["temporal_market_shares"] = [
            remapped_interpolation_weights[producer_date]
            if producer in market_producers
            else None
            for producer, producer_date in zip(tl_df["producer"], tl_df["date_producer"])
        ]
```

Note: `add_column_temporal_market_shares_to_timeline` receives `tl_df` after the
`producer`/`date_producer` columns exist, so `_leaf_background_producers` can read
them. Confirm `tl_df` has a `consumer` column at that point (it does — see
`build_timeline` column ordering); if not, pass the pre-grouped frame's consumer
set in instead.

- [ ] **Step 6: Run the equivalence test**

Run: `pytest tests/test_background_traversal.py::test_bfs_equivalence_without_background_tds -v`
Expected: PASS — descending with no background TDs reproduces the static result.

- [ ] **Step 7: Run the full suite**

Run: `pytest -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/edge_extractor.py bw_timex/timeline_builder.py tests/test_background_traversal.py
git commit -m "BFS: descend into background under traverse_background (no-TD equivalence)"
```

---

> **COURSE CORRECTION (2026-06-19, after Task 4).** During Task 5 we verified that
> Tasks 3+4 ALREADY route a background TD whose endpoint is a *leaf* (e.g. the
> emission-only `bg_B` in the shallow `background_td_db` fixture) to the correct
> variants — referenced-variant TD propagation spreads the flow in time and the
> existing leaf `temporal_market_shares` interpolation routes each cohort to the
> right variant by date. The genuinely new work (respective-variant lookup) only
> matters for **deeper** chains: when descent continues *into* a background process
> reached at a shifted date routing to a NON-referenced variant. Tasks 5–7 below are
> therefore executed against a NEW deeper fixture `background_td_deep_db`
> (`bg_A→bg_B→bg_C`, with `bg_B→bg_C` differing per variant) and the corrected,
> self-contained briefs in `.git/sdd/task-5-brief.md`, `task-6-brief.md`,
> `task-7-brief.md`. The numeric expectations and exact code in those briefs
> supersede the original Task 5–7 text below where they differ. The shallow
> `background_td_db` fixture and its leaf-routing behavior remain valid and are kept
> as regression coverage.

### Task 5: BFS variant-aware split at background entry + TD edges

Make the BFS descent split a producer into its date-interpolated variants at background entry and at TD-carrying edges, reading each branch's exchanges/amounts/TDs from the respective variant db. This is the core feature: a background TD spreads its downstream flow across variants.

**Files:**
- Modify: `bw_timex/edge_extractor.py` (`EdgeExtractorBFS.build_edge_timeline` + helpers)
- Modify: `bw_timex/timex_lca.py` (ensure `interdatabase_activity_mapping` is available to the extractor; see Interfaces)
- Test: `tests/test_background_traversal.py`

**Interfaces:**
- Consumes: `interdatabase_activity_mapping` (an `InterDatabaseMapping`, `helper_classes.py:146`) mapping `producer_id -> {db_name: id}`; `database_dates_static` (`dict[str, datetime]`); `self.nodes` (`{id: Activity}`); the interpolation helpers on `TimelineBuilder` (`get_weights_for_interpolation_between_nearest_years`, `find_closest_date`).
- Produces: timeline `Edge`s whose `producer` is the variant-resolved background node, each carrying the accumulated branch share folded into `td_producer.amount`. Leaves resolve to `{resolved_variant_db: 1}` shares downstream.

**Design note on where the split lives:** The cleanest implementation keeps the *amount scaling* (share) in the convolved `distribution`/`td_producer` and emits one `Edge` per (variant, cohort). The branch's variant determines which db's `bg_B` node id is recorded as the `producer`, so the existing `_leaf_background_producers` + `temporal_market_shares = {db: 1}` path wires each branch to its own variant's static inventory.

- [ ] **Step 1: Write the failing spread test**

Add to `tests/test_background_traversal.py`. Hand-computation for the fixture:

- FU demands 2 units of `bg_A` (background_2020) at 2024.
- `bg_A → bg_B` amount 5, TD: 60% at +0y (2024), 40% at +10y (2034).
- So `bg_B` demand = 2 × 5 = 10 kg, split: 6 kg at 2024, 4 kg at 2034.
- `bg_B → CO2` = 1 kg CO2 / kg. Each kg bg_B ⇒ 1 kg CO2.
- 2024 lies between variant dates 2020 and 2030 → linear weights {2020: 0.6, 2030: 0.4}.
- 2034 is above all dates → clamps to nearest variant 2030 → {2030: 1.0}.
- All variants emit the same 1 kg CO2/kg here, so the **total** CO2 = 10 kg
  regardless of variant choice, but the **temporal/variant split** differs.

Assert on the timeline's variant routing rather than only total score (total is
invariant in this symmetric fixture by construction — that is intentional so the
test isolates routing):

```python
def test_bfs_background_td_spreads_bg_b_across_variants(background_td_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tl = tlca.timeline
    # bg_B must now appear as a producer at two distinct producer dates (2024 & 2034)
    bg_b_rows = tl[tl["producer_name"] == "bg_B"]
    producer_years = sorted({d.year for d in bg_b_rows["date_producer"]})
    assert producer_years == [2024, 2034]
    # the 2034 cohort routes entirely to the 2030 variant
    row_2034 = bg_b_rows[[d.year == 2034 for d in bg_b_rows["date_producer"]]].iloc[0]
    assert set(row_2034["temporal_market_shares"].keys()) == {"background_2030"}
    # the summed bg_B amount equals 10 kg (2*5)
    assert bg_b_rows["amount"].abs().sum() == pytest.approx(10.0, rel=1e-9)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_background_traversal.py::test_bfs_background_td_spreads_bg_b_across_variants -v`
Expected: FAIL — without variant splitting, `bg_B` appears at a single date / single variant, or the descent reads only the referenced variant.

- [ ] **Step 3: Give the BFS extractor access to interpolation + variant mapping**

In `bw_timex/timeline_builder.py`, after constructing `EdgeExtractorBFS`, hand it the data it needs for variant resolution (these already exist on the builder / are passed in):

```python
            self.edge_extractor.database_dates_static = self.database_dates_static
            self.edge_extractor.interdatabase_activity_mapping = (
                self.interdatabase_activity_mapping
            )
            self.edge_extractor.interpolation_type = self.interpolation_type
```

`TimelineBuilder` must therefore receive `interdatabase_activity_mapping`. It is
already built on `TimexLCA` (`add_interdatabase_activity_mapping_from_timeline`)
**after** the timeline today; for traversal-time variant resolution it must exist
**before**. Add a pre-pass: in `TimexLCA.build_timeline`, before constructing
`TimelineBuilder`, when `traverse_background` is set, call a new method
`self.add_full_interdatabase_activity_mapping()` that populates the mapping for all
background activities (name/reference product/location across
`database_dates_static`). Implement it by generalizing the existing
`add_interdatabase_activity_mapping_from_timeline` to iterate all background nodes
rather than only timeline producers:

```python
    def add_full_interdatabase_activity_mapping(self) -> None:
        static_dbs = set(self.database_dates_static.keys())
        tuples_dict = {}
        for node in self.nodes.values():
            if node["database"] not in static_dbs:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            tuples_dict.setdefault(key, node.id)
        for node in self.nodes.values():
            if node["database"] not in static_dbs:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            anchor = tuples_dict[key]
            self.interdatabase_activity_mapping.setdefault(anchor, {})
            self.interdatabase_activity_mapping[anchor][node["database"]] = node.id
        self.interdatabase_activity_mapping.make_reciprocal()
```

Pass `self.interdatabase_activity_mapping` into the `TimelineBuilder(...)` call as a
new keyword `interdatabase_activity_mapping=self.interdatabase_activity_mapping` and
store it on the builder.

- [ ] **Step 4: Implement variant resolution + split in BFS**

In `bw_timex/edge_extractor.py`, add helpers to `EdgeExtractorBFS`:

```python
    def _variant_shares_for_date(self, producer_date) -> dict:
        """Return {db_name: share} for a datetime producer_date over the static
        background variant dates, using the configured interpolation."""
        from datetime import datetime as _dt

        dates_to_db = {
            v: k
            for k, v in self.database_dates_static.items()
            if isinstance(v, _dt)
        }
        sorted_dates = tuple(sorted(dates_to_db))
        if getattr(self, "interpolation_type", "linear") == "nearest":
            weights = _nearest_weight(producer_date, sorted_dates)
        else:
            weights = _linear_weights(producer_date, sorted_dates)
        return {dates_to_db[d]: w for d, w in weights.items()}

    def _resolve_in_variant(self, node_id: int, db_name: str) -> int:
        """Map node_id to its counterpart in db_name (identity if already there)."""
        node = self.nodes.get(node_id)
        if node is not None and node["database"] == db_name:
            return node_id
        return self.interdatabase_activity_mapping.find_match(node_id, db_name)
```

Add module-level `_linear_weights` / `_nearest_weight` mirroring
`TimelineBuilder.get_weights_for_interpolation_between_nearest_years` and
`find_closest_date` (copy the bisect logic; return `{date: weight}` dicts). Keep
them as free functions so both the builder and extractor can share intent (DRY:
prefer importing the existing builder methods if feasible — if you extract the
bisect logic into `bw_timex/utils.py` as `linear_interpolation_weights(date,
sorted_dates)` and `nearest_date_weight(date, sorted_dates)`, have the
`TimelineBuilder` methods call them too, and import them here).

In `build_edge_timeline`, when descending into a background producer, branch on the
producer's resolved date vs. the consumer's current variant:

```python
                producer_node = self.nodes.get(input_id)
                producer_is_background = (
                    producer_node is not None
                    and producer_node["database"] in self.database_dates_static
                )

                if self.traverse_background and producer_is_background:
                    # Determine the producer's absolute date(s). For a single-cohort
                    # abs_td_producer this is one datetime; for multi-cohort, iterate.
                    producer_dates = np.atleast_1d(abs_td_producer.date)
                    producer_amounts = np.atleast_1d(abs_td_producer.amount)
                    current_db = (
                        producer_node["database"]
                        if not isinstance(td_producer_raw, TemporalDistribution)
                        else None
                    )
                    for p_date, p_amount in zip(producer_dates, producer_amounts):
                        shares = self._variant_shares_for_date(
                            p_date.astype("datetime64[s]").astype(object)
                        )
                        for db_name, share in shares.items():
                            variant_id = self._resolve_in_variant(input_id, db_name)
                            # enqueue a branch in this variant, scaled by share*p_amount
                            ... build a per-branch Edge + queue item ...
```

Implementation details for the inner branch:
- Build a single-cohort `TemporalDistribution` for this branch:
  `branch_td = TemporalDistribution(date=np.array([p_date]), amount=np.array([share * p_amount]))`.
- Emit an `Edge(producer=variant_id, consumer=node_id, ...)` with
  `abs_td_producer=branch_td`, `td_producer` scaled by `share`, `edge_type` from the
  exchange, `temporal_evolution` as read.
- Compute `new_supply` as today but multiplied by `share`.
- If not a leaf and above cutoff, descend into `variant_id` **in db `db_name`**,
  reading that variant's exchanges. Because `self.nodes` and the matrix are keyed by
  global ids, descend using `variant_id` directly; `_get_technosphere_inputs`,
  `_get_exchange_td_and_type`, and `_get_production_amount` already resolve by id, so
  reading `variant_id`'s own exchanges yields the respective variant's TDs/amounts.
  NOTE: `variant_id` may not be in `base_lca`'s matrix (only the referenced variant
  is). Read amounts/TDs from the **bw2data activity proxy** (`self.nodes[variant_id]`
  if present, else `bd.get_activity(variant_id)`), NOT from `tech_matrix_csc`. Add a
  `_get_exchange_td_and_type_from_proxy(variant_id)` that iterates
  `self.nodes[variant_id].technosphere()`/`.substitution()` and reads
  `exc["amount"]`, `exc.get("temporal_distribution")`, and
  `extract_temporal_evolution(exc.data)` directly. Ensure such variant nodes are in
  `self.nodes` by having `TimelineBuilder` populate `nodes` for all background
  variant activities when `traverse_background` is set (extend the `nodes` dict
  construction accordingly).

The sticky (no-split) case — when the producer's date resolves to the same single
variant as the consumer context — falls out naturally: `shares` has one entry equal
to the current db with weight 1.

- [ ] **Step 5: Run the spread test**

Run: `pytest tests/test_background_traversal.py::test_bfs_background_td_spreads_bg_b_across_variants -v`
Expected: PASS.

- [ ] **Step 6: Re-run equivalence + full suite**

Run: `pytest tests/test_background_traversal.py -v && pytest -q`
Expected: all pass — including `test_bfs_equivalence_without_background_tds` (no-TD ⇒ single sticky variant per branch).

- [ ] **Step 7: Commit**

```bash
git add bw_timex/edge_extractor.py bw_timex/timeline_builder.py bw_timex/timex_lca.py bw_timex/utils.py tests/test_background_traversal.py
git commit -m "BFS: variant-aware split for background temporal distributions"
```

---

### Task 6: Per-variant background TD + no-double-count tests (BFS)

Lock in two behaviors with dedicated tests: (a) a TD defined differently across variants produces variant-dependent spreads; (b) a traversed background process contributes its explicit edges, not its full static inventory.

**Files:**
- Modify: `tests/fixtures/background_td_db_fixture.py` (parametrizable TD per variant) OR a second fixture
- Test: `tests/test_background_traversal.py`

**Interfaces:**
- Consumes: Task 5 behavior.
- Produces: regression coverage; no new production code expected (if a test fails, fix the smallest cause in `edge_extractor.py`).

- [ ] **Step 1: Write the per-variant TD test**

Add a fixture `background_td_db_per_variant` (copy `background_td_db`, give
`background_2030`'s `bg_A→bg_B` a different TD, e.g. 100% at +0y), register it in
`conftest.py`, then:

```python
def test_per_variant_td_changes_spread(background_td_db_per_variant):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tl = tlca.timeline
    bg_b = tl[tl["producer_name"] == "bg_B"]
    # The 2020-variant branch spreads to 2034; the 2030-variant branch does not.
    years = sorted({d.year for d in bg_b["date_producer"]})
    assert 2024 in years and 2034 in years
```

(Adjust the precise assertion to the fixture's TDs once observed; the invariant is
that the spread differs by which variant the branch reads.)

- [ ] **Step 2: Run it**

Run: `pytest tests/test_background_traversal.py::test_per_variant_td_changes_spread -v`
Expected: PASS (or reveals a bug to fix minimally in `edge_extractor.py`).

- [ ] **Step 3: Write the no-double-count test**

```python
def test_traversed_background_not_double_counted(background_td_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lca.lcia()
    # bg_A is traversed into -> it must be a temporalized process, NOT a temporal market.
    bg_a_id = background_td_db["background_2020"][0].id
    tl = tlca.timeline
    bg_a_rows = tl[tl["producer"] == bg_a_id]
    assert bg_a_rows["temporal_market_shares"].isnull().all()
    # total CO2 is 10 kg (2*5*1), not doubled.
    tlca.static_lca.lcia()
    assert tlca.static_lca.score == pytest.approx(10.0, rel=1e-9)
```

- [ ] **Step 4: Run it**

Run: `pytest tests/test_background_traversal.py::test_traversed_background_not_double_counted -v`
Expected: PASS.

- [ ] **Step 5: Full suite**

Run: `pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/background_td_db_fixture.py tests/conftest.py tests/test_background_traversal.py bw_timex/edge_extractor.py
git commit -m "Tests: per-variant background TD and no-double-count (bfs)"
```

---

### Task 7: Priority-engine descent + variant hopping + documented score approximation

Bring the priority `EdgeExtractor` to parity: descend into background, split at variant frontiers, and read respective-variant exchanges. Heap ordering uses `base_lca` scores for hopped-variant nodes (accepted approximation), with a one-time warning.

**Files:**
- Modify: `bw_timex/edge_extractor.py:114-327` (`EdgeExtractor.build_edge_timeline`)
- Modify: `bw_timex/timex_lca.py` `build_timeline` (warning emission)
- Test: `tests/test_background_traversal.py`

**Interfaces:**
- Consumes: same variant-resolution helpers as Task 5 (share-by-date, `_resolve_in_variant`); reuse them — extract the shared logic into `bw_timex/utils.py` if not already done in Task 5, and call from both extractors.
- Produces: priority-mode parity; `EdgeExtractor` gains `database_dates_static`, `interdatabase_activity_mapping`, `interpolation_type` attributes (set by `TimelineBuilder`, mirroring Task 5 Step 3).

- [ ] **Step 1: Write the parametrized parity test**

Add to `tests/test_background_traversal.py`:

```python
@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_background_td_parity_across_engines(background_td_db, graph_traversal):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2024-01-01",
        graph_traversal=graph_traversal,
        traverse_background=True,
    )
    tlca.lci()
    tlca.static_lca.lcia()
    tl = tlca.timeline
    bg_b = tl[tl["producer_name"] == "bg_B"]
    assert sorted({d.year for d in bg_b["date_producer"]}) == [2024, 2034]
    assert tlca.static_lca.score == pytest.approx(10.0, rel=1e-9)
```

- [ ] **Step 2: Run it (priority case fails)**

Run: `pytest "tests/test_background_traversal.py::test_background_td_parity_across_engines[priority]" -v`
Expected: FAIL — priority does not yet split/descend into background variants.

- [ ] **Step 3: Set extractor attributes for priority**

In `bw_timex/timeline_builder.py`, after building the `EdgeExtractor` (priority
branch), set the same three attributes as Task 5 Step 3:

```python
            self.edge_extractor.database_dates_static = self.database_dates_static
            self.edge_extractor.interdatabase_activity_mapping = (
                self.interdatabase_activity_mapping
            )
            self.edge_extractor.interpolation_type = self.interpolation_type
```

- [ ] **Step 4: Implement descent + split in priority `build_edge_timeline`**

In `bw_timex/edge_extractor.py` `EdgeExtractor.build_edge_timeline`, inside the
`while heap:` loop where each child edge produces an `Edge` and (if `not leaf`) is
pushed to the heap, add a variant-split branch analogous to BFS:

```python
                producer_node = self.nodes_proxy_for(producer.activity_datapackage_id)
                producer_is_background = (
                    producer_node is not None
                    and producer_node["database"] in self.database_dates_static
                )
                if self.traverse_background and producer_is_background:
                    shares = variant_shares_for_date(
                        abs_td_producer_date, self.database_dates_static,
                        self.interpolation_type,
                    )
                    for db_name, share in shares.items():
                        variant_id = resolve_in_variant(
                            producer.activity_datapackage_id, db_name,
                            self.interdatabase_activity_mapping, self.nodes_lookup,
                        )
                        # emit a scaled Edge for (variant_id -> node) and, if not leaf
                        # and above cutoff, push variant_id with the approximated score:
                        approx_score = node.cumulative_score  # base_lca proxy
                        heappush(heap, (1 / approx_score, scaled_distribution,
                                        td_producer, scaled_abs_td, variant_node))
                    continue  # replaced the single non-split push
```

Key adaptations vs BFS:
- The priority extractor uses `self.nodes` keyed by `unique_id` (bw_temporalis
  node objects), not by activity id. Add a small lookup
  `self.nodes_lookup = {n.activity_datapackage_id: n for n in self.nodes.values()}`
  built once in `__init__` (after `super().__init__`). For a hopped `variant_id`
  that has no bw_temporalis node (not in `base_lca`), construct a lightweight stand-in
  carrying `.activity_datapackage_id = variant_id`, `.cumulative_score =
  node.cumulative_score` (the approximation), and `.unique_id` unique — OR, simpler,
  represent hopped variants as **leaf** edges that get `temporal_market_shares =
  {db: 1}` downstream and do not require further heap descent. **Prefer the leaf
  approach for variants whose subgraph is not in `base_lca`:** it avoids fabricating
  scores and matches the spec's "leaves resolve to {variant: 1}". Only the
  referenced variant (present in `base_lca`) is descended deeper; non-referenced
  variants of a node are emitted as leaf temporal markets at the split. Reading the
  *referenced* variant's own deeper TDs is what continues the chain.
- Read amounts/TDs for emitted variant edges from the bw2data proxy (as in BFS
  `_get_exchange_td_and_type_from_proxy`), since non-referenced variants are not in
  `base_lca`.

Reuse the shared helpers `variant_shares_for_date`, `resolve_in_variant`,
`get_exchange_td_and_type_from_proxy` (factor these into `bw_timex/utils.py` in
Task 5 and import in both extractors — DRY).

- [ ] **Step 5: Emit the one-time priority warning**

In `bw_timex/timex_lca.py` `build_timeline`, after `graph_traversal` is resolved:

```python
        if traverse_background and graph_traversal == "priority":
            logger.warning(
                "traverse_background=True with graph_traversal='priority': heap "
                "ordering uses base_lca scores as an approximation for nodes in "
                "non-referenced background variants. Edge exploration order under "
                "max_calc/cutoff may differ slightly; explored amounts are correct. "
                "Use graph_traversal='bfs' to avoid the approximation."
            )
```

(`logger` is already imported in `timex_lca.py`.)

- [ ] **Step 6: Run the parity test (both engines)**

Run: `pytest tests/test_background_traversal.py::test_background_td_parity_across_engines -v`
Expected: PASS for both `priority` and `bfs`.

- [ ] **Step 7: Full suite**

Run: `pytest -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/edge_extractor.py bw_timex/timeline_builder.py bw_timex/timex_lca.py bw_timex/utils.py tests/test_background_traversal.py
git commit -m "Priority: descend into background with variant splitting (base_lca score approx)"
```

---

### Task 8: Documentation

Document the flag and its semantics in the `build_timeline` docstring and (if present) the user-facing docs.

**Files:**
- Modify: `bw_timex/timex_lca.py:217-264` (`build_timeline` docstring)
- Modify: `docs/` user guide if a relevant page exists (search first)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add the `traverse_background` parameter to the docstring**

In `bw_timex/timex_lca.py` `build_timeline`, document:

```
        traverse_background : bool, optional
            If True, the graph traversal descends into background databases instead
            of stopping at the first-level background frontier. Temporal
            distributions defined on exchanges inside background databases are then
            honored: time-spread flows are sourced from the temporally-appropriate
            background-db variant(s). Bounded by ``cutoff`` and ``max_calc``.
            Default is False (background treated as static, as before). With
            ``graph_traversal='priority'`` the heap ordering uses base_lca scores as
            an approximation for non-referenced variant nodes; use
            ``graph_traversal='bfs'`` for exact ordering.
```

- [ ] **Step 2: Check for a docs page to update**

Run: `grep -rl "build_timeline\|temporal market" docs/ 2>/dev/null`
If a relevant `.md`/`.rst` exists, add a short subsection describing
`traverse_background`. If none, skip.

- [ ] **Step 3: Build docs if the project does so (optional sanity)**

Run: `grep -q "sphinx\|mkdocs" pyproject.toml setup.cfg 2>/dev/null && echo "has docs tooling" || echo "no docs build"`
If docs tooling exists and is quick, build; otherwise skip.

- [ ] **Step 4: Commit**

```bash
git add bw_timex/timex_lca.py docs/
git commit -m "Document traverse_background flag"
```

---

## Self-Review

**Spec coverage:**
- API flag `traverse_background` → Task 1. ✓
- Remove static exclusion, bound by cutoff/max_calc (+ BFS max_calc guard) → Task 4. ✓
- Split rule (entry + TD edges, sticky otherwise) → Tasks 5 (bfs), 7 (priority). ✓
- Read respective variant's exchanges/amounts/TDs → Task 5 Step 4, Task 7 Step 4. ✓
- Priority base_lca score approximation + one-time warning → Task 7 Steps 4–5. ✓
- `build_timeline` leaf-based `temporal_market_shares` → Task 4 Step 5. ✓
- `matrix_modifier` + `collect_temporalized` key on shares (no double-count) → Task 3, tested Task 6. ✓
- `interdatabase_activity_mapping` available pre-traversal → Task 5 Step 3. ✓
- Tests: equivalence (Task 4), spread (Task 5), no-double-count (Task 6), per-variant (Task 6), both engines (Task 7). ✓
- Backward compatibility (default unchanged) → asserted by full-suite runs in every task. ✓

**Known implementation risks (call out to reviewer, resolve during execution):**
- The priority `EdgeExtractor` internals (`self.nodes` keyed by `unique_id`,
  `node.cumulative_score`, `edge_mapping`) come from `bw_temporalis`. Task 7 Step 4
  proposes the **leaf approach** for non-referenced variants to avoid fabricating
  scores; if descent into non-referenced variants deeper than one hop is required by
  a failing test, revisit. The referenced variant is descended normally.
- `abs_td_producer` may be multi-cohort; the BFS split iterates cohorts. Confirm the
  date dtype handling (`datetime64[s]` → python `datetime`) when calling the
  interpolation helpers.
- `_leaf_background_producers` reads `consumer`/`producer` columns; verify they exist
  at the call site in `add_column_temporal_market_shares_to_timeline` (Task 4 Step 5
  note).

**Placeholder scan:** Test code is concrete; production steps show concrete code or
exact structural edits against named functions. The few "adjust assertion once
observed" notes are in tests where the exact numeric split is fixture-dependent — the
invariant being tested is stated explicitly.

**Type consistency:** Helper names used consistently across tasks
(`variant_shares_for_date`, `resolve_in_variant`,
`get_exchange_td_and_type_from_proxy`, `_leaf_background_producers`); shared helpers
are factored into `utils.py` in Task 5 and imported in Task 7.
