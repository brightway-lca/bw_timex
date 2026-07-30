# Background production-edge TD conservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `traverse_background=True` conserve impact (and not raise `KeyError`) when a descended background node carries a production-edge temporal distribution, by registering the node at the same production-TD-weighted cohorts it is consumed at.

**Architecture:** In the shared `VariantBackgroundMixin` proxy-descent, fold a background producer's own production-edge TD into the *effective producer TD of the edge that produces it* (outer-product convolution — dates sum, amounts multiply), instead of applying it only to the node's child expansion. This makes the producer's registered cohort-years equal its consumed cohort-years (kills the `KeyError`) and carries `exchange_weight × prodTD_weight` per cohort (conserves). Foreground / explicit product-process modelling and the matrix traversal are untouched.

**Tech Stack:** Python, `bw_timex`, `bw_temporalis` (`TemporalDistribution`), `numpy`, `pytest`, `bw2data` test fixtures (`@bw2test`).

## Global Constraints

- Change lives ONLY in `VariantBackgroundMixin` (background proxy-descent) — do not modify `build_edge_timeline` FU-seed logic, the matrix traversal, or `_join_datetime_and_timedelta_distributions`'s global behaviour.
- Both engines (`EdgeExtractor` priority, `EdgeExtractorBFS`) must pass every test — parametrize `graph_traversal` over `["priority", "bfs"]`.
- Conservation assertion: time-explicit `static_score == base_lca.score` within `rel=1e-6`.
- Keep the existing nearest-registered-year snap in `TimelineBuilder.get_time_mapping_key` (defense-in-depth); do not remove it.
- Preserve array alignment: the emitted `Edge`'s `td_producer`, `abs_td_producer`, and `distribution` must stay index-aligned (same length, same ravel order) so `extract_edge_data` explodes them consistently.
- TDD: failing test first, watch it fail, minimal fix, watch it pass, commit. No Claude attribution in commit messages.

---

## File Structure

- `bw_timex/edge_extractor.py` — add one helper to `VariantBackgroundMixin`; modify `_emit_variant_split_for_consumer_date` and `_descend_variant_subtree`.
- `tests/fixtures/background_prod_td_db_fixture.py` — new fixtures (single-chain and convergent) with production-edge TDs.
- `tests/conftest.py` — register the new fixtures.
- `tests/test_background_production_td.py` — new conservation tests.
- `tests/test_repro_variant_mismatch.py` — throwaway; delete at the end (its scenarios are superseded by the new named tests).

---

### Task 1: Failing conservation test — production TD on a first-level descended background node

**Files:**
- Create: `tests/fixtures/background_prod_td_db_fixture.py`
- Modify: `tests/conftest.py`
- Test: `tests/test_background_production_td.py`

**Interfaces:**
- Produces: fixture `background_prod_td_db` returning `{db_name: {"bg_A":node, "bg_B":node, "bg_C":node}}`; two dated variants `background_2020`, `background_2030`; `bg_A->bg_B` carries a technosphere TD; `bg_B` carries a production-edge TD.

- [ ] **Step 1: Write the fixture**

Create `tests/fixtures/background_prod_td_db_fixture.py`:

```python
import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def background_prod_td_db():
    """fu -> bg_A -> bg_B -> bg_C -> CO2, two dated variants.

    bg_A->bg_B carries a technosphere TD (triggers the variant-split descent).
    bg_B carries a PRODUCTION-edge TD spread over several years, so the descent
    must register bg_B at the same production-TD-weighted cohorts it consumes
    bg_C at. All coefficients are 1, so the total impact must equal 1.0.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    bg20 = bd.Database("background_2020")
    bg20.register()
    bg30 = bd.Database("background_2030")
    bg30.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod_td_b = TemporalDistribution(
        date=np.array([0, 3, 6], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.3, 0.2]),
    )

    variants = {}
    for db in (bg20, bg30):
        bg_a = db.new_node("bg_A", name="bg_A", unit="k"); bg_a["reference product"] = "bg_A"; bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="k"); bg_b["reference product"] = "bg_B"; bg_b.save()
        bg_c = db.new_node("bg_C", name="bg_C", unit="k"); bg_c["reference product"] = "bg_C"; bg_c.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        pb = bg_b.new_edge(input=bg_b, amount=1, type="production")
        pb["temporal_distribution"] = prod_td_b
        pb.save()
        bg_c.new_edge(input=bg_c, amount=1, type="production").save()

        e = bg_a.new_edge(input=bg_b, amount=1, type="technosphere")
        e["temporal_distribution"] = td_a_to_b
        e.save()
        bg_b.new_edge(input=bg_c, amount=1, type="technosphere").save()
        bg_c.new_edge(input=co2, amount=1, type="biosphere").save()
        variants[db.name] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c}

    fu.new_edge(input=variants["background_2020"]["bg_A"], amount=1, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variants
```

- [ ] **Step 2: Register the fixture in conftest**

In `tests/conftest.py`, add alongside the other fixture imports:

```python
from .fixtures.background_prod_td_db_fixture import background_prod_td_db
```

- [ ] **Step 3: Write the failing conservation test**

Create `tests/test_background_production_td.py`:

```python
from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime(2020, 1, 1),
    "background_2030": datetime(2030, 1, 1),
    "foreground": "dynamic",
}


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_first_level_production_td_conserves(background_prod_td_db, graph_traversal):
    t = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    t.build_timeline(
        starting_datetime="2020-01-01",
        temporal_grouping="year",
        graph_traversal=graph_traversal,
        traverse_background=True,
        cutoff=1e-9,
        max_calc=2000,
    )
    t.lci()
    t.static_lcia()
    assert t.static_score == pytest.approx(t.base_lca.score, rel=1e-6)
```

- [ ] **Step 4: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_background_production_td.py -q --no-cov`
Expected: FAIL — `assert 3.0 == 1.0 ± 1.0e-06` (both `priority` and `bfs`). The production-TD cohorts are each counted in full.

- [ ] **Step 5: Commit the failing test**

```bash
git add tests/fixtures/background_prod_td_db_fixture.py tests/conftest.py tests/test_background_production_td.py
git commit -m "test: failing conservation test for background production-edge TD"
```

---

### Task 2: Add the outer-product convolution helper

**Files:**
- Modify: `bw_timex/edge_extractor.py` (add method to `VariantBackgroundMixin`, near `_normalized_production_edge_td_from_proxy` around line 208)
- Test: `tests/test_edge_extractor.py`

**Interfaces:**
- Produces: `VariantBackgroundMixin._fold_production_td(base_td: TemporalDistribution, prod_td: TemporalDistribution) -> TemporalDistribution` — returns a TD whose dates are the outer sum `base.date[i] + prod.date[j]` and amounts are the outer product `base.amount[i] * prod.amount[j]`, raveled in row-major (`i`-major) order. Works for `base` datetime or timedelta.

- [ ] **Step 1: Write the failing unit test**

Add to `tests/test_edge_extractor.py`:

```python
import numpy as np
from bw_temporalis import TemporalDistribution
from bw_timex.edge_extractor import VariantBackgroundMixin


def test_fold_production_td_outer_product():
    base = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod = TemporalDistribution(
        date=np.array([0, 3], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.5]),
    )
    out = VariantBackgroundMixin._fold_production_td(base, prod)
    # dates: 0+0, 0+3, 10+0, 10+3  (i-major)
    assert list(out.date.astype("timedelta64[Y]").astype(int)) == [0, 3, 10, 13]
    # amounts: 0.6*0.5, 0.6*0.5, 0.4*0.5, 0.4*0.5
    np.testing.assert_allclose(out.amount, [0.3, 0.3, 0.2, 0.2])
    # total weight preserved (prod is normalized)
    assert out.amount.sum() == pytest.approx(base.amount.sum())
```

Add `import pytest` at the top of the file if not already present.

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_edge_extractor.py::test_fold_production_td_outer_product -q --no-cov`
Expected: FAIL — `AttributeError: ... has no attribute '_fold_production_td'`.

- [ ] **Step 3: Implement the helper**

In `bw_timex/edge_extractor.py`, inside `class VariantBackgroundMixin`, immediately after `_normalized_production_edge_td_from_proxy` (ends ~line 221), add:

```python
    @staticmethod
    def _fold_production_td(base_td, prod_td):
        """Convolve ``base_td`` with a producer's normalized production-edge TD.

        Unlike ``_join_datetime_and_timedelta_distributions`` (which tiles the
        producer amounts and drops the consumer-side amounts), this takes the
        outer product of amounts and the outer sum of dates, so the cohort
        weights carried in ``base_td`` are preserved. Ravel is ``base``-major so
        the result stays index-aligned with a sibling ``base_td`` folded the same
        way. Used to register a descended background producer at its
        production-TD-weighted cohorts.
        """
        date = (
            base_td.date.reshape(-1, 1) + prod_td.date.reshape(1, -1)
        ).ravel()
        amount = (
            base_td.amount.reshape(-1, 1) * prod_td.amount.reshape(1, -1)
        ).ravel()
        return TemporalDistribution(date=date, amount=amount)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_edge_extractor.py::test_fold_production_td_outer_product -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/edge_extractor.py tests/test_edge_extractor.py
git commit -m "feat: add production-TD outer-product fold helper"
```

---

### Task 3: Fold production TD into the first-level variant-split edge

**Files:**
- Modify: `bw_timex/edge_extractor.py` — `_emit_variant_split_for_consumer_date` (the per-variant loop body, lines ~369-408)

**Interfaces:**
- Consumes: `self._fold_production_td` (Task 2), `self._normalized_production_edge_td_from_proxy`.
- Produces: the emitted split `Edge` for `variant_id` now spans the production-TD cohorts; `_descend_variant_subtree` is entered with `td`/`abs_td` already folded (no separate re-application).

- [ ] **Step 1: Replace the masked-arrays + child block**

In `_emit_variant_split_for_consumer_date`, replace the block from `masked_abs_td_producer = TemporalDistribution(` (line ~370) through the end of the `if producer_production_td is not None:` child block (line ~408) with:

```python
            masked_abs_td_producer = TemporalDistribution(
                date=abs_td_producer.date[keep_idx],
                amount=abs_td_producer.amount[keep_idx] * weights,
            )
            masked_distribution = TemporalDistribution(
                date=distribution.date[keep_idx],
                amount=distribution.amount[keep_idx] * weights,
            )
            masked_td_producer = TemporalDistribution(
                date=td_producer.date[keep_idx],
                amount=td_producer.amount[keep_idx] * weights,
            )
            variant_id = self._resolve_in_variant(producer_process, db_name)
            self.variant_resolved_producers.add(variant_id)

            # If this background producer has a production-edge TD, spread it into
            # production-TD-weighted cohorts on the PRODUCER side too, so it is
            # registered at exactly the cohort-years it is later consumed at
            # (bands match -> no KeyError) with weights = exchange x production
            # (conserves). Fold identically into all three arrays to keep them
            # index-aligned for extract_edge_data.
            producer_production_td = self._normalized_production_edge_td_from_proxy(
                variant_id
            )
            if producer_production_td is not None:
                masked_td_producer = self._fold_production_td(
                    masked_td_producer, producer_production_td
                )
                masked_abs_td_producer = self._fold_production_td(
                    masked_abs_td_producer, producer_production_td
                )
                masked_distribution = self._fold_production_td(
                    masked_distribution, producer_production_td
                )

            edges.append(
                Edge(
                    edge_type=edge_type,
                    distribution=masked_distribution,
                    leaf=self.edge_ff(producer_process),
                    consumer=node_id,
                    producer=variant_id,
                    td_producer=masked_td_producer,
                    td_consumer=td_parent,
                    abs_td_producer=masked_abs_td_producer,
                    abs_td_consumer=abs_td,
                    temporal_evolution=temporal_evolution,
                )
            )

            child_td, child_abs_td = masked_distribution, masked_abs_td_producer
```

(This moves the `Edge` append to AFTER the fold, deletes the old post-append `child_td, child_abs_td = ...` + `producer_production_td` re-application block, and sets the child directly from the already-folded arrays. Leave the `variant_supply = ...` line and the `self._descend_variant_subtree(...)` call that follow unchanged.)

- [ ] **Step 2: Run the Task 1 conservation test**

Run: `.venv/bin/python -m pytest tests/test_background_production_td.py -q --no-cov`
Expected: PASS for both `priority` and `bfs` (score `== 1.0`). bg_B's production TD is applied at this first-level split site.

- [ ] **Step 3: Run the full existing suite (no regressions)**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/test_repro_variant_mismatch.py -q --no-cov`
Expected: all pass (baseline was 244 + the 2 new = 246).

- [ ] **Step 4: Commit**

```bash
git add bw_timex/edge_extractor.py
git commit -m "fix: fold production-edge TD into first-level background variant split"
```

---

### Task 4: Fold production TD into the descent edges

**Files:**
- Modify: `bw_timex/edge_extractor.py` — `_descend_variant_subtree` inner loop (edge emit ~520-533 and child block ~535-549)
- Test: `tests/test_background_production_td.py` (add a deeper-node case)

**Interfaces:**
- Consumes: `self._fold_production_td`, `self._normalized_production_edge_td_from_proxy`, `self._producer_process_in_variant`.
- Produces: descent edges producing `input_id` span its production-TD cohorts; the queued child uses the folded arrays.

- [ ] **Step 1: Add a failing test — production TD on a node reached via the descent**

Append to `tests/fixtures/background_prod_td_db_fixture.py`:

```python
@pytest.fixture
@bw2test
def background_prod_td_deep_db():
    """fu -> bg_A -> bg_B -> bg_C -> CO2, two variants.

    bg_A->bg_B carries a technosphere TD (starts the descent); bg_C (reached
    one level deeper, inside the locked-variant descent) carries the
    production-edge TD. Exercises the descent emit site rather than the
    first-level split. Total impact must equal 1.0.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    bg20 = bd.Database("background_2020")
    bg20.register()
    bg30 = bd.Database("background_2030")
    bg30.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td_a_to_b = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod_td_c = TemporalDistribution(
        date=np.array([0, 3, 6], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.3, 0.2]),
    )

    variants = {}
    for db in (bg20, bg30):
        bg_a = db.new_node("bg_A", name="bg_A", unit="k"); bg_a["reference product"] = "bg_A"; bg_a.save()
        bg_b = db.new_node("bg_B", name="bg_B", unit="k"); bg_b["reference product"] = "bg_B"; bg_b.save()
        bg_c = db.new_node("bg_C", name="bg_C", unit="k"); bg_c["reference product"] = "bg_C"; bg_c.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_b.new_edge(input=bg_b, amount=1, type="production").save()
        pc = bg_c.new_edge(input=bg_c, amount=1, type="production")
        pc["temporal_distribution"] = prod_td_c
        pc.save()

        e = bg_a.new_edge(input=bg_b, amount=1, type="technosphere")
        e["temporal_distribution"] = td_a_to_b
        e.save()
        bg_b.new_edge(input=bg_c, amount=1, type="technosphere").save()
        bg_c.new_edge(input=co2, amount=1, type="biosphere").save()
        variants[db.name] = {"bg_A": bg_a, "bg_B": bg_b, "bg_C": bg_c}

    fu.new_edge(input=variants["background_2020"]["bg_A"], amount=1, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variants
```

Register it in `tests/conftest.py`:

```python
from .fixtures.background_prod_td_db_fixture import (
    background_prod_td_db,
    background_prod_td_deep_db,
)
```

Add to `tests/test_background_production_td.py`:

```python
@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_deep_production_td_conserves(background_prod_td_deep_db, graph_traversal):
    t = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    t.build_timeline(
        starting_datetime="2020-01-01",
        temporal_grouping="year",
        graph_traversal=graph_traversal,
        traverse_background=True,
        cutoff=1e-9,
        max_calc=2000,
    )
    t.lci()
    t.static_lcia()
    assert t.static_score == pytest.approx(t.base_lca.score, rel=1e-6)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_background_production_td.py::test_deep_production_td_conserves -q --no-cov`
Expected: FAIL — score `3.0` (or another N×) vs `1.0`. bg_C's production TD is applied at the descent site, not yet fixed.

- [ ] **Step 3: Fix the descent emit + child block**

In `_descend_variant_subtree`, the inner `for input_id in input_ids:` loop currently computes `distribution` and `abs_td_producer` (lines ~496-499), resolves `producer_process` (~507), emits the `Edge` (~520-533), then applies the production TD only to the child (~538-546). Replace the emit-and-child region so the production TD is folded into the emitted edge.

Replace the block starting at `producer_process = self._producer_process_in_variant(` (line ~507) through the `queue.append((...))` at the end of the child block (line ~549) with:

```python
                producer_process = self._producer_process_in_variant(
                    input_id, variant_db
                )
                will_descend = (
                    not leaf
                    and new_supply >= self.cutoff * total_demand
                    and producer_process is not None
                )

                # Already routed to its real variant database -> temporalize it.
                if self._is_static_background(input_id):
                    self.variant_resolved_producers.add(input_id)

                # Fold this producer's own production-edge TD into the producer
                # side so it is registered at the same production-TD-weighted
                # cohorts it is consumed at (bands match -> no KeyError; weights
                # = exchange x production -> conserves). Fold identically into
                # td_producer/abs_td_producer/distribution to keep them aligned.
                producer_production_td = None
                if producer_process is not None:
                    producer_production_td = (
                        self._normalized_production_edge_td_from_proxy(producer_process)
                    )
                if producer_production_td is not None:
                    td_producer = self._fold_production_td(
                        td_producer, producer_production_td
                    )
                    abs_td_producer = self._fold_production_td(
                        abs_td_producer, producer_production_td
                    )
                    distribution = self._fold_production_td(
                        distribution, producer_production_td
                    )

                edges.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=cur_id,
                        producer=input_id,
                        td_producer=td_producer,
                        td_consumer=cur_parent,
                        abs_td_producer=abs_td_producer,
                        abs_td_consumer=cur_abs_td,
                        temporal_evolution=temporal_evolution,
                    )
                )

                if not will_descend:
                    continue

                queue.append(
                    (producer_process, distribution, td_producer, abs_td_producer, new_supply)
                )
```

Note: this removes the old separate `child_td/child_abs_td` re-application (the folded `distribution`/`abs_td_producer` are now queued directly). The `leaf`, `td_producer_raw`, `edge_supply`, and `new_supply` computations earlier in the loop are unchanged (supply tracking still uses the raw exchange amount). If `producer_process is None` (pure leaf/product with no producer), `producer_production_td` stays `None` and behaviour is unchanged.

- [ ] **Step 4: Run the deep test + first-level test**

Run: `.venv/bin/python -m pytest tests/test_background_production_td.py -q --no-cov`
Expected: PASS for all four cases (first-level + deep, each `priority`/`bfs`), score `== 1.0`.

- [ ] **Step 5: Run the full existing suite**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/test_repro_variant_mismatch.py -q --no-cov`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/edge_extractor.py tests/fixtures/background_prod_td_db_fixture.py tests/conftest.py tests/test_background_production_td.py
git commit -m "fix: fold production-edge TD into background descent edges"
```

---

### Task 5: Convergent + production-TD conservation

**Files:**
- Modify: `tests/fixtures/background_prod_td_db_fixture.py` (add convergent fixture)
- Modify: `tests/conftest.py`
- Test: `tests/test_background_production_td.py`

**Interfaces:**
- Produces: fixture `background_prod_td_convergent_db` — a background node with a production-edge TD reached via two parents.

- [ ] **Step 1: Add the convergent fixture**

Append to `tests/fixtures/background_prod_td_db_fixture.py`:

```python
@pytest.fixture
@bw2test
def background_prod_td_convergent_db():
    """fu -> bg_A -> {bg_S, bg_R -> bg_S}; bg_S -> CO2, two variants.

    bg_S (production-edge TD) is reached both directly from bg_A and via bg_R,
    so it appears at multiple cohorts through two paths. Total impact = 2.0
    (bg_A demands bg_S once directly and once through bg_R, coefficients 1).
    """
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}}
    )
    co2 = biosphere.get("CO2")

    foreground = bd.Database("foreground")
    foreground.register()
    bg20 = bd.Database("background_2020")
    bg20.register()
    bg30 = bd.Database("background_2030")
    bg30.register()

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()

    td = TemporalDistribution(
        date=np.array([0, 8], dtype="timedelta64[Y]"),
        amount=np.array([0.7, 0.3]),
    )
    prod_td_s = TemporalDistribution(
        date=np.array([0, 4], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    variants = {}
    for db in (bg20, bg30):
        bg_a = db.new_node("bg_A", name="bg_A", unit="k"); bg_a["reference product"] = "bg_A"; bg_a.save()
        bg_r = db.new_node("bg_R", name="bg_R", unit="k"); bg_r["reference product"] = "bg_R"; bg_r.save()
        bg_s = db.new_node("bg_S", name="bg_S", unit="k"); bg_s["reference product"] = "bg_S"; bg_s.save()

        bg_a.new_edge(input=bg_a, amount=1, type="production").save()
        bg_r.new_edge(input=bg_r, amount=1, type="production").save()
        ps = bg_s.new_edge(input=bg_s, amount=1, type="production")
        ps["temporal_distribution"] = prod_td_s
        ps.save()

        e1 = bg_a.new_edge(input=bg_s, amount=1, type="technosphere")
        e1["temporal_distribution"] = td
        e1.save()
        e2 = bg_a.new_edge(input=bg_r, amount=1, type="technosphere")
        e2["temporal_distribution"] = td
        e2.save()
        bg_r.new_edge(input=bg_s, amount=1, type="technosphere").save()
        bg_s.new_edge(input=co2, amount=1, type="biosphere").save()
        variants[db.name] = {"bg_A": bg_a, "bg_R": bg_r, "bg_S": bg_s}

    fu.new_edge(input=variants["background_2020"]["bg_A"], amount=1, type="technosphere").save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])
    for dbn in bd.databases:
        bd.Database(dbn).process()
    return variants
```

Register in `tests/conftest.py`:

```python
from .fixtures.background_prod_td_db_fixture import (
    background_prod_td_db,
    background_prod_td_deep_db,
    background_prod_td_convergent_db,
)
```

- [ ] **Step 2: Add the test**

Add to `tests/test_background_production_td.py`:

```python
@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_convergent_production_td_conserves(
    background_prod_td_convergent_db, graph_traversal
):
    t = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    t.build_timeline(
        starting_datetime="2020-01-01",
        temporal_grouping="year",
        graph_traversal=graph_traversal,
        traverse_background=True,
        cutoff=1e-9,
        max_calc=5000,
    )
    t.lci()
    t.static_lcia()
    assert t.static_score == pytest.approx(t.base_lca.score, rel=1e-6)
```

- [ ] **Step 3: Run it**

Run: `.venv/bin/python -m pytest tests/test_background_production_td.py::test_convergent_production_td_conserves -q --no-cov`
Expected: PASS both engines (score `== 2.0`). If it fails, STOP — the convergent case exposes a residual bug; return to systematic-debugging before proceeding.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/background_prod_td_db_fixture.py tests/conftest.py tests/test_background_production_td.py
git commit -m "test: convergent background production-edge TD conserves"
```

---

### Task 6: Cleanup and full verification

**Files:**
- Delete: `tests/test_repro_variant_mismatch.py`
- Verify: whole suite

- [ ] **Step 1: Delete the throwaway repro module**

```bash
git rm tests/test_repro_variant_mismatch.py
```

- [ ] **Step 2: Full suite green**

Run: `.venv/bin/python -m pytest tests/ -q --no-cov`
Expected: all pass, no errors/warnings beyond the pre-existing deprecation noise.

- [ ] **Step 3: Confirm the snap fallback is still present (defense-in-depth)**

Run: `grep -n "_nearest_time_mapping_key" bw_timex/timeline_builder.py`
Expected: the helper and its call in `get_time_mapping_key` are present (added earlier this session). Do not remove.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: remove throwaway repro after production-TD fix lands"
```

---

### Task 7: Premise integration smoke check (manual, not in CI suite)

**Files:** none (manual verification against the `ei312_REMIND_EU` premise project, which is not available in CI).

- [ ] **Step 1: Run the premise diesel case**

With the `ei312_REMIND_EU` project populated (premise `dp312_SSP2_NDC_*` dbs + `add_premise_temporal_distributions(BG_DBS)`), build a foreground referencing a premise diesel transport activity and run:

```python
t.build_timeline(starting_datetime="2050-01-01", temporal_grouping="year",
                 graph_traversal="bfs", traverse_background=True,
                 cutoff=1e-3, max_calc=2000)
t.lci(); t.static_lcia()
```

Expected: completes without `KeyError`/`NonsquareTechnosphere`; `static_score` is finite and stable across a re-run. (Premise backgrounds carry no production-edge TDs, so this primarily confirms the snap still guards the separate dual-path facet and nothing regressed. If it raises `Found N exchanges` in `_get_exchange`, that is an unrelated pre-existing limitation, not this fix.)

- [ ] **Step 2: Record the observed score** in the PR description; no commit.

---

## Notes for the implementer

- After each `edge_extractor.py` edit, if the conservation test still shows N× or a new `KeyError` appears, inspect the timeline directly: build with `cutoff=1e-9`, print `t.timeline[["producer_name","date_producer","consumer_name","date_consumer","amount"]]`, and check that each production-TD producer's `date_producer` set equals its `date_consumer` set and that per-cohort amounts carry the production-TD weights (e.g. `0.6*0.5, 0.6*0.3, 0.6*0.2`).
- Do not `.simplify()` the folded `td_producer`/`abs_td_producer`/`distribution` at the emit sites — simplify can merge entries and break the index alignment `extract_edge_data` relies on.
- The premise dual-path `KeyError` is explicitly out of scope; the snap fallback handles it for now.
```
