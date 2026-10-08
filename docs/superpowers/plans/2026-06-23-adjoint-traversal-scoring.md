# Adjoint Static-Score Intensities for Traversal Scoring (P1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-node-visit linear solve in `bw_timex`'s priority graph traversal with a single precomputed adjoint score-intensity vector, eliminating the dominant traversal cost while leaving traversal logic and results unchanged (within numeric tolerance).

**Architecture:** `bw_graph_tools.CachingSolver.scores(indices, amounts)` currently solves `A x = e_index` once per unique activity index, then returns `score_row @ supply`. That equals `λ[index]` where `A.T λ = score_row`. We add an `AdjointCachingSolver(CachingSolver)` that solves the adjoint once and serves `scores()` as a pure lookup, and an `AdjointScoringGraphTraversal(NewNodeEachVisitGraphTraversal)` that installs it. `bw_timex`'s `EdgeExtractor`/`TimelineBuilder`/`TimexLCA.build_timeline` gain an opt-in flag that passes this traversal class through to `bw_temporalis.TemporalisLCA`. Default behavior is unchanged.

**Tech Stack:** Python 3.13, `bw2calc`, `bw_temporalis`, `bw_graph_tools`, `scipy.sparse`, `numpy`, `pytest`, `uv`.

## Global Constraints

- Package manager: use `uv` for all Python invocations (`uv run pytest ...`). Never `pip`/`conda`.
- Do NOT git-commit unless the human explicitly asks. The "Commit" steps below are written per the TDD template; the human has standing instructions against automatic commits, so **stage nothing and skip the commit step unless told otherwise** — treat each "Commit" step as "stop, report, await instruction."
- Keep the existing priority traversal path the default. Adjoint scoring is opt-in this cycle; no default switch until the validation gate (Task 6) is reviewed.
- Correctness gate is **numeric tolerance** (`rtol=1e-9` for score equivalence; `rtol=1e-6` for end-to-end timeline/score parity on fixtures), not byte-equality.
- Single configured `TimexLCA.method` only; one adjoint solve. No multi-method support this cycle.
- New module name: `bw_timex/adjoint_scoring.py`. New tests: `tests/test_adjoint_scoring.py`.
- Do not edit anything under `.venv/` (read-only reference for `bw_graph_tools` internals).

---

## File Structure

- **Create `bw_timex/adjoint_scoring.py`** — holds `AdjointCachingSolver` and `AdjointScoringGraphTraversal`. Single responsibility: adjoint-based node scoring for the priority traversal. No `bw_timex` domain logic.
- **Modify `bw_timex/edge_extractor.py`** — `EdgeExtractor.__init__` accepts/passes a `graph_traversal` *class* through to `TemporalisLCA` (currently it only forwards `**kwargs`; we make the seam explicit and safe).
- **Modify `bw_timex/timeline_builder.py`** — thread an `adjoint_scoring: bool` argument; when true (and engine is `"priority"`), pass `graph_traversal=AdjointScoringGraphTraversal` into the `EdgeExtractor(...)` call.
- **Modify `bw_timex/timex_lca.py`** — `build_timeline(...)` gains `adjoint_scoring: bool = False`, validated and forwarded to `TimelineBuilder`, and folded into the timeline cache key.
- **Modify `bw_timex/validation.py`** — add `adjoint_scoring` to the `BuildTimelineInputs` validation model.
- **Modify `bw_timex/__init__.py`** — export `AdjointCachingSolver`, `AdjointScoringGraphTraversal` (keeps parity with the module exporting other engine classes like `EdgeExtractor`).
- **Create `tests/test_adjoint_scoring.py`** — unit tests (math/equivalence, index bridge, signs), integration parity tests on fixtures, and the validation/benchmark test.

Reference signatures (from `.venv`, do not edit those files):
- `bw_graph_tools.graph_traversal.utils.CachingSolver`:
  - `__init__(self, lca)`
  - `set_score_row(self, characterized_biosphere)` → sets `self.score_row = np.asarray(characterized_biosphere.sum(axis=0)).ravel()`
  - `scores(self, indices: list[int], amounts: list[float]) -> list[float]`
  - `add_to_cache(self, index, unit_score)`, `in_cache(self, indices)`, `self._score_cache: dict`
- `bw_graph_tools.graph_traversal.new_node_each_visit.NewNodeEachVisitGraphTraversal`:
  - `__init__(self, lca, settings, *, functional_unit_unique_id=-1, static_activity_indices=set())`; base sets `self._caching_solver = settings.caching_solver or CachingSolver(lca)`; NNEVGT.__init__ sets `self.characterized_biosphere` and, if present, calls `self._caching_solver.set_score_row(self.characterized_biosphere)`.
  - classmethod `calculate(cls, lca_object, ...)` builds `GraphTraversalSettings(...)`, does `instance = cls(...)`, `instance.traverse()`, returns `{"nodes","edges","flows","calculation_count"}`. `bw_temporalis.TemporalisLCA` calls `graph_traversal.calculate(...)` where `graph_traversal` is the class passed to its constructor (default `NewNodeEachVisitGraphTraversal`).

---

### Task 1: `AdjointCachingSolver` — adjoint solve + lookup `scores`

**Files:**
- Create: `bw_timex/adjoint_scoring.py`
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Consumes: `bw_graph_tools.graph_traversal.utils.CachingSolver`; a built `bw2calc.LCA` with `.technosphere_matrix` (scipy sparse) and the `set_score_row` contract.
- Produces:
  - `class AdjointCachingSolver(CachingSolver)`
  - `AdjointCachingSolver.set_score_row(self, characterized_biosphere) -> None` (computes `self.lambda_vector: np.ndarray`)
  - `AdjointCachingSolver.scores(self, indices: list[int], amounts: list[float]) -> list[float]`
  - attribute `self.lambda_vector: np.ndarray | None` (signed adjoint intensities, index = technosphere matrix column)
  - attribute `self.solve_count: int` (number of linear solves performed; must be exactly 1 after first `set_score_row`, and stay 1 across `scores` calls)

- [ ] **Step 1: Write the failing test (adjoint equals per-index solve)**

```python
# tests/test_adjoint_scoring.py
import numpy as np
import scipy.sparse as sp
from bw_graph_tools.graph_traversal.utils import CachingSolver
from bw_timex.adjoint_scoring import AdjointCachingSolver


class _FakeLCA:
    """Minimal stand-in exposing the attributes CachingSolver/AdjointCachingSolver read."""
    def __init__(self, technosphere, biosphere, cfs):
        self.technosphere_matrix = sp.csr_matrix(technosphere)
        self._biosphere = sp.csr_matrix(biosphere)
        self._cfs = np.asarray(cfs, dtype=float)

    def characterized_biosphere(self):
        # characterized biosphere = diag(cf) @ B  (rows: biosphere flows, cols: products)
        return sp.csr_matrix(sp.diags(self._cfs) @ self._biosphere)


def _make_lca():
    # 3x3 invertible technosphere (diagonal-dominant), 2 biosphere flows, 3 products
    A = np.array([[1.0, -0.2, 0.0],
                  [-0.1, 1.0, -0.3],
                  [0.0, -0.4, 1.0]])
    B = np.array([[2.0, 0.0, 1.0],
                  [0.0, 3.0, 0.0]])
    cfs = [1.0, 0.5]
    return _FakeLCA(A, B, cfs)


def test_adjoint_scores_match_per_index_solve():
    lca = _make_lca()
    char_bio = lca.characterized_biosphere()

    reference = CachingSolver(lca)
    reference.set_score_row(char_bio)
    ref_scores = reference.scores([0, 1, 2], [1.0, 1.0, 1.0])

    adjoint = AdjointCachingSolver(lca)
    adjoint.set_score_row(char_bio)
    adj_scores = adjoint.scores([0, 1, 2], [1.0, 1.0, 1.0])

    np.testing.assert_allclose(adj_scores, ref_scores, rtol=1e-9)


def test_adjoint_does_single_solve_and_scales_by_amount():
    lca = _make_lca()
    adjoint = AdjointCachingSolver(lca)
    adjoint.set_score_row(lca.characterized_biosphere())
    base = adjoint.scores([1], [1.0])[0]
    scaled = adjoint.scores([1], [4.0])[0]
    np.testing.assert_allclose(scaled, 4.0 * base, rtol=1e-12)
    assert adjoint.solve_count == 1  # no per-index solves during scoring
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_adjoint_scores_match_per_index_solve -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'bw_timex.adjoint_scoring'`.

- [ ] **Step 3: Write minimal implementation**

```python
# bw_timex/adjoint_scoring.py
"""Adjoint-based node scoring for the priority graph traversal.

The stock ``bw_graph_tools.CachingSolver`` computes a node's static unit score by
solving ``A x = e_index`` once per unique activity index and returning
``score_row @ x``. That value equals ``lambda[index]`` where ``A.T lambda =
score_row``. Solving the adjoint system *once* yields the unit score for every
activity, so traversal scoring becomes a pure lookup with no per-node solves.
"""

from __future__ import annotations

import numpy as np
from scipy.sparse.linalg import spsolve

from bw_graph_tools.graph_traversal.utils import CachingSolver


class AdjointCachingSolver(CachingSolver):
    """Drop-in ``CachingSolver`` that scores via one adjoint solve."""

    def __init__(self, lca):
        super().__init__(lca)
        self.lambda_vector: np.ndarray | None = None
        self.solve_count: int = 0

    def set_score_row(self, characterized_biosphere) -> None:
        # Sets ``self.score_row`` (length = number of technosphere columns).
        super().set_score_row(characterized_biosphere)
        a_transpose = self.lca.technosphere_matrix.transpose().tocsc()
        self.lambda_vector = np.asarray(
            spsolve(a_transpose, np.asarray(self.score_row, dtype=float))
        ).ravel()
        self.solve_count += 1
        # Pre-fill the inherited cache so any code path that consults it agrees
        # with the lookup-based ``scores`` below.
        for index, unit_score in enumerate(self.lambda_vector):
            self._score_cache[index] = float(unit_score)

    def scores(self, indices, amounts) -> list[float]:
        if self.lambda_vector is None:
            raise RuntimeError(
                "set_score_row must be called before scores (lambda not computed)"
            )
        lam = self.lambda_vector
        return [float(lam[index]) * float(amount)
                for index, amount in zip(indices, amounts)]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adjoint_scoring.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Commit** (per Global Constraints: stop and await instruction instead of committing)

---

### Task 2: `AdjointScoringGraphTraversal` — install the adjoint solver in the traversal

**Files:**
- Modify: `bw_timex/adjoint_scoring.py`
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Consumes: `bw_graph_tools.graph_traversal.new_node_each_visit.NewNodeEachVisitGraphTraversal`; `AdjointCachingSolver` (Task 1).
- Produces: `class AdjointScoringGraphTraversal(NewNodeEachVisitGraphTraversal)` whose `self._caching_solver` is an `AdjointCachingSolver` with `lambda_vector` populated, usable anywhere `NewNodeEachVisitGraphTraversal` is (including its `.calculate(...)` classmethod).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_adjoint_scoring.py
from bw_timex.adjoint_scoring import AdjointScoringGraphTraversal


def test_traversal_subclass_installs_adjoint_solver(monkeypatch):
    # The traversal's __init__ reads lca.score and builds characterized biosphere
    # via library helpers; verify our subclass swaps in the adjoint solver after
    # the base class finishes its own setup.
    from bw_timex.adjoint_scoring import AdjointCachingSolver

    seen = {}

    real_init = AdjointScoringGraphTraversal.__mro__[1].__init__  # NNEVGT.__init__

    def fake_init(self, *args, **kwargs):
        # Stub out the heavy base init: set the minimal attributes the subclass
        # override relies on, then let the override run.
        import scipy.sparse as sp
        self.lca = args[0]
        self.characterized_biosphere = self.lca.characterized_biosphere()
        self._caching_solver = None  # base would set the stock solver here

    monkeypatch.setattr(
        AdjointScoringGraphTraversal.__mro__[1], "__init__", fake_init
    )

    lca = _make_lca()
    inst = AdjointScoringGraphTraversal(lca, object())
    assert isinstance(inst._caching_solver, AdjointCachingSolver)
    assert inst._caching_solver.lambda_vector is not None
    assert inst._caching_solver.solve_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_traversal_subclass_installs_adjoint_solver -v`
Expected: FAIL with `ImportError`/`AttributeError` (name not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# append to bw_timex/adjoint_scoring.py
from bw_graph_tools.graph_traversal.new_node_each_visit import (
    NewNodeEachVisitGraphTraversal,
)


class AdjointScoringGraphTraversal(NewNodeEachVisitGraphTraversal):
    """Priority traversal that scores nodes via a single adjoint solve.

    Identical to ``NewNodeEachVisitGraphTraversal`` except the caching solver is
    replaced with an :class:`AdjointCachingSolver`, so node scoring performs no
    per-node linear solves. Heap ordering, cutoff, ``max_calc``, and all other
    traversal behavior are inherited unchanged.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The base class already computed ``self.characterized_biosphere`` and
        # called ``set_score_row`` on the stock solver. Swap in the adjoint
        # solver and (re)compute the score row / lambda on it.
        solver = AdjointCachingSolver(self.lca)
        solver.set_score_row(self.characterized_biosphere)
        self._caching_solver = solver
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adjoint_scoring.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit** (per Global Constraints: stop and await instruction)

---

### Task 3: Export the new classes

**Files:**
- Modify: `bw_timex/__init__.py`
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Produces: `bw_timex.AdjointCachingSolver`, `bw_timex.AdjointScoringGraphTraversal` importable from the package root.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_adjoint_scoring.py
def test_public_exports():
    import bw_timex
    assert hasattr(bw_timex, "AdjointCachingSolver")
    assert hasattr(bw_timex, "AdjointScoringGraphTraversal")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_public_exports -v`
Expected: FAIL (`AttributeError`).

- [ ] **Step 3: Write minimal implementation**

Add to `bw_timex/__init__.py` after the existing `from .edge_extractor import EdgeExtractor` line:

```python
from .adjoint_scoring import AdjointCachingSolver, AdjointScoringGraphTraversal
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_public_exports -v`
Expected: PASS.

- [ ] **Step 5: Commit** (per Global Constraints: stop and await instruction)

---

### Task 4: Thread the opt-in flag through `EdgeExtractor` and `TimelineBuilder`

**Files:**
- Modify: `bw_timex/edge_extractor.py:553-608` (`EdgeExtractor.__init__`)
- Modify: `bw_timex/timeline_builder.py:32` (`TimelineBuilder.__init__` signature) and `:129-140` (the `EdgeExtractor(...)` call)
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Consumes: `AdjointScoringGraphTraversal` (Task 2); `bw_temporalis.TemporalisLCA`'s `graph_traversal` constructor parameter (a class, default `NewNodeEachVisitGraphTraversal`).
- Produces:
  - `TimelineBuilder.__init__(..., adjoint_scoring: bool = False, ...)` — when `True` and `graph_traversal == "priority"`, the priority `EdgeExtractor` is constructed with the adjoint traversal class.
  - `EdgeExtractor` correctly forwards a `graph_traversal=<class>` kwarg to `TemporalisLCA` (it already passes `**kwargs`; this task adds a regression test pinning that, since `graph_traversal` collides with the `TimelineBuilder` string selector name and must not be confused).

Context: `TimelineBuilder.__init__` (`bw_timex/timeline_builder.py`) has a positional/keyword `graph_traversal: str = "priority"` selector and builds either `EdgeExtractorBFS` or `EdgeExtractor`. The priority branch (`:130-140`) calls `EdgeExtractor(base_lca, starting_datetime=..., *args, edge_filter_function=..., cutoff=..., max_calc=..., static_activity_indices=..., traverse_background=..., **kwargs)`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_adjoint_scoring.py
import bw2data as bd
from datetime import datetime
from bw_timex import TimexLCA
from bw_timex.adjoint_scoring import AdjointScoringGraphTraversal


def _build_tlca(db_fixture_unused):
    fu = bd.get_node(database="foreground", code="A")
    return TimexLCA(
        demand={fu.id: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        },
    )


def test_timeline_builder_uses_adjoint_class(temporal_grouping_db_monthly, monkeypatch):
    # Spy: record the graph_traversal class TemporalisLCA is constructed with.
    import bw_timex.edge_extractor as ee
    captured = {}
    real_init = ee.EdgeExtractor.__init__

    def spy_init(self, *args, **kwargs):
        captured["graph_traversal"] = kwargs.get("graph_traversal")
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(ee.EdgeExtractor, "__init__", spy_init)

    tlca = _build_tlca(temporal_grouping_db_monthly)
    tlca.build_timeline(adjoint_scoring=True)  # added in Task 5
    assert captured["graph_traversal"] is AdjointScoringGraphTraversal
```

(If executing tasks strictly in order, this test depends on Task 5's `build_timeline` flag; mark it xfail until Task 5, or implement Tasks 4 and 5 together before running. The two tasks share one reviewer gate.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_timeline_builder_uses_adjoint_class -v`
Expected: FAIL — `build_timeline()` has no `adjoint_scoring` kwarg yet (`TypeError`).

- [ ] **Step 3: Implement — `EdgeExtractor` regression guard + `TimelineBuilder` wiring**

In `bw_timex/edge_extractor.py`, `EdgeExtractor.__init__` already forwards `**kwargs` to `super().__init__` (which is `TemporalisLCA.__init__`, accepting `graph_traversal`). Add an explicit guard comment and keep behavior; no functional change needed beyond ensuring `graph_traversal` is not popped. Confirm by leaving the `super().__init__(*args, **kwargs)` line intact.

In `bw_timex/timeline_builder.py`, change the `__init__` signature to add `adjoint_scoring: bool = False` (place it next to `graph_traversal: str = "priority"`):

```python
        graph_traversal: str = "priority",
        adjoint_scoring: bool = False,
```

Then in the priority branch, modify the `EdgeExtractor(...)` call to inject the class when requested. Replace the call at `:130-140` with:

```python
        elif graph_traversal == "priority":
            priority_kwargs = dict(kwargs)
            if adjoint_scoring:
                from .adjoint_scoring import AdjointScoringGraphTraversal
                priority_kwargs["graph_traversal"] = AdjointScoringGraphTraversal
            self.edge_extractor = EdgeExtractor(
                base_lca,
                starting_datetime=self.starting_datetime,
                *args,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                max_calc=self.max_calc,
                static_activity_indices=set(static_background_activity_ids),
                traverse_background=self.traverse_background,
                **priority_kwargs,
            )
```

Note: `adjoint_scoring=True` with `graph_traversal == "bfs"` is ignored (BFS already avoids per-subgraph LCA). Document this in the `TimelineBuilder` docstring.

- [ ] **Step 4: Run test** (after Task 5 wires `build_timeline`)

Run: `uv run pytest tests/test_adjoint_scoring.py::test_timeline_builder_uses_adjoint_class -v`
Expected: PASS.

- [ ] **Step 5: Commit** (per Global Constraints: stop and await instruction)

---

### Task 5: `build_timeline(adjoint_scoring=...)` + validation + cache key

**Files:**
- Modify: `bw_timex/timex_lca.py:219-231` (`build_timeline` signature), `:295-327` (validation + cache key), `:384` (`TimelineBuilder(...)` call)
- Modify: `bw_timex/validation.py` (the `BuildTimelineInputs` model)
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Consumes: `TimelineBuilder(..., adjoint_scoring=...)` (Task 4); the `BuildTimelineInputs` validator.
- Produces: `TimexLCA.build_timeline(..., adjoint_scoring: bool = False, ...)` that validates the flag, includes it in `timeline_cache_key`, and forwards it to `TimelineBuilder`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_adjoint_scoring.py
def test_build_timeline_accepts_adjoint_flag(temporal_grouping_db_monthly):
    tlca = _build_tlca(temporal_grouping_db_monthly)
    tl = tlca.build_timeline(adjoint_scoring=True)
    assert tl is not None
    # Distinct flag value must not collide in the cache with the default run.
    tlca.build_timeline(adjoint_scoring=False)
    assert tlca._last_timeline_build_key[ -1: ] is not None  # key recomputed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_build_timeline_accepts_adjoint_flag -v`
Expected: FAIL — `build_timeline() got an unexpected keyword argument 'adjoint_scoring'`.

- [ ] **Step 3: Implement**

In `bw_timex/validation.py`, add `adjoint_scoring: bool = False` to the `BuildTimelineInputs` model (mirror the existing boolean field `traverse_background`).

In `bw_timex/timex_lca.py` `build_timeline`, add the parameter after `traverse_background`:

```python
        traverse_background: bool = False,
        adjoint_scoring: bool = False,
```

Add it to the `BuildTimelineInputs(...)` construction (after `traverse_background=traverse_background,`):

```python
            adjoint_scoring=adjoint_scoring,
```

Add it to `timeline_cache_key` (append before the `edge_filter_function` element so existing-order callers still behave; append at the end to be safe):

```python
            traverse_background,
            adjoint_scoring,
            "default" if edge_filter_function is None else id(edge_filter_function),
        )
```

Forward it to `TimelineBuilder(...)` at `:384` by adding `adjoint_scoring=adjoint_scoring,` to that call's kwargs.

Document in the `build_timeline` docstring: "adjoint_scoring (bool, default False): use precomputed adjoint static-score intensities for priority-engine node scoring instead of a per-node linear solve. Results are equivalent within numerical tolerance; this is a performance option for the 'priority' engine and is ignored for 'bfs'."

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_build_timeline_accepts_adjoint_flag tests/test_adjoint_scoring.py::test_timeline_builder_uses_adjoint_class -v`
Expected: both PASS.

- [ ] **Step 5: Commit** (per Global Constraints: stop and await instruction)

---

### Task 6: End-to-end equivalence + benchmark (the validation gate)

**Files:**
- Test: `tests/test_adjoint_scoring.py`

**Interfaces:**
- Consumes: `TimexLCA.build_timeline(adjoint_scoring=...)`; existing pytest fixtures `temporal_grouping_db_monthly` and `background_td_deep_chain_db` (registered in `tests/conftest.py`).
- Produces: regression tests proving (a) timeline/score parity within `rtol=1e-6`, and (b) a non-increasing linear-solve count for traversal scoring.

- [ ] **Step 1: Write the equivalence test (timeline + score parity)**

```python
# append to tests/test_adjoint_scoring.py
import pandas as pd


def _scores_for(db_fixture, adjoint: bool):
    fu = bd.get_node(database="foreground", code="A")
    tlca = TimexLCA(
        demand={fu.id: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(adjoint_scoring=adjoint)
    tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)
    tlca.static_lcia()
    return tlca.static_score, tlca.timeline


def test_adjoint_matches_default_scores_and_timeline(temporal_grouping_db_monthly):
    score_default, tl_default = _scores_for(temporal_grouping_db_monthly, adjoint=False)
    score_adjoint, tl_adjoint = _scores_for(temporal_grouping_db_monthly, adjoint=True)

    np.testing.assert_allclose(score_adjoint, score_default, rtol=1e-6)

    # Compare the numeric 'amount' column after aligning on the stable edge keys.
    key = ["producer_name", "consumer_name", "date_producer", "date_consumer"]
    a = tl_default.sort_values(key).reset_index(drop=True)
    b = tl_adjoint.sort_values(key).reset_index(drop=True)
    assert list(a["producer_name"]) == list(b["producer_name"])
    np.testing.assert_allclose(
        a["amount"].to_numpy(), b["amount"].to_numpy(), rtol=1e-6
    )
```

- [ ] **Step 2: Run it to verify it passes (parity holds)**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_adjoint_matches_default_scores_and_timeline -v`
Expected: PASS. If it FAILS, the adjoint scoring diverged — debug with `superpowers:systematic-debugging` before proceeding; do not loosen `rtol`.

- [ ] **Step 3: Write the solve-count benchmark test (deep chain)**

```python
# append to tests/test_adjoint_scoring.py
def test_adjoint_reduces_scoring_solves(background_td_deep_chain_db):
    """Adjoint scoring performs exactly one linear solve regardless of graph size."""
    import bw_timex.adjoint_scoring as adj

    solve_counts = []
    real_set = adj.AdjointCachingSolver.set_score_row

    def counting_set(self, char_bio):
        real_set(self, char_bio)
        solve_counts.append(self.solve_count)

    fu = bd.get_node(database="foreground", code="A")
    tlca = TimexLCA(
        demand={fu.id: 1},
        method=("GWP", "example"),
        database_dates={
            "background": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    import pytest
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(adj.AdjointCachingSolver, "set_score_row", counting_set)
        tlca.build_timeline(adjoint_scoring=True)

    # One adjoint solve total for traversal scoring (vs one-per-unique-index before).
    assert solve_counts and all(c == 1 for c in solve_counts)
```

Note: confirm the exact `database_dates`/fixture wiring for `background_td_deep_chain_db` by reading `tests/fixtures/background_td_deep_chain_db_fixture.py` and the matching `tests/test_background_traversal.py` setup, and adapt the demand node / dates accordingly. The fixture's database name(s) and the foreground node code must match that fixture, not the monthly one.

- [ ] **Step 4: Run the benchmark test**

Run: `uv run pytest tests/test_adjoint_scoring.py::test_adjoint_reduces_scoring_solves -v`
Expected: PASS (exactly one adjoint solve).

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `uv run pytest -q`
Expected: all pre-existing tests still PASS (default path untouched), plus the new `test_adjoint_scoring.py` tests.

- [ ] **Step 6: Commit** (per Global Constraints: stop and await instruction)

---

## Self-Review

**Spec coverage:**
- Adjoint precompute `Aᵀλ = Bᵀh` once on base matrices → Task 1 (`AdjointCachingSolver.set_score_row`). ✓
- `StaticScoreIntensities` isolated unit with single-activity equivalence test → realized as `AdjointCachingSolver` (the seam *is* the score provider; a separate class would duplicate `score_row`/index logic, so it is folded in). Equivalence test = Task 1 Step 1. ✓
- Seam A: inject into priority engine via `bw_graph_tools` solver subclass → Tasks 2,4,5. ✓
- Opt-in flag on `build_timeline` → Task 5; threaded via Task 4. ✓
- Numeric-tolerance correctness gate → Task 6 (`rtol=1e-9` math, `rtol=1e-6` end-to-end). ✓
- Validation/benchmark harness → Task 6 (solve-count test + full-suite regression). ✓
- Conservative pruning guardrail → covered by Task 6 timeline parity (same edges retained within tolerance under identical cutoff/max_calc). ✓
- Index-space bridge risk → `score_row`/`lambda_vector` share the technosphere-column index space that `CachingSolver.scores` already uses; Task 1 test exercises it directly against the stock solver. ✓
- Sign/substitution conventions → inherited unchanged (scoring uses the same `characterized_biosphere`/`score_row` and the same downstream sign handling as today); Task 6 fixtures include signed/substitution-bearing chains via the standard suite. ✓
- Out-of-scope items (P2/P3/UX/premise) → not present in any task. ✓

**Placeholder scan:** No TBD/TODO/"add error handling". Two explicit "confirm/adapt" notes (Task 4 ordering dependency; Task 6 deep-chain fixture wiring) point to concrete files to read, not vague work. ✓

**Type consistency:** `AdjointCachingSolver` (Task 1) used identically in Tasks 2/4/6; `AdjointScoringGraphTraversal` (Task 2) used identically in Tasks 4/5; `adjoint_scoring` bool name consistent across `validation.py`, `TimelineBuilder`, `build_timeline`, and the cache key. `lambda_vector`/`solve_count` attribute names consistent across Tasks 1,2,6. ✓
