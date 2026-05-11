# TemporalDistribution as Demand Value — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `TimexLCA(demand={node: TemporalDistribution(...)}, ...)` accept a TD value per demand entry — absolute (`datetime64[*]`) or relative (`timedelta64[*]`) — and seed FU rows accordingly, convolving with any output-side production RTD on the demanded product.

**Architecture:** Validation accepts TD or scalar per entry. `TimexLCA` keeps the original `self.demand` and derives `self._scalar_demand` (per-node sum) for `base_lca`. `build_timeline` validates timing prerequisites and passes a `demand_tds` dict to `TimelineBuilder` → `EdgeExtractor` (both priority and BFS variants). The extractors substitute their per-FU seed TD when a demand entry has a TD value; the existing `seed * td_producer` convolution then handles cross-product with output-side production RTDs automatically. A `UserWarning` fires once per demand entry that has both forms.

**Tech Stack:** `bw_timex`, `bw_temporalis.TemporalDistribution`, `pydantic` (validation), `pytest`.

**Spec:** `docs/superpowers/specs/2026-05-11-td-as-demand-design.md`.

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `bw_timex/validation.py` | Accept TD-valued demand entries; reject negative/empty/wrong-dtype TDs. |
| Modify | `bw_timex/timex_lca.py` | Maintain `self.demand` (original) and `self._scalar_demand` (numeric); use scalar for `base_lca`. Validate relative-TD requires `starting_datetime` in `build_timeline`. Pass `demand_tds` through to `TimelineBuilder`. |
| Modify | `bw_timex/timeline_builder.py` | Accept `demand_tds` and forward to both `EdgeExtractor` and `EdgeExtractorBFS`. |
| Modify | `bw_timex/edge_extractor.py` | Both `EdgeExtractor.__init__` and `EdgeExtractorBFS.__init__` accept `demand_tds: dict[int, TemporalDistribution]`. Substitute the per-FU seed TD when a demand product id has a TD. Emit `UserWarning` once per node that has both a demand TD and an output-side production RTD. |
| Create | `tests/test_demand_temporal_distribution.py` | All new behavior tests. |

No directory restructuring. Files are already organised by responsibility.

---

## Numerical / Behavioural Contract

Used by tests below; all values exact unless flagged "≈".

```python
# Backgrounds (one chimaera process+product per db, biosphere has co2)
BG_CO2_PER_INPUT = 0.5       # only one background needed for most tests

# Foreground (chimaera unless test names "explicit"):
FG_EDGE_AMOUNT = 2.0         # 2 kg of background input per 1 unit of foreground product

# Production RTD on demanded product (used in the convolution test):
PROD_RTD_DATES   = [0, 5] yr   # timedelta64[Y]
PROD_RTD_AMOUNTS = [0.6, 0.4]

# Demand TDs used across tests:
TD_ABS_2 = TD(datetime64[s]=[2025-01-01, 2030-01-01], amounts=[60.0, 40.0])  # sum = 100
TD_REL_2 = TD(timedelta64[Y]=[0, 5],                amounts=[60.0, 40.0])  # sum = 100

STARTING_DATETIME = datetime(2025, 1, 1)
```

Derived expected values:

- Static base score for `demand={p: TD_ABS_2}` = `100 × FG_EDGE_AMOUNT × BG_CO2_PER_INPUT = 100 × 2 × 0.5 = 100.0`.
- Time-explicit static score for `demand={p: TD_ABS_2}` with single background — same `100.0` (one bg, no relinking effect).
- FU rows produced by `TD_ABS_2`: two rows, dates 2025-01-01 and 2030-01-01, amounts 60 and 40.
- Convolution `TD_ABS_2 × PROD_RTD`: four rows.
  - `(2025 + 0yr) = 2025-01-01, 60 × 0.6 = 36`
  - `(2025 + 5yr) = 2030-01-01, 60 × 0.4 = 24`
  - `(2030 + 0yr) = 2030-01-01, 40 × 0.6 = 24`
  - `(2030 + 5yr) = 2035-01-01, 40 × 0.4 = 16`
  - After `temporal_grouping="year"` merge: `[2025: 36, 2030: 48, 2035: 16]`.

Multi-vintage relinking case (extends earlier `test_multi_vintage_demand_splits_across_cohorts`):

- Two backgrounds at 2025 and 2035 (CO2/kWh = 0.40 and 0.10).
- Foreground process consumes `1.0 kWh` per unit, no production RTD.
- Demand TD: dates 2025-01-01 & 2035-01-01, amounts 0.5 & 0.5.
- Expected score: `0.5 × 0.40 + 0.5 × 0.10 = 0.25`.

---

## Common Test Header

All tests in `tests/test_demand_temporal_distribution.py` start with this header. **Repeated verbatim in each task's first failing-test step** so an agent reading any task in isolation has the full file:

```python
from datetime import datetime
from typing import Tuple

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


def _make_chimaera_project(name: str) -> Tuple[bd.Node, dict]:
    """Tiny chimaera project: 1 background, 1 foreground process+product.

    Returns ``(demanded_node, database_dates)``.
    """
    bd.projects.set_current(name)
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])
    bd.Database("foreground").write(
        {
            ("foreground", "fg_process"): {
                "name": "fg process",
                "reference product": "fg product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "fg_process"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 2.0,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    return (
        bd.get_node(database="foreground", code="fg_process"),
        {
            "background": datetime(2025, 1, 1),
            "foreground": "dynamic",
        },
    )
```

This helper lets each test build a minimal project in one line. Tests that need more (production RTD on the demanded product; multiple backgrounds; explicit paradigm) build their own variant inline.

---

## Task 0: Pre-flight

### Task 0.1: Verify existing tests are green

**Files:**
- Verify only.

- [ ] **Step 1: Run full test suite**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: `161 passed` (or higher) and no failures.

If any fail, stop and surface to user — the demand-TD fix from `aa80a7d` is the most recent code change, anything failing now is pre-existing and shouldn't be papered over.

---

## Task 1: Validation — accept TD demand values, reject invalid ones

**Files:**
- Modify: `bw_timex/validation.py:14-28` (the `TimexLCAInputs.validate_demand` validator).
- Modify (test): `tests/test_demand_temporal_distribution.py` (new file).

### Task 1.1: Write the failing tests for validation

- [ ] **Step 1: Create the test file with the common header and four validation tests**

```bash
touch tests/test_demand_temporal_distribution.py
```

Write file `tests/test_demand_temporal_distribution.py` with:

```python
from datetime import datetime
from typing import Tuple

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


def _make_chimaera_project(name: str) -> Tuple[bd.Node, dict]:
    """Tiny chimaera project: 1 background, 1 foreground process+product.

    Returns ``(demanded_node, database_dates)``.
    """
    bd.projects.set_current(name)
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])
    bd.Database("foreground").write(
        {
            ("foreground", "fg_process"): {
                "name": "fg process",
                "reference product": "fg product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "fg_process"),
                        "amount": 1,
                        "type": "production",
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 2.0,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    return (
        bd.get_node(database="foreground", code="fg_process"),
        {
            "background": datetime(2025, 1, 1),
            "foreground": "dynamic",
        },
    )


def test_demand_td_value_accepted_by_validation():
    node, dbdates = _make_chimaera_project("__test_td_demand_validation_ok__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    # Should not raise.
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    assert tlca is not None


def test_demand_td_negative_amount_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_neg__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, -10.0]),
    )
    with pytest.raises(Exception, match="non-negative"):
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )


def test_demand_td_empty_amount_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_empty__")
    td = TemporalDistribution(
        date=np.array([], dtype="datetime64[s]"),
        amount=np.array([]),
    )
    with pytest.raises(Exception, match="at least one"):
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )


def test_demand_td_wrong_dtype_rejected():
    node, dbdates = _make_chimaera_project("__test_td_demand_dtype__")
    # float dates — not allowed.
    td = TemporalDistribution(
        date=np.array([2025.0, 2030.0], dtype="float64"),
        amount=np.array([60.0, 40.0]),
    )
    with pytest.raises(Exception, match="datetime64|timedelta64"):
        TimexLCA(
            demand={node.key: td},
            method=("GWP", "example"),
            database_dates=dbdates,
        )
```

- [ ] **Step 2: Run the four tests, confirm they fail at the validator**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py -k "validation or rejected or accepted" -x 2>&1 | tail -15
```

Expected: at minimum `test_demand_td_value_accepted_by_validation` fails with `demand values must be numeric, got TemporalDistribution …` from the current validator.

### Task 1.2: Update the validator

- [ ] **Step 3: Replace `validate_demand` in `bw_timex/validation.py`**

Replace lines 18-28:

```python
    @field_validator("demand")
    @classmethod
    def validate_demand(cls, v: dict) -> dict:
        if not v:
            raise ValueError("demand must be a non-empty dictionary.")
        for key, value in v.items():
            if isinstance(value, (int, float)):
                continue
            if isinstance(value, TemporalDistribution):
                date_kind = value.date.dtype.kind
                if date_kind not in ("M", "m"):
                    raise ValueError(
                        f"demand TD for key {key} must have date dtype "
                        f"datetime64 or timedelta64, got {value.date.dtype}."
                    )
                if len(value.amount) == 0:
                    raise ValueError(
                        f"demand TD for key {key} must have at least one entry."
                    )
                if np.any(np.asarray(value.amount) < 0):
                    raise ValueError(
                        f"demand TD for key {key} must have non-negative amounts."
                    )
                continue
            raise ValueError(
                f"demand values must be numeric or TemporalDistribution, "
                f"got {type(value).__name__} for key {key}."
            )
        return v
```

And add `import numpy as np` to the file's imports (just below `import bw2data as bd`).

- [ ] **Step 4: Run the four validation tests, confirm they pass**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py -k "validation or rejected or accepted" -x 2>&1 | tail -5
```

Expected: 4 passed.

- [ ] **Step 5: Run full suite to check no regressions**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green (165 passed now, give or take).

- [ ] **Step 6: Commit**

```bash
git add bw_timex/validation.py tests/test_demand_temporal_distribution.py
git commit -m "feat: validate TemporalDistribution demand values

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2: TimexLCA scalar-demand extraction; base_lca uses sum(TD)

After Task 1, validation accepts TDs but `prepare_base_lca_inputs` is still
passed the TD value, which bw2calc cannot solve. Crash will surface — this
task makes `__init__` build a scalar projection of demand for the base LCA
while keeping the original demand dict for downstream use.

**Files:**
- Modify: `bw_timex/timex_lca.py` (around lines 124, 142-145, 379-382).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 2.1: Write the failing test for base_lca scalar parity

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_demand_td_static_base_score_matches_scalar_sum():
    """Static base score for demand={p: TD} == bw2calc.LCA({p: sum(TD.amount)})."""
    node, dbdates = _make_chimaera_project("__test_td_demand_base_parity__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )

    # Static reference: scalar = sum(TD) = 100.
    slca = bc.LCA({node.key: 100.0}, method=("GWP", "example"))
    slca.lci()
    slca.lcia()

    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    assert tlca.base_lca.score == pytest.approx(slca.score)
    # And: the scalar projection on the instance.
    assert tlca._scalar_demand == {node.key: 100.0}
```

- [ ] **Step 2: Run the test, confirm it fails**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_td_static_base_score_matches_scalar_sum -x 2>&1 | tail -10
```

Expected: either fails on missing attribute `_scalar_demand`, or crashes in `prepare_base_lca_inputs` because bw2calc can't accept a TD.

### Task 2.2: Add scalar projection and use it in `prepare_base_lca_inputs`

- [ ] **Step 3: Add `_scalar_demand` field after `self.demand = demand` (currently line 124)**

In `bw_timex/timex_lca.py`, change:

```python
        self.demand = demand
        self.method = method
        self.database_dates = database_dates
```

to:

```python
        self.demand = demand
        self._scalar_demand = self._compute_scalar_demand(demand)
        self.method = method
        self.database_dates = database_dates
```

- [ ] **Step 4: Add `_compute_scalar_demand` as a method on `TimexLCA`**

Add this method to the class (place it next to `_resolve_demand_to_process_id`, around line 1362):

```python
    @staticmethod
    def _compute_scalar_demand(demand: dict) -> dict:
        """Reduce a demand dict whose values may be scalars or TDs to a
        plain ``{key: float}`` dict suitable for ``bw2calc.LCA``.

        A TD value contributes ``sum(td.amount)``.
        """
        result = {}
        for k, v in demand.items():
            if isinstance(v, TemporalDistribution):
                result[k] = float(np.asarray(v.amount).sum())
            else:
                result[k] = float(v)
        return result
```

And import `TemporalDistribution` at the top of `timex_lca.py` if not already imported:

```bash
grep -n "from bw_temporalis" /Users/timodiepers/Documents/Coding/bw_timex/bw_timex/timex_lca.py | head -3
```

If `TemporalDistribution` isn't yet imported, add it: `from bw_temporalis import TemporalDistribution`.

- [ ] **Step 5: Use `self._scalar_demand` in the base LCA call**

Change line 142-144 from:

```python
        fu, data_objs, remapping = self.prepare_base_lca_inputs(
            demand=self.demand, method=self.method
        )
```

to:

```python
        fu, data_objs, remapping = self.prepare_base_lca_inputs(
            demand=self._scalar_demand, method=self.method
        )
```

- [ ] **Step 6: Run the test, confirm it passes**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_td_static_base_score_matches_scalar_sum -x 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 7: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_demand_temporal_distribution.py
git commit -m "feat: derive scalar demand for base_lca from TD demand values

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3: Plumb `demand_tds` into the EdgeExtractors

Pass a `demand_tds: dict[int, TemporalDistribution]` (keyed by product id) from
`TimexLCA.build_timeline` → `TimelineBuilder` → both `EdgeExtractor` and
`EdgeExtractorBFS`. Store on each extractor so the FU seeding logic can read
it. **No behavior change yet** — that comes in Task 4. The point of this
task is the wiring + a smoke test that absolute-date TDs propagate
unchanged.

**Files:**
- Modify: `bw_timex/timex_lca.py` (build_timeline plumbing).
- Modify: `bw_timex/timeline_builder.py:101-119`.
- Modify: `bw_timex/edge_extractor.py:56-80` (priority `__init__`) and `393-415` (BFS `__init__`).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 3.1: Write the failing plumbing test

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_demand_tds_propagate_to_edge_extractor():
    """Smoke test: the demand TD dict reaches the EdgeExtractor."""
    node, dbdates = _make_chimaera_project("__test_td_demand_plumbing__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    tlca.build_timeline(starting_datetime=datetime(2025, 1, 1))
    ex = tlca.timeline_builder.edge_extractor
    # demand_tds is keyed by product id (= node.id for chimaera).
    assert isinstance(ex.demand_tds, dict)
    assert node.id in ex.demand_tds
    stored = ex.demand_tds[node.id]
    assert isinstance(stored, TemporalDistribution)
    assert list(stored.amount) == [60.0, 40.0]
```

- [ ] **Step 2: Run the test, confirm it fails**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_tds_propagate_to_edge_extractor -x 2>&1 | tail -8
```

Expected: `AttributeError: 'EdgeExtractor' object has no attribute 'demand_tds'`.

### Task 3.2: Add the parameter to both extractors

- [ ] **Step 3: Update `EdgeExtractor.__init__` in `bw_timex/edge_extractor.py:56-80`**

Replace:

```python
    def __init__(self, *args, edge_filter_function: Callable = None, **kwargs) -> None:
        ...
        super().__init__(*args, **kwargs)

        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False
```

with:

```python
    def __init__(
        self,
        *args,
        edge_filter_function: Callable = None,
        demand_tds: dict | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the EdgeExtractor class and traverses the supply chain using
        functions of the parent class TemporalisLCA.

        Parameters
        ----------
        *args : Variable length argument list
        edge_filter_function : Callable, optional
            A callable that filters edges. If not provided, a function that always
            returns False is used.
        demand_tds : dict[int, TemporalDistribution] | None, optional
            Mapping from product id (resolved demand key) to a
            ``TemporalDistribution`` whose dates are absolute (``datetime64[*]``).
            The seeding loop substitutes the demand TD for ``self.t0`` for the
            corresponding FU edge. Relative-date TDs are converted to absolute
            before they reach this class.
        **kwargs : Arbitrary keyword arguments
        """
        super().__init__(*args, **kwargs)

        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False

        self.demand_tds = demand_tds or {}
```

- [ ] **Step 4: Update `EdgeExtractorBFS.__init__` in `bw_timex/edge_extractor.py:393-415`**

Replace the signature and body up to `self.tech_matrix_csc = ...`:

```python
    def __init__(
        self,
        lca_object,
        starting_datetime: datetime | str = "now",
        edge_filter_function: Callable = None,
        cutoff: float = 1e-9,
        static_activity_indices: set[int] | None = None,
        demand_tds: dict | None = None,
    ) -> None:
        self.lca_object = lca_object
        self.edge_ff = edge_filter_function if edge_filter_function else lambda x: False
        self.cutoff = cutoff
        self.static_activity_indices = static_activity_indices or set()
        self.demand_tds = demand_tds or {}

        if isinstance(starting_datetime, str):
            if starting_datetime == "now":
                starting_datetime = datetime.now()
            else:
                starting_datetime = datetime.fromisoformat(starting_datetime)

        self.t0 = TemporalDistribution(
            np.array([np.datetime64(starting_datetime)]),
            np.array([1]),
        )

        self.tech_matrix_csc = self.lca_object.technosphere_matrix.tocsc()
        self._ad_cache = {}
```

- [ ] **Step 5: Update `TimelineBuilder.__init__` in `bw_timex/timeline_builder.py`**

Add a `demand_tds` parameter and forward it to both extractor variants.

Change the signature (around line 30-45) by inserting a new kwarg between
`graph_traversal` and `*args`:

```python
        graph_traversal: str = "priority",
        demand_tds: dict | None = None,
        *args,
        **kwargs,
```

Then add at the top of the body (before the bfs/priority branch):

```python
        self.demand_tds = demand_tds or {}
```

In the `graph_traversal == "bfs"` branch (around line 101-108), add to the
`EdgeExtractorBFS` call:

```python
            self.edge_extractor = EdgeExtractorBFS(
                lca_object=base_lca,
                starting_datetime=self.starting_datetime,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                static_activity_indices=set(static_background_activity_ids),
                demand_tds=self.demand_tds,
            )
```

In the `graph_traversal == "priority"` branch (around line 109-119), add to
the `EdgeExtractor` call:

```python
            self.edge_extractor = EdgeExtractor(
                base_lca,
                starting_datetime=self.starting_datetime,
                *args,
                edge_filter_function=edge_filter_function,
                cutoff=self.cutoff,
                max_calc=self.max_calc,
                static_activity_indices=set(static_background_activity_ids),
                demand_tds=self.demand_tds,
                **kwargs,
            )
```

- [ ] **Step 6: Resolve demand-dict keys to product ids and pass through `TimexLCA.build_timeline`**

In `bw_timex/timex_lca.py`, find the `TimelineBuilder(...)` instantiation
(starts around line 282 inside `build_timeline`). Build a `demand_tds`
dict resolved to product ids first:

```python
        demand_tds = {}
        for k, v in self.demand.items():
            if isinstance(v, TemporalDistribution):
                product_id = bd.get_id(k)
                demand_tds[product_id] = v
```

Place this *before* the `self.timeline_builder = TimelineBuilder(...)` call.
Then pass it into the constructor by adding the kwarg:

```python
        self.timeline_builder = TimelineBuilder(
            self.base_lca,
            self.starting_datetime,
            self.edge_filter_function,
            self.database_dates,
            self.database_dates_static,
            self.activity_time_mapping,
            self.node_collections,
            self.nodes,
            self.temporal_grouping,
            self.interpolation_type,
            self.cutoff,
            self.max_calc,
            graph_traversal=graph_traversal,
            demand_tds=demand_tds,
            *args,
            **kwargs,
        )
```

- [ ] **Step 7: Run the plumbing test, confirm it passes**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_tds_propagate_to_edge_extractor -x 2>&1 | tail -5
```

Expected: PASS. Note: timeline rows may or may not yet reflect the TD —
that's Task 4. Test only inspects `demand_tds` propagation.

- [ ] **Step 8: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add bw_timex/timex_lca.py bw_timex/timeline_builder.py bw_timex/edge_extractor.py tests/test_demand_temporal_distribution.py
git commit -m "feat: plumb demand_tds through TimelineBuilder to EdgeExtractor

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4: FU seeding substitution in `EdgeExtractor` (priority path)

Replace `self.t0` with the demand TD when seeding FU edges for products
that have a demand TD. The existing `seed * td_producer` machinery then
handles convolution with output-side production RTDs automatically.

**Files:**
- Modify: `bw_timex/edge_extractor.py:106-180` (`build_edge_timeline`).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 4.1: Write the failing test for absolute TD demand timeline shape

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_absolute_td_demand_produces_one_fu_row_per_entry():
    node, dbdates = _make_chimaera_project("__test_td_demand_abs_fu_rows__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    tlca.build_timeline(
        starting_datetime=datetime(2025, 1, 1),
        temporal_grouping="year",
    )
    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1].sort_values(
        "date_producer"
    )
    assert fu_rows["date_producer"].dt.year.tolist() == [2025, 2030]
    assert fu_rows["amount"].tolist() == pytest.approx([60.0, 40.0])

    tlca.lci()
    tlca.static_lcia()
    # 100 units × 2 kg/unit × 0.5 kg CO2/kg = 100.
    assert tlca.static_score == pytest.approx(100.0)
```

- [ ] **Step 2: Run, confirm it fails on timeline shape**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_absolute_td_demand_produces_one_fu_row_per_entry -x 2>&1 | tail -10
```

Expected: fails — FU rows have only one entry (the old `t0` seed) with
amount 100.

### Task 4.2: Substitute the seed

- [ ] **Step 3: Add a helper to pick the seed TD for an FU edge**

Add this method to `EdgeExtractor` (place near `build_edge_timeline`):

```python
    def _seed_td_for_fu_edge(self, product_id: int, edge_amount: float) -> "TemporalDistribution":
        """Return the TemporalDistribution used in place of ``self.t0`` to seed
        the FU edge for ``product_id``.

        When ``product_id`` is registered in ``self.demand_tds``, return a TD
        whose dates are the demand TD's dates and whose amounts are normalised
        so that ``seed * edge_amount`` reproduces the demand TD's absolute
        amounts. Otherwise, fall back to ``self.t0`` (single-date TD with
        amount 1).
        """
        td = self.demand_tds.get(product_id)
        if td is None:
            return self.t0
        amounts = np.asarray(td.amount, dtype=float)
        total = amounts.sum()
        if total == 0:
            return self.t0
        # The seeding code multiplies by edge.amount, which (from the base LCA
        # using scalar = total) equals total. Normalising by total here keeps
        # seed * edge.amount == td.amount.
        return TemporalDistribution(
            date=np.asarray(td.date),
            amount=amounts / total,
        )
```

- [ ] **Step 4: Use the helper in `build_edge_timeline` (priority path)**

In `bw_timex/edge_extractor.py`, replace the body of the FU-edge loop (lines
106-180). Locate:

```python
        for edge in self.edge_mapping[
            self.unique_id
        ]:  # starting at the edges of the functional unit
            node = self.nodes[edge.producer_unique_id]
            td_producer = edge.amount
            initial_distribution = self.t0 * edge.amount
            abs_td_producer = self.t0
            abs_td_consumer = None

            row_id = self.lca_object.dicts.product.reversed[edge.product_index]
            col_id = node.activity_datapackage_id
            exchange = self.get_technosphere_exchange(input_id=row_id, output_id=col_id)
```

and the explicit-paradigm block below it, and rewrite as:

```python
        for edge in self.edge_mapping[
            self.unique_id
        ]:  # starting at the edges of the functional unit
            node = self.nodes[edge.producer_unique_id]
            row_id = self.lca_object.dicts.product.reversed[edge.product_index]
            col_id = node.activity_datapackage_id

            seed_td = self._seed_td_for_fu_edge(row_id, edge.amount)

            td_producer = edge.amount
            initial_distribution = seed_td * edge.amount
            abs_td_producer = seed_td
            abs_td_consumer = None

            exchange = self.get_technosphere_exchange(input_id=row_id, output_id=col_id)

            # In the explicit process/product paradigm, the demanded product is
            # produced by an off-diagonal production edge. A TD on that edge
            # distributes the process invocation itself before the process
            # inputs are traversed.
            if (
                row_id != col_id
                and hasattr(exchange, "data")
                and exchange.data.get("type") == "production"
            ):
                if row_id in self.demand_tds and self._has_output_td(exchange):
                    warnings.warn(
                        f"Demand TD and output-side production temporal_distribution "
                        f"are both present on product id {row_id}. "
                        f"Convolving them; resulting FU schedule = cross product.",
                        UserWarning,
                        stacklevel=2,
                    )
                production_amount = abs(
                    get_reference_product_production_amount(
                        col_id, reference_product=row_id, lca=self.lca_object
                    )
                )
                td_producer = (
                    self._exchange_value(
                        exchange=exchange,
                        row_id=row_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / production_amount
                    * edge.amount
                )
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )
                initial_distribution = (seed_td * td_producer).simplify()
                abs_td_producer = self.join_datetime_and_timedelta_distributions(
                    td_producer, seed_td
                )
                abs_td_consumer = seed_td
```

(Below this loop, the heappush / `timeline.append` lines remain unchanged
— they reference `abs_td_producer`, `initial_distribution`, etc., which
are now the substituted versions.)

- [ ] **Step 5: Add the small `_has_output_td` helper and `warnings` import**

At the top of the file, add `import warnings` next to `import numpy as np`.

Add this method to `EdgeExtractor`:

```python
    @staticmethod
    def _has_output_td(exchange) -> bool:
        """Return True if the production-edge exchange has a non-empty
        ``temporal_distribution`` attribute."""
        try:
            td = exchange.get("temporal_distribution")
        except Exception:
            try:
                td = exchange.data.get("temporal_distribution")
            except Exception:
                return False
        return td is not None and isinstance(td, TemporalDistribution) and len(td.amount) > 0
```

- [ ] **Step 6: Run the test, confirm it passes**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_absolute_td_demand_produces_one_fu_row_per_entry -x 2>&1 | tail -8
```

Expected: PASS. The static score is 100.0.

- [ ] **Step 7: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green (no regressions — scalar demands still take the
`self.t0` fallback).

- [ ] **Step 8: Commit**

```bash
git add bw_timex/edge_extractor.py tests/test_demand_temporal_distribution.py
git commit -m "feat: substitute demand TD as FU seed in EdgeExtractor (priority path)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 5: FU seeding substitution in `EdgeExtractorBFS`

Same substitution but in the BFS-traversal class.

**Files:**
- Modify: `bw_timex/edge_extractor.py:510-535` (BFS FU-seeding loop).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 5.1: Write the failing test for BFS

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_absolute_td_demand_bfs_path():
    """Same as test_absolute_td_demand_produces_one_fu_row_per_entry but via
    the BFS graph-traversal path."""
    node, dbdates = _make_chimaera_project("__test_td_demand_abs_bfs__")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    tlca.build_timeline(
        starting_datetime=datetime(2025, 1, 1),
        temporal_grouping="year",
        graph_traversal="bfs",
    )
    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1].sort_values(
        "date_producer"
    )
    assert fu_rows["date_producer"].dt.year.tolist() == [2025, 2030]
    assert fu_rows["amount"].tolist() == pytest.approx([60.0, 40.0])
    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(100.0)
```

- [ ] **Step 2: Run, confirm it fails**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_absolute_td_demand_bfs_path -x 2>&1 | tail -8
```

Expected: FU rows have only one entry; assertion fails.

### Task 5.2: Substitute the seed in BFS path

- [ ] **Step 3: Update `EdgeExtractorBFS.build_edge_timeline`**

Find the FU-seeding loop in `bw_timex/edge_extractor.py` (around line 510):

```python
        for fu_id in fu_activity_ids:
            fu_amount = demand_array[self.lca_object.dicts.activity[fu_id]]
            td = self.t0 * fu_amount

            timeline.append(
                Edge(
                    edge_type="production",
                    distribution=td,
                    leaf=False,
                    consumer=-1,
                    producer=fu_id,
                    td_producer=fu_amount,
                    td_consumer=self.t0,
                    abs_td_producer=self.t0,
                )
            )

            queue.append((fu_id, td, self.t0, self.t0, abs(fu_amount)))
```

Replace with:

```python
        for fu_id in fu_activity_ids:
            fu_amount = demand_array[self.lca_object.dicts.activity[fu_id]]
            seed_td = self._seed_td_for_fu_edge(fu_id, float(fu_amount))
            td = seed_td * fu_amount

            timeline.append(
                Edge(
                    edge_type="production",
                    distribution=td,
                    leaf=False,
                    consumer=-1,
                    producer=fu_id,
                    td_producer=fu_amount,
                    td_consumer=seed_td,
                    abs_td_producer=seed_td,
                )
            )

            queue.append((fu_id, td, seed_td, seed_td, abs(fu_amount)))
```

- [ ] **Step 4: Add the `_seed_td_for_fu_edge` helper to `EdgeExtractorBFS`**

Either lift the helper from `EdgeExtractor` into a shared module-level
function, or duplicate it on `EdgeExtractorBFS`. **Use a module-level
helper** to keep DRY. At module level (above the `EdgeExtractor` class):

```python
def _seed_td_from_demand(
    demand_tds: dict,
    product_id: int,
    edge_amount: float,
    fallback_t0: "TemporalDistribution",
) -> "TemporalDistribution":
    """Return the seed TD to use for an FU edge. See class-method docstrings.

    Normalises a registered demand TD so that ``seed * edge_amount``
    reproduces the original demand TD amounts.
    """
    td = demand_tds.get(product_id)
    if td is None:
        return fallback_t0
    amounts = np.asarray(td.amount, dtype=float)
    total = amounts.sum()
    if total == 0:
        return fallback_t0
    return TemporalDistribution(
        date=np.asarray(td.date),
        amount=amounts / total,
    )
```

Then change `EdgeExtractor._seed_td_for_fu_edge` and add
`EdgeExtractorBFS._seed_td_for_fu_edge` to call the helper:

```python
    def _seed_td_for_fu_edge(self, product_id: int, edge_amount: float) -> "TemporalDistribution":
        return _seed_td_from_demand(self.demand_tds, product_id, edge_amount, self.t0)
```

Add the same method to `EdgeExtractorBFS`.

Note for BFS: `fu_id` is an activity id, not a product id. For chimaera
nodes activity_id == product_id, so it works directly. For explicit
paradigm, the demand is on the product but the BFS extractor's
`fu_activity_ids` are activity ids resolved by bw2calc from the demand
dict. **The TD demand is keyed by product id in `self.demand_tds`,
populated by `TimexLCA.build_timeline` via `bd.get_id(k)` on the demand
key**, which is the product id. We therefore look up
`self.demand_tds.get(fu_id)` — and for the BFS path, when the user
demands a product node, `fu_id` from `demand_array.nonzero()` is the
activity (= process) id, not the product id. Detect this mismatch by
**also keying `demand_tds` by the resolved activity id** at plumbing
time, so the lookup works for both extractors regardless of paradigm.

Update `TimexLCA.build_timeline`'s plumbing block (modified in Task 3)
to register both ids:

```python
        demand_tds = {}
        for k, v in self.demand.items():
            if isinstance(v, TemporalDistribution):
                product_id = bd.get_id(k)
                demand_tds[product_id] = v
                # Also key by the producing-process id so the BFS extractor's
                # activity-id-based lookup finds the TD.
                process_id = self._resolve_demand_to_process_id(k)
                demand_tds[process_id] = v
```

- [ ] **Step 5: Run the BFS test, confirm it passes**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_absolute_td_demand_bfs_path -x 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 6: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add bw_timex/edge_extractor.py bw_timex/timex_lca.py tests/test_demand_temporal_distribution.py
git commit -m "feat: substitute demand TD as FU seed in EdgeExtractorBFS

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 6: Relative-date demand TD (timedelta64) — requires `starting_datetime`

A demand TD with `timedelta64[*]` dates means "offsets from
`starting_datetime`." Convert it to absolute before storing into
`demand_tds`. Raise if `starting_datetime` is missing or string `"now"`.

**Files:**
- Modify: `bw_timex/timex_lca.py` (in `build_timeline`).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 6.1: Write the failing tests

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_relative_td_demand_requires_starting_datetime():
    node, dbdates = _make_chimaera_project("__test_td_demand_rel_requires_sdt__")
    td = TemporalDistribution(
        date=np.array([0, 5], dtype="timedelta64[Y]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    with pytest.raises(ValueError, match="starting_datetime"):
        tlca.build_timeline(starting_datetime="now")


def test_relative_td_demand_matches_absolute_equivalent():
    """A relative-date demand TD anchored at 2025-01-01 should produce the
    same timeline as the absolute-date equivalent."""
    node, dbdates = _make_chimaera_project("__test_td_demand_rel_eq_abs__")
    td_rel = TemporalDistribution(
        date=np.array([0, 5], dtype="timedelta64[Y]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={node.key: td_rel},
        method=("GWP", "example"),
        database_dates=dbdates,
    )
    tlca.build_timeline(
        starting_datetime=datetime(2025, 1, 1),
        temporal_grouping="year",
    )
    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1].sort_values(
        "date_producer"
    )
    assert fu_rows["date_producer"].dt.year.tolist() == [2025, 2030]
    assert fu_rows["amount"].tolist() == pytest.approx([60.0, 40.0])
```

- [ ] **Step 2: Run both tests, confirm they fail**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py -k "relative_td_demand" -x 2>&1 | tail -10
```

Expected: both fail — `starting_datetime="now"` uses `datetime.now()` and no
validation rejects it; the equivalence test errors deep in `bw_temporalis`
when timedelta64 dates are seeded as `t0`-replacement.

### Task 6.2: Convert relative TDs to absolute in `build_timeline`

- [ ] **Step 3: Add a helper on `TimexLCA`**

Place near `_compute_scalar_demand`:

```python
    @staticmethod
    def _to_absolute_td(td: TemporalDistribution, starting_datetime: datetime) -> TemporalDistribution:
        """Convert a TD whose dates are relative (``timedelta64[*]``) to one
        whose dates are absolute (``datetime64[s]``), anchored at
        ``starting_datetime``. Absolute TDs are returned unchanged.
        """
        date = np.asarray(td.date)
        if date.dtype.kind == "M":
            return td
        if date.dtype.kind != "m":
            raise ValueError(
                f"demand TD date dtype must be datetime64 or timedelta64, "
                f"got {date.dtype}."
            )
        anchor = np.datetime64(starting_datetime).astype("datetime64[s]")
        absolute_dates = (anchor + date).astype("datetime64[s]")
        return TemporalDistribution(date=absolute_dates, amount=np.asarray(td.amount, dtype=float))
```

- [ ] **Step 4: Validate and convert in `build_timeline`**

Inside `TimexLCA.build_timeline` (around line 270 — just after
`starting_datetime` is resolved by `BuildTimelineInputs`), find the new
`demand_tds = {}` block added in Task 3/5 and replace it with:

```python
        relative_demand_keys = [
            k
            for k, v in self.demand.items()
            if isinstance(v, TemporalDistribution)
            and np.asarray(v.date).dtype.kind == "m"
        ]
        if relative_demand_keys and not isinstance(
            self.starting_datetime, datetime
        ):
            raise ValueError(
                f"demand TDs with relative (timedelta64) dates require an "
                f"explicit starting_datetime in build_timeline. "
                f"Affected demand keys: {relative_demand_keys}."
            )

        demand_tds = {}
        for k, v in self.demand.items():
            if isinstance(v, TemporalDistribution):
                v_abs = self._to_absolute_td(v, self.starting_datetime)
                product_id = bd.get_id(k)
                process_id = self._resolve_demand_to_process_id(k)
                demand_tds[product_id] = v_abs
                demand_tds[process_id] = v_abs
```

**Important:** The `BuildTimelineInputs` validator converts the
default `"now"` to `datetime.now()` before this code runs, so the
`isinstance(self.starting_datetime, datetime)` check needs to compare
to a *user-supplied* datetime. Add a sentinel:

Look at the existing `build_timeline` method body in
`bw_timex/timex_lca.py` (the validation block around lines 234-260
that constructs `BuildTimelineInputs(...)`). Note the parameter
`starting_datetime` and its default `"now"`. Right before passing
through the validator, record whether the user supplied an explicit
datetime:

```python
        user_supplied_starting_datetime = isinstance(starting_datetime, datetime)
```

Then in the relative-TD check above, use that flag instead of
`isinstance(self.starting_datetime, datetime)`:

```python
        if relative_demand_keys and not user_supplied_starting_datetime:
            raise ValueError(
                f"demand TDs with relative (timedelta64) dates require an "
                f"explicit starting_datetime in build_timeline. "
                f"Affected demand keys: {relative_demand_keys}."
            )
```

- [ ] **Step 5: Run the two relative-TD tests, confirm they pass**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py -k "relative_td_demand" -x 2>&1 | tail -5
```

Expected: 2 passed.

- [ ] **Step 6: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_demand_temporal_distribution.py
git commit -m "feat: support relative-date demand TDs anchored at starting_datetime

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 7: Convolution + warning when both demand TD and production RTD present

The seed substitution from Task 4 already convolves via `seed * td_producer`
on the explicit-paradigm branch. This task adds the chimaera-paradigm
case (production-self exchange with `temporal_distribution`) and writes
the cross-product test plus the warning test.

**Files:**
- Modify: `bw_timex/edge_extractor.py` (handle chimaera output-RTD).
- Modify: `tests/test_demand_temporal_distribution.py`.

### Task 7.1: Write the failing convolution test

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_demand_td_convolves_with_production_rtd():
    """Demand TD with two dates and output-side production RTD with two
    relative offsets produces cross-product FU rows; UserWarning is emitted.
    """
    bd.projects.set_current("__test_td_demand_convolution__")
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    prod_rtd = TemporalDistribution(
        date=np.array([0, 5], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    bd.Database("foreground").write(
        {
            ("foreground", "fg_product"): {
                "name": "fg product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
            ("foreground", "fg_process"): {
                "name": "fg process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "fg_product"),
                        "amount": 1,
                        "type": "production",
                        "temporal_distribution": prod_rtd,
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 2.0,
                        "type": "technosphere",
                    },
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    product = bd.get_node(database="foreground", code="fg_product")

    td_demand = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )

    with pytest.warns(UserWarning, match="Demand TD and output-side"):
        tlca = TimexLCA(
            demand={product.key: td_demand},
            method=("GWP", "example"),
            database_dates={
                "background": datetime(2025, 1, 1),
                "foreground": "dynamic",
            },
        )
        tlca.build_timeline(
            starting_datetime=datetime(2025, 1, 1),
            temporal_grouping="year",
        )

    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1].sort_values(
        "date_producer"
    )
    # Cross product: (2025, 60×0.6=36), (2030, 60×0.4+40×0.6=48), (2035, 40×0.4=16).
    assert fu_rows["date_producer"].dt.year.tolist() == [2025, 2030, 2035]
    assert fu_rows["amount"].tolist() == pytest.approx([36.0, 48.0, 16.0])

    tlca.lci()
    tlca.static_lcia()
    # Static base = 100 × 2 × 0.5 = 100. With single background, no relinking,
    # static_score should also equal 100.
    assert tlca.base_lca.score == pytest.approx(100.0)
    assert tlca.static_score == pytest.approx(100.0)
```

- [ ] **Step 2: Run, confirm it fails on either timeline shape or warning**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_td_convolves_with_production_rtd -x 2>&1 | tail -10
```

Expected: the explicit-paradigm convolution branch from Task 4 already
fires; the warning branch from Task 4 only triggered on
`exchange.data.get("type") == "production"` and may be working — but the
assertions on date+amount lists must verify the cross-product math. If
that already passes, this task is reduced to a verification step.
Document either outcome.

### Task 7.2: If the test failed, the warning isn't firing — fix it

The Task 4 implementation already emits the warning inside the explicit
branch. Confirm the warning fires for this test (which uses explicit
paradigm). If it doesn't:

- [ ] **Step 3: Trace whether `_has_output_td` returns True**

Add a one-line debug print in `EdgeExtractor.build_edge_timeline` just
above the `warnings.warn(...)` call:

```python
                print(f"DEBUG: row_id {row_id} in demand_tds? {row_id in self.demand_tds}; has output td? {self._has_output_td(exchange)}")
```

Run the test; if both flags True, the warning should fire. Remove the
debug line.

If the warning does not fire because `_has_output_td` misreads the
exchange (e.g. older bw2data versions store td on `.data` vs `.input`),
update the helper:

```python
    @staticmethod
    def _has_output_td(exchange) -> bool:
        for accessor in (
            lambda e: e.get("temporal_distribution"),
            lambda e: e.data.get("temporal_distribution") if hasattr(e, "data") else None,
            lambda e: e._data.get("temporal_distribution") if hasattr(e, "_data") else None,
        ):
            try:
                td = accessor(exchange)
            except Exception:
                td = None
            if td is not None and isinstance(td, TemporalDistribution) and len(td.amount) > 0:
                return True
        return False
```

- [ ] **Step 4: Re-run the test, confirm it passes**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_td_convolves_with_production_rtd -x 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/edge_extractor.py tests/test_demand_temporal_distribution.py
git commit -m "test: cover demand-TD × output-side-production-RTD convolution

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 8: Mixed scalar + TD demand entries + multi-vintage relinking

Round out coverage: a demand dict with both forms, and a demand TD that
exercises the multi-vintage relinking the prior fix established.

**Files:**
- Modify: `tests/test_demand_temporal_distribution.py`.
- No source changes expected — but if either test fails, return to
  Phase 1 of `systematic-debugging` and find the gap.

### Task 8.1: Mixed-form demand dict

- [ ] **Step 1: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_mixed_scalar_and_td_demand_entries():
    """Two demand entries: one scalar, one TD. Both contribute correctly to
    the FU rows and to the static base score."""
    bd.projects.set_current("__test_td_demand_mixed__")
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])
    bd.Database("foreground").write(
        {
            ("foreground", "p1"): {
                "name": "p1",
                "reference product": "p1",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {"input": ("foreground", "p1"), "amount": 1, "type": "production"},
                    {"input": ("background", "input"), "amount": 2.0, "type": "technosphere"},
                ],
            },
            ("foreground", "p2"): {
                "name": "p2",
                "reference product": "p2",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {"input": ("foreground", "p2"), "amount": 1, "type": "production"},
                    {"input": ("background", "input"), "amount": 3.0, "type": "technosphere"},
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    p1 = bd.get_node(database="foreground", code="p1")
    p2 = bd.get_node(database="foreground", code="p2")

    td_p2 = TemporalDistribution(
        date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        amount=np.array([60.0, 40.0]),
    )
    tlca = TimexLCA(
        demand={p1.key: 5.0, p2.key: td_p2},
        method=("GWP", "example"),
        database_dates={
            "background": datetime(2025, 1, 1),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(starting_datetime=datetime(2025, 1, 1), temporal_grouping="year")
    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1]
    assert sorted(
        (row.producer_name, row.date_producer.year, float(row.amount))
        for row in fu_rows.itertuples()
    ) == [
        ("p1", 2025, 5.0),
        ("p2", 2025, 60.0),
        ("p2", 2030, 40.0),
    ]

    # Static base score = 5 × 2 × 0.5 + 100 × 3 × 0.5 = 5 + 150 = 155.
    slca = bc.LCA({p1.key: 5.0, p2.key: 100.0}, method=("GWP", "example"))
    slca.lci()
    slca.lcia()
    assert tlca.base_lca.score == pytest.approx(slca.score)
```

- [ ] **Step 2: Run, observe**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_mixed_scalar_and_td_demand_entries -x 2>&1 | tail -10
```

Expected: PASS if Tasks 1-6 are wired correctly. If FAIL, inspect the
timeline and identify which assumption broke.

### Task 8.2: Multi-vintage relinking with demand TD

- [ ] **Step 3: Append to `tests/test_demand_temporal_distribution.py`**

```python
def test_demand_td_multi_vintage_relinking():
    """Demand TD with two dates falling in two different background
    databases. Score = demand-weighted mix of the two backgrounds.

    Extends the chimaera regression test for the multi-vintage fix:
    here the cohorts are induced purely by the demand TD, not by a
    production-edge RTD.
    """
    bd.projects.set_current("__test_td_demand_multi_vintage__")
    bd.databases.clear()
    bd.methods.clear()
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "co2", "unit": "kg", "type": "emission"}}
    )
    for db_name, co2 in [
        ("electricity_market_2025", 0.40),
        ("electricity_market_2035", 0.10),
    ]:
        bd.Database(db_name).write(
            {
                (db_name, "elec"): {
                    "name": "electricity market",
                    "reference product": "electricity",
                    "location": "DE",
                    "unit": "kWh",
                    "exchanges": [
                        {"input": (db_name, "elec"), "amount": 1, "type": "production"},
                        {"input": ("bio", "co2"), "amount": co2, "type": "biosphere"},
                    ],
                }
            }
        )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])
    bd.Database("foreground").write(
        {
            ("foreground", "fg_process"): {
                "name": "fg process",
                "reference product": "fg product",
                "unit": "unit",
                "location": "DE",
                "exchanges": [
                    {"input": ("foreground", "fg_process"), "amount": 1, "type": "production"},
                    {
                        "input": ("electricity_market_2025", "elec"),
                        "amount": 1.0,
                        "type": "technosphere",
                    },
                ],
            }
        }
    )
    for db in bd.databases:
        bd.Database(db).process()
    node = bd.get_node(database="foreground", code="fg_process")
    td = TemporalDistribution(
        date=np.array(["2025-01-01", "2035-01-01"], dtype="datetime64[s]"),
        amount=np.array([0.5, 0.5]),
    )
    tlca = TimexLCA(
        demand={node.key: td},
        method=("GWP", "example"),
        database_dates={
            "electricity_market_2025": datetime(2025, 1, 1),
            "electricity_market_2035": datetime(2035, 1, 1),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(
        starting_datetime=datetime(2025, 1, 1),
        temporal_grouping="year",
    )
    tlca.lci()
    tlca.static_lcia()
    # 0.5 × 0.40 + 0.5 × 0.10 = 0.25.
    assert tlca.static_score == pytest.approx(0.25)
```

- [ ] **Step 4: Run, observe**

```bash
.venv/bin/python -m pytest tests/test_demand_temporal_distribution.py::test_demand_td_multi_vintage_relinking -x 2>&1 | tail -8
```

Expected: PASS (uses the demand-distribution fix from commit `aa80a7d`
plus the FU-seed substitution from this plan).

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add tests/test_demand_temporal_distribution.py
git commit -m "test: cover mixed-form demand and multi-vintage relinking via demand TD

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 9: Documentation in docstrings

Polish the public-facing docstrings so users see the new option from
`help(TimexLCA)`. **No new code paths.** This task is purely
documentation.

**Files:**
- Modify: `bw_timex/timex_lca.py` (`TimexLCA` class docstring + `__init__` docstring + `build_timeline` docstring).

- [ ] **Step 1: Update `TimexLCA` class docstring**

Find the `TimexLCA` class definition (around line 60). Locate the
`Parameters` / `Examples` section. Add an entry for the `demand`
parameter that describes TD support:

```
        demand : dict
            Mapping from a Brightway ``Node``, ``(database, code)`` tuple, or
            integer id to either a numeric quantity OR a
            ``bw_temporalis.TemporalDistribution`` whose ``date`` is
            ``datetime64[*]`` (absolute calendar dates) or
            ``timedelta64[*]`` (offsets from ``starting_datetime``). When a
            TD value is given, each ``(date, amount)`` entry seeds an FU
            cohort. If the demanded product's production edge also has an
            output-side ``temporal_distribution``, the demand TD and the
            production RTD are convolved and a ``UserWarning`` is emitted.
```

Add a brief example to the existing example block:

```
        >>> from bw_temporalis import TemporalDistribution
        >>> td = TemporalDistribution(
        ...     date=np.array(["2025-01-01", "2030-01-01"], dtype="datetime64[s]"),
        ...     amount=np.array([60.0, 40.0]),
        ... )
        >>> tlca = TimexLCA(demand={product: td}, method=method, database_dates=db_dates)
```

- [ ] **Step 2: Update `build_timeline` docstring**

Find `TimexLCA.build_timeline` (around line 175). Append to its
docstring:

```
        Notes
        -----
        When any demand entry is a ``TemporalDistribution`` with relative
        (``timedelta64[*]``) dates, ``starting_datetime`` must be an
        explicit ``datetime`` — the default ``"now"`` is rejected.
```

- [ ] **Step 3: Run full suite (sanity, in case docstrings broke imports)**

```bash
.venv/bin/python -m pytest tests/ -q 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add bw_timex/timex_lca.py
git commit -m "docs: document TemporalDistribution support for demand in TimexLCA

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|---|---|
| Scalar + TD demand values accepted | Tasks 1 (validator), 2 (scalar projection) |
| TD must have datetime64 or timedelta64 dates | Task 1 |
| Negative amounts rejected | Task 1 |
| Empty TD rejected | Task 1 |
| `tlca.base_lca.score` = scalar-sum equivalent | Task 2 |
| Absolute-TD demand → FU rows match TD entries | Task 4 |
| BFS path matches | Task 5 |
| Relative-TD demand needs `starting_datetime` | Task 6 |
| Relative-TD demand matches absolute equivalent | Task 6 |
| Demand TD × production RTD → convolution + warning | Tasks 4 (warn) + 7 (math) |
| Mixed scalar + TD entries | Task 8 |
| Multi-vintage relinking with demand TD | Task 8 |
| Docs reflect new option | Task 9 |

### Placeholder scan

No "TBD", no "implement later", no bare "add tests". One genuinely
diagnostic step in Task 7.2 (print-and-remove) — that's a debugging
escape hatch only used if Task 7's test fails, and the debug line is
removed before commit.

### Type / name consistency

- `demand_tds` (dict) used consistently across `TimexLCA.build_timeline`,
  `TimelineBuilder.__init__`, `EdgeExtractor.__init__`,
  `EdgeExtractorBFS.__init__`.
- `_seed_td_for_fu_edge` exists on both extractor classes; both call
  the module-level `_seed_td_from_demand` helper.
- `_compute_scalar_demand`, `_to_absolute_td` are static methods on
  `TimexLCA`.
- `_has_output_td` is a static method on `EdgeExtractor`. Not needed
  on `EdgeExtractorBFS` because the BFS path does not currently route
  through the explicit-paradigm production-edge branch.
- Demand TD keys: `bd.get_id(k)` (product id) AND
  `_resolve_demand_to_process_id(k)` (process id) both registered to
  cover both extractor variants' lookup conventions.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-11-td-as-demand.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

Which?
