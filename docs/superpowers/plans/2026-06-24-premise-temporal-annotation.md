# premise Temporal-Distribution Annotation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Annotate existing premise-generated, year-specific bw2 databases with `bw_temporalis.TemporalDistribution`s, sourcing parameters and placement rules from premise's `temporal_distributions.csv`, so bw_timex can run time-explicit LCA on a premise background with no hand-defined temporal data.

**Architecture:** A single new module `bw_timex/premise_temporal.py`. A pure converter turns premise's distribution codes into `TemporalDistribution`s; an annotation function applies premise's placement rules to a bw2 database's exchanges using an injected `TemporalSpecs` (so the core is premise-free and fully testable); a thin adapter reuses premise's own CSV loader to build `TemporalSpecs`; a public entry point ties it together. `premise` is an optional extra, imported lazily and feature-detected.

**Tech Stack:** Python 3.13, numpy, `bw_temporalis` (core dep), `bw2data`, `premise` (optional extra), pytest, uv.

## Global Constraints

- Package manager: `uv` for all Python (`uv run pytest ...`). Never pip/conda.
- COMMITS ENABLED on branch `feat/premise-temporal` (off `main`). Commit per task. End every commit message body with exactly these two trailers:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01NadULkEstbs67wxr2W8DtL`
- `premise` is an OPTIONAL dependency (extra `premise`); core bw_timex must import and all existing tests must pass without premise installed. Import premise lazily inside functions only.
- Detect premise's temporal support by **feature detection** (presence of `premise.trails.TrailsDataPackage` and `premise.trails.FILEPATH_TEMPORAL_PARAMETERS`), NOT a version-number check (the loader currently ships on a premise branch numbered 2.3.7; the released version will be ≥2.5.0). Error messages may reference "premise>=2.5.0 / bw-timex[premise]".
- premise temporal distribution codes (years as the time unit): `1` discrete (mass at `loc`), `3` normal, `4` uniform (`[min,max]`), `5` triangular (mode=`loc`), `6` discrete empirical (explicit `offsets`/`weights`).
- premise placement rules to mirror EXACTLY (from `premise/trails.py::add_temporal_distributions`):
  - biomass_growth: dataset `(name, reference product)` in `biomass_growth_params` → its biosphere exchange named exactly `"Carbon dioxide, in air"`.
  - stock_asset: technosphere exchange whose SUPPLIER `(name, product)` is in `stock_asset_params` → supplier params.
  - maintenance: technosphere exchange whose supplier is in `maintenance_suppliers` → uniform (code 4) over `[0, lifetime]` using the CALLING dataset's lifetime.
  - end_of_life: technosphere exchange whose supplier is in `end_of_life_suppliers` → single pulse (code 6) at the calling dataset's lifetime.
  - supplier matching >1 of stock_asset/maintenance/end_of_life → fault, skip.
  - technosphere exchange with no supplier product, or maintenance/end_of_life with no dataset lifetime → fault, skip.
- premise's CSV loader returns the 5-tuple `(stock_assets, end_of_life, biomass_growth, maintenance, dataset_lifetimes)` and uses no `self`.
- Do not edit anything under `.venv/`. No unfold, no materialization, no database_dates building.

---

## File Structure

- **Create `bw_timex/premise_temporal.py`** — the entire feature (one responsibility: premise→bw_timex temporal annotation): dataclasses `TemporalSpecs`, `AnnotationReport`; pure converter `premise_params_to_td`; `annotate_database`; premise adapter `load_temporal_specs`; public `add_premise_temporal_distributions`.
- **Modify `bw_timex/__init__.py`** — export `add_premise_temporal_distributions`.
- **Modify `pyproject.toml`** — add `premise` optional-dependency extra.
- **Create `tests/test_premise_temporal.py`** — unit tests for the converter and annotation (premise-free, bw2 fixtures), plus premise-gated tests for the loader (`pytest.importorskip`).

Reference facts (verified):
- premise CSV loader: `premise.trails.TrailsDataPackage._load_temporal_specs_from_csv(self, path)` — ignores `self`; returns `(stock_assets, end_of_life, biomass_growth, maintenance, dataset_lifetimes)`. Param dicts have keys: `temporal_distribution` (int code), `temporal_loc`, `temporal_scale`, `temporal_offsets` (list|None), `temporal_weights` (list|None), `temporal_min`, `temporal_max`, `lifetime`.
- CSV path constant: `premise.trails.FILEPATH_TEMPORAL_PARAMETERS`.
- `bw_temporalis.easy_timedelta_distribution(start: int, end: int, resolution: str, steps: int|None=50, kind: str|None="uniform", param: float|None=None)`; kinds include `"uniform"`, `"triangular"` (param=mode), `"normal"` (param=std). `bw_temporalis.TemporalDistribution(date, amount)` with `date` a `timedelta64` ndarray and `amount` a float ndarray.
- bw_timex stores TDs as `exchange["temporal_distribution"] = <TemporalDistribution>; exchange.save()`.

---

### Task 1: Module scaffold — dataclasses + pure converter `premise_params_to_td`

**Files:**
- Create: `bw_timex/premise_temporal.py`
- Test: `tests/test_premise_temporal.py`

**Interfaces:**
- Consumes: numpy, `bw_temporalis` (`TemporalDistribution`, `easy_timedelta_distribution`).
- Produces:
  - `@dataclass TemporalSpecs` with fields `biomass_growth_params: dict`, `stock_asset_params: dict`, `maintenance_suppliers: set`, `end_of_life_suppliers: set`, `dataset_lifetimes: dict`.
  - `@dataclass AnnotationReport` with `annotated: int = 0`, `skipped_existing: int = 0`, `faults: list = field(default_factory=list)`, and a `merge(self, other: "AnnotationReport") -> None` method.
  - `premise_params_to_td(params: dict, *, max_steps: int = 200) -> bw_temporalis.TemporalDistribution`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_premise_temporal.py
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution


def test_discrete_code_1_single_pulse_at_loc():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td({"temporal_distribution": 1, "temporal_loc": -5.0})
    assert isinstance(td, TemporalDistribution)
    assert td.date.astype("timedelta64[Y]").astype(int).tolist() == [-5]
    assert np.allclose(td.amount.sum(), 1.0)


def test_empirical_code_6_offsets_weights_normalised():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 6, "temporal_offsets": [0, 10], "temporal_weights": [1.0, 3.0]}
    )
    assert td.date.astype("timedelta64[Y]").astype(int).tolist() == [0, 10]
    np.testing.assert_allclose(td.amount, [0.25, 0.75])


def test_uniform_code_4_from_min_max():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td({"temporal_distribution": 4, "temporal_min": 0.0, "temporal_max": 5.0})
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == 0 and yrs.max() == 5
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_normal_code_3_bounds_from_min_max():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 3, "temporal_loc": -20.0, "temporal_scale": 3.0,
         "temporal_min": -40.0, "temporal_max": -1.0}
    )
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == -40 and yrs.max() == -1
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_triangular_code_5():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 5, "temporal_loc": 5.0, "temporal_min": 0.0, "temporal_max": 10.0}
    )
    assert td.date.astype("timedelta64[Y]").astype(int).max() == 10
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_unsupported_code_raises():
    from bw_timex.premise_temporal import premise_params_to_td
    with pytest.raises(ValueError):
        premise_params_to_td({"temporal_distribution": 99, "temporal_loc": 1.0})


def test_annotation_report_merge():
    from bw_timex.premise_temporal import AnnotationReport
    a = AnnotationReport(annotated=1, skipped_existing=2, faults=[{"x": 1}])
    b = AnnotationReport(annotated=3, skipped_existing=0, faults=[{"y": 2}])
    a.merge(b)
    assert a.annotated == 4 and a.skipped_existing == 2 and len(a.faults) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_premise_temporal.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'bw_timex.premise_temporal'`.

- [ ] **Step 3: Write the implementation**

```python
# bw_timex/premise_temporal.py
"""Annotate existing premise databases with bw_timex temporal distributions.

premise (the trails work, released in premise >= 2.5.0) curates background
temporal data in ``temporal_distributions.csv`` and places it on exchanges via
fixed rules. This module reuses premise's CSV loader and mirrors those
placement rules to write ``bw_temporalis.TemporalDistribution`` objects onto the
exchanges of already-existing, year-specific premise bw2 databases. It does not
build, unfold, or materialize databases.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from bw_temporalis import TemporalDistribution, easy_timedelta_distribution

_RESOLUTION = "Y"  # premise temporal values are in years


@dataclass
class TemporalSpecs:
    """premise's categorized temporal buckets (keys are ``(name, reference product)``)."""

    biomass_growth_params: dict
    stock_asset_params: dict
    maintenance_suppliers: set
    end_of_life_suppliers: set
    dataset_lifetimes: dict


@dataclass
class AnnotationReport:
    """Summary of an annotation pass."""

    annotated: int = 0
    skipped_existing: int = 0
    faults: list = field(default_factory=list)

    def merge(self, other: "AnnotationReport") -> None:
        self.annotated += other.annotated
        self.skipped_existing += other.skipped_existing
        self.faults.extend(other.faults)


def _single_pulse(year: float) -> TemporalDistribution:
    return TemporalDistribution(
        date=np.array([int(round(year))], dtype="timedelta64[Y]"),
        amount=np.array([1.0], dtype=float),
    )


def _bounds(params: dict, loc, scale) -> tuple[int, int]:
    mn = params.get("temporal_min")
    mx = params.get("temporal_max")
    if mn is not None and mx is not None:
        start, end = int(np.floor(mn)), int(np.ceil(mx))
    elif loc is not None and scale:
        start, end = int(np.floor(loc - 3 * scale)), int(np.ceil(loc + 3 * scale))
    else:
        raise ValueError(
            "Cannot determine distribution bounds: need temporal_min/temporal_max "
            "or temporal_loc + temporal_scale."
        )
    if start > end:
        start, end = end, start
    return start, end


def premise_params_to_td(params: dict, *, max_steps: int = 200) -> TemporalDistribution:
    """Convert one premise temporal-parameter dict into a ``TemporalDistribution``.

    ``params`` uses premise keys: ``temporal_distribution`` (int code),
    ``temporal_loc``, ``temporal_scale``, ``temporal_min``, ``temporal_max``,
    ``temporal_offsets``, ``temporal_weights``. Time unit is years.
    """
    code = params.get("temporal_distribution")
    loc = params.get("temporal_loc")
    scale = params.get("temporal_scale")

    if code == 1:  # discrete: all mass at loc
        if loc is None:
            raise ValueError("discrete (code 1) temporal distribution requires temporal_loc")
        return _single_pulse(loc)

    if code == 6:  # discrete empirical: explicit offsets/weights
        offsets = params.get("temporal_offsets")
        weights = params.get("temporal_weights")
        if not offsets or not weights or len(offsets) != len(weights):
            raise ValueError("empirical (code 6) requires matching temporal_offsets/temporal_weights")
        amount = np.asarray(weights, dtype=float)
        total = amount.sum()
        if total == 0:
            raise ValueError("empirical (code 6) weights sum to zero")
        amount = amount / total
        return TemporalDistribution(
            date=np.array([int(round(o)) for o in offsets], dtype="timedelta64[Y]"),
            amount=amount,
        )

    start, end = _bounds(params, loc, scale)
    steps = max(2, min(max_steps, end - start + 1))

    if code == 3:  # normal
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="normal", param=scale)
    if code == 4:  # uniform
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="uniform")
    if code == 5:  # triangular, mode = loc
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="triangular", param=loc)

    raise ValueError(f"Unsupported premise temporal_distribution code: {code!r}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_premise_temporal.py -v`
Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/premise_temporal.py tests/test_premise_temporal.py
git commit  # "feat: premise_temporal converter + dataclasses"
```

---

### Task 2: `annotate_database` — apply premise placement rules to a bw2 database

**Files:**
- Modify: `bw_timex/premise_temporal.py`
- Test: `tests/test_premise_temporal.py`

**Interfaces:**
- Consumes: `TemporalSpecs`, `AnnotationReport`, `premise_params_to_td` (Task 1); `bw2data` (lazy import).
- Produces: `annotate_database(db_name: str, specs: TemporalSpecs, *, overwrite: bool = False) -> AnnotationReport`. Premise-free (operates on an injected `TemporalSpecs`).

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_premise_temporal.py
from bw2data.tests import bw2test


def _write_synthetic_dbs():
    import bw2data as bd
    bd.Database("bio").write({
        ("bio", "co2"): {"name": "Carbon dioxide, in air", "type": "emission", "categories": ("air",)},
    })
    bd.Database("ei").write({
        # biomass-growth dataset: has the CO2-in-air biosphere exchange
        ("ei", "forest"): {
            "name": "forestry", "reference product": "wood", "location": "GLO", "unit": "kg",
            "exchanges": [
                {"input": ("ei", "forest"), "amount": 1.0, "type": "production"},
                {"input": ("bio", "co2"), "amount": -2.0, "type": "biosphere"},
            ],
        },
        # supplier used as stock_asset, maintenance, and end_of_life by consumers below
        ("ei", "machine"): {
            "name": "machine", "reference product": "machine", "location": "GLO", "unit": "unit",
            "exchanges": [{"input": ("ei", "machine"), "amount": 1.0, "type": "production"}],
        },
        # consumer with a 50-year lifetime that buys the machine (tagged maintenance/eol per specs)
        ("ei", "plant"): {
            "name": "plant", "reference product": "power", "location": "GLO", "unit": "kWh",
            "exchanges": [
                {"input": ("ei", "plant"), "amount": 1.0, "type": "production"},
                {"input": ("ei", "machine"), "amount": 0.1, "type": "technosphere"},
            ],
        },
    })


@bw2test
def test_biomass_growth_lands_on_co2_exchange():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={("forestry", "wood"): {
            "temporal_distribution": 3, "temporal_loc": -20.0, "temporal_scale": 3.0,
            "temporal_min": -40.0, "temporal_max": -1.0}},
        stock_asset_params={}, maintenance_suppliers=set(),
        end_of_life_suppliers=set(), dataset_lifetimes={},
    )
    report = annotate_database("ei", specs)
    forest = bd.get_node(database="ei", code="forest")
    bio_exc = [e for e in forest.exchanges() if e["type"] == "biosphere"][0]
    assert bio_exc.get("temporal_distribution") is not None
    assert report.annotated == 1


@bw2test
def test_maintenance_uniform_over_lifetime():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={}, stock_asset_params={},
        maintenance_suppliers={("machine", "machine")}, end_of_life_suppliers=set(),
        dataset_lifetimes={("plant", "power"): 50.0},
    )
    report = annotate_database("ei", specs)
    plant = bd.get_node(database="ei", code="plant")
    tech_exc = [e for e in plant.exchanges() if e["type"] == "technosphere"][0]
    td = tech_exc.get("temporal_distribution")
    assert td is not None
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == 0 and yrs.max() == 50
    assert report.annotated == 1


@bw2test
def test_ambiguous_supplier_is_faulted_not_applied():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={}, stock_asset_params={},
        maintenance_suppliers={("machine", "machine")},
        end_of_life_suppliers={("machine", "machine")},
        dataset_lifetimes={("plant", "power"): 50.0},
    )
    report = annotate_database("ei", specs)
    plant = bd.get_node(database="ei", code="plant")
    tech_exc = [e for e in plant.exchanges() if e["type"] == "technosphere"][0]
    assert tech_exc.get("temporal_distribution") is None
    assert report.annotated == 0 and len(report.faults) == 1


@bw2test
def test_idempotent_skip_then_overwrite():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={("forestry", "wood"): {
            "temporal_distribution": 1, "temporal_loc": -5.0}},
        stock_asset_params={}, maintenance_suppliers=set(),
        end_of_life_suppliers=set(), dataset_lifetimes={},
    )
    annotate_database("ei", specs)
    again = annotate_database("ei", specs)
    assert again.annotated == 0 and again.skipped_existing >= 1
    forced = annotate_database("ei", specs, overwrite=True)
    assert forced.annotated == 1


@bw2test
def test_unknown_database_raises():
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    specs = TemporalSpecs({}, {}, set(), set(), {})
    with pytest.raises(ValueError):
        annotate_database("does-not-exist", specs)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_premise_temporal.py -k "biomass_growth_lands or maintenance_uniform or ambiguous or idempotent or unknown_database" -v`
Expected: FAIL (`annotate_database` not defined).

- [ ] **Step 3: Write the implementation**

```python
# append to bw_timex/premise_temporal.py
def _clean(value) -> str:
    return (value or "").strip()


def _supplier_key(exchange) -> tuple[str, str]:
    supplier = exchange.input
    return _clean(supplier.get("name")), _clean(
        supplier.get("reference product") or supplier.get("product")
    )


def annotate_database(db_name, specs: TemporalSpecs, *, overwrite: bool = False) -> AnnotationReport:
    """Write temporal distributions onto an existing premise bw2 database.

    Mirrors premise's ``add_temporal_distributions`` placement rules using the
    buckets in ``specs``. Returns an :class:`AnnotationReport`; never raises out
    of a single bad exchange (records a fault and continues).
    """
    import bw2data as bd

    if db_name not in bd.databases:
        raise ValueError(f"Database {db_name!r} not found in the current project.")

    report = AnnotationReport()

    def _fault(ds, exc, reason):
        report.faults.append({
            "database": db_name,
            "dataset": f"{_clean(ds.get('name'))} | {_clean(ds.get('reference product'))}",
            "exchange": _clean(exc.get("name")),
            "reason": reason,
        })

    def _apply(exc, td):
        exc["temporal_distribution"] = td
        exc.save()
        report.annotated += 1

    for ds in bd.Database(db_name):
        ds_key = (_clean(ds.get("name")), _clean(ds.get("reference product")))
        bg = specs.biomass_growth_params.get(ds_key)
        ds_lifetime = specs.dataset_lifetimes.get(ds_key)

        for exc in ds.exchanges():
            if not overwrite and exc.get("temporal_distribution") is not None:
                report.skipped_existing += 1
                continue

            etype = exc.get("type")

            if etype == "biosphere":
                if (
                    bg is not None
                    and _clean(exc.input.get("name")) == "Carbon dioxide, in air"
                    and bg.get("temporal_distribution") is not None
                ):
                    _apply(exc, premise_params_to_td(bg))
                continue

            if etype != "technosphere":
                continue

            sup_name, sup_ref = _supplier_key(exc)
            if not sup_ref:
                _fault(ds, exc, "Missing supplier product on technosphere exchange.")
                continue
            key = (sup_name, sup_ref)

            params = specs.stock_asset_params.get(key)
            is_maintenance = key in specs.maintenance_suppliers
            is_end_of_life = key in specs.end_of_life_suppliers
            matched = int(params is not None) + int(is_maintenance) + int(is_end_of_life)

            if matched == 0:
                continue
            if matched > 1:
                _fault(ds, exc, f"Ambiguous temporal tags for supplier {key}.")
                continue

            if params is not None:
                _apply(exc, premise_params_to_td(params))
                continue

            if ds_lifetime is None:
                _fault(ds, exc, "Missing dataset lifetime for maintenance/end_of_life.")
                continue

            if is_maintenance:
                _apply(exc, premise_params_to_td(
                    {"temporal_distribution": 4, "temporal_min": 0.0, "temporal_max": ds_lifetime}))
            else:  # end_of_life
                _apply(exc, premise_params_to_td(
                    {"temporal_distribution": 6, "temporal_offsets": [ds_lifetime], "temporal_weights": [1.0]}))

    return report
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_premise_temporal.py -v`
Expected: all PASS (Task 1 + Task 2 tests).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/premise_temporal.py tests/test_premise_temporal.py
git commit  # "feat: annotate_database applying premise placement rules"
```

---

### Task 3: `load_temporal_specs` — reuse premise's CSV loader (optional dep)

**Files:**
- Modify: `bw_timex/premise_temporal.py`
- Test: `tests/test_premise_temporal.py`

**Interfaces:**
- Consumes: `TemporalSpecs` (Task 1); `premise.trails` (lazy, optional).
- Produces:
  - `load_temporal_specs(path=None) -> TemporalSpecs`.
  - `_import_premise_trails()` helper raising a clear error when premise lacks temporal support.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_premise_temporal.py
def test_import_error_when_premise_missing(monkeypatch):
    import builtins
    from bw_timex import premise_temporal
    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "premise" or name.startswith("premise."):
            raise ImportError("no premise")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="bw-timex\\[premise\\]"):
        premise_temporal.load_temporal_specs()


def test_load_temporal_specs_reads_premise_csv():
    pytest.importorskip("premise")
    from bw_timex.premise_temporal import load_temporal_specs, TemporalSpecs
    try:
        specs = load_temporal_specs()
    except RuntimeError:
        pytest.skip("installed premise lacks TrailsDataPackage temporal support")
    assert isinstance(specs, TemporalSpecs)
    # premise's bundled CSV is non-empty across at least one bucket
    assert (
        specs.biomass_growth_params
        or specs.stock_asset_params
        or specs.maintenance_suppliers
        or specs.end_of_life_suppliers
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_premise_temporal.py::test_import_error_when_premise_missing -v`
Expected: FAIL (`load_temporal_specs` not defined).

- [ ] **Step 3: Write the implementation**

```python
# append to bw_timex/premise_temporal.py
def _import_premise_trails():
    """Import premise's trails module, or raise a clear, actionable error."""
    try:
        from premise import trails as premise_trails
    except ImportError as exc:
        raise ImportError(
            "premise temporal annotation requires premise (>=2.5.0). "
            "Install it with: pip install bw-timex[premise]"
        ) from exc
    if not hasattr(premise_trails, "TrailsDataPackage") or not hasattr(
        premise_trails, "FILEPATH_TEMPORAL_PARAMETERS"
    ):
        raise RuntimeError(
            "The installed premise lacks temporal-distribution support "
            "(TrailsDataPackage / temporal_distributions.csv). Upgrade to premise>=2.5.0."
        )
    return premise_trails


class _DummySelf:
    """Stand-in for the unused ``self`` of premise's CSV loader method."""


def load_temporal_specs(path=None) -> TemporalSpecs:
    """Load premise's curated temporal specs into a :class:`TemporalSpecs`.

    Reuses premise's own ``_load_temporal_specs_from_csv`` (which ignores
    ``self``) so parsing/categorization stays in premise. ``path`` defaults to
    premise's bundled ``temporal_distributions.csv``.
    """
    premise_trails = _import_premise_trails()
    csv_path = path if path is not None else premise_trails.FILEPATH_TEMPORAL_PARAMETERS
    loader = premise_trails.TrailsDataPackage._load_temporal_specs_from_csv
    stock_assets, end_of_life, biomass_growth, maintenance, dataset_lifetimes = loader(
        _DummySelf(), csv_path
    )
    return TemporalSpecs(
        biomass_growth_params=biomass_growth,
        stock_asset_params=stock_assets,
        maintenance_suppliers=maintenance,
        end_of_life_suppliers=end_of_life,
        dataset_lifetimes=dataset_lifetimes,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_premise_temporal.py -v`
Expected: PASS (`test_import_error_when_premise_missing` passes; `test_load_temporal_specs_reads_premise_csv` passes if premise installed, else skips).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/premise_temporal.py tests/test_premise_temporal.py
git commit  # "feat: load_temporal_specs reusing premise CSV loader"
```

---

### Task 4: Public API + export + optional extra

**Files:**
- Modify: `bw_timex/premise_temporal.py` (add `add_premise_temporal_distributions`)
- Modify: `bw_timex/__init__.py` (export)
- Modify: `pyproject.toml` (premise extra)
- Test: `tests/test_premise_temporal.py`

**Interfaces:**
- Consumes: `load_temporal_specs`, `annotate_database`, `AnnotationReport` (Tasks 2–3).
- Produces: `add_premise_temporal_distributions(databases, *, overwrite=False) -> AnnotationReport`, exported as `bw_timex.add_premise_temporal_distributions`.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_premise_temporal.py
def test_public_export():
    import bw_timex
    assert hasattr(bw_timex, "add_premise_temporal_distributions")


@bw2test
def test_add_premise_temporal_distributions_uses_injected_specs(monkeypatch):
    import bw2data as bd
    from bw_timex import premise_temporal
    from bw_timex.premise_temporal import TemporalSpecs, add_premise_temporal_distributions
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={("forestry", "wood"): {"temporal_distribution": 1, "temporal_loc": -5.0}},
        stock_asset_params={}, maintenance_suppliers=set(),
        end_of_life_suppliers=set(), dataset_lifetimes={},
    )
    monkeypatch.setattr(premise_temporal, "load_temporal_specs", lambda *a, **k: specs)
    report = add_premise_temporal_distributions(["ei"])
    assert report.annotated == 1
    forest = bd.get_node(database="ei", code="forest")
    assert [e for e in forest.exchanges() if e["type"] == "biosphere"][0].get("temporal_distribution") is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_premise_temporal.py::test_public_export -v`
Expected: FAIL (`AttributeError`).

- [ ] **Step 3: Implement**

Add to `bw_timex/premise_temporal.py`:

```python
def add_premise_temporal_distributions(databases, *, overwrite: bool = False) -> AnnotationReport:
    """Annotate existing premise databases with temporal distributions.

    ``databases`` is an iterable of database names (or a mapping whose keys are
    database names; values are ignored). Loads premise's temporal specs once and
    annotates each database. Returns an aggregated :class:`AnnotationReport`.
    """
    names = list(databases.keys()) if isinstance(databases, dict) else list(databases)
    specs = load_temporal_specs()
    report = AnnotationReport()
    for name in names:
        report.merge(annotate_database(name, specs, overwrite=overwrite))
    return report
```

Add to `bw_timex/__init__.py` after the existing imports:

```python
from .premise_temporal import add_premise_temporal_distributions
```

In `pyproject.toml`, under `[project.optional-dependencies]`, add:

```toml
premise = [
    "premise>=2.5.0",
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_premise_temporal.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/premise_temporal.py bw_timex/__init__.py pyproject.toml tests/test_premise_temporal.py
git commit  # "feat: public add_premise_temporal_distributions + premise extra"
```

---

### Task 5: Validation gate — full suite + premise-free import check

**Files:**
- Test: `tests/test_premise_temporal.py`

**Interfaces:**
- Consumes: the whole module (Tasks 1–4).
- Produces: a guard that `bw_timex` imports and the suite passes with premise NOT required.

- [ ] **Step 1: Add a premise-free import guard test**

```python
# append to tests/test_premise_temporal.py
def test_core_import_does_not_require_premise(monkeypatch):
    # Importing bw_timex and using the converter must not require premise.
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "premise" or name.startswith("premise."):
            raise ImportError("premise blocked")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    import importlib
    import bw_timex.premise_temporal as pt
    importlib.reload(pt)
    td = pt.premise_params_to_td({"temporal_distribution": 1, "temporal_loc": 0.0})
    assert td is not None
```

- [ ] **Step 2: Run the new test**

Run: `uv run pytest tests/test_premise_temporal.py::test_core_import_does_not_require_premise -v`
Expected: PASS (module import + converter work without premise).

- [ ] **Step 3: Run the full suite**

Run: `uv run pytest -q`
Expected: all pre-existing tests still PASS, plus `tests/test_premise_temporal.py`; no new warnings.

- [ ] **Step 4: Commit**

```bash
git add tests/test_premise_temporal.py
git commit  # "test: premise_temporal validation gate"
```

---

## Self-Review

**Spec coverage:**
- Reuse premise CSV loader → Task 3 (`load_temporal_specs` calls premise's `_load_temporal_specs_from_csv`). ✓
- premise→TD converter for all codes (1/3/4/5/6), years → Task 1. ✓
- placement rules (biomass_growth/stock_asset/maintenance/end_of_life, ambiguity, missing-lifetime/supplier faults) → Task 2. ✓
- idempotency / overwrite → Task 2. ✓
- public API + export → Task 4. ✓
- premise optional extra + lazy import + feature-detect guard → Tasks 3–4. ✓
- error handling (premise missing, unknown db, per-exchange faults non-fatal) → Tasks 2–3. ✓
- testing incl. premise-free core → Tasks 1–5. ✓
- **Deviation from spec:** the spec described a "reference test comparing placement to premise's own `add_temporal_distributions` output." premise's assignment is not standalone-callable (it is a method over a `TrailsDataPackage` with scenario plumbing), so the drift guard here is instead (a) reusing premise's loader verbatim for the parsing/categorization and (b) rule-encoded annotation tests (Task 2) that assert each premise rule explicitly. This is the practical equivalent; noted for the reviewer.
- Out of scope (unfold/materialize/database_dates) → not present. ✓

**Placeholder scan:** No TBD/TODO/"add error handling". All code steps contain complete code. ✓

**Type consistency:** `TemporalSpecs` field names (`biomass_growth_params`, `stock_asset_params`, `maintenance_suppliers`, `end_of_life_suppliers`, `dataset_lifetimes`) consistent across Tasks 1–4; premise loader 5-tuple order `(stock_assets, end_of_life, biomass_growth, maintenance, dataset_lifetimes)` mapped correctly in Task 3; `premise_params_to_td` / `annotate_database` / `add_premise_temporal_distributions` / `AnnotationReport.merge` signatures consistent across tasks and tests. ✓
