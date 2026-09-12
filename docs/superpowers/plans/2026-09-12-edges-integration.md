# edges Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Characterise a bw_timex time-explicit inventory with `edges` LCIA methods, evaluating every exchange's characterisation factor at that exchange's own year.

**Architecture:** A new module `bw_timex/edges_lcia.py` defines `TimexEdgeLCIA`, a subclass of `edges.EdgeLCIA` that (a) replaces `edges`' flow-metadata resolution so it understands bw_timex's time-mapped activity ids, and (b) loops over the distinct years in the system, calling `evaluate_cfs(scenario_idx=year)` once per year and applying each year's CF matrix only to that year's inventory entries. Biosphere entries come from the **dynamic inventory**, which is the only object carrying temporal-distribution-resolved emission dates; technosphere entries come from the expanded technosphere flow matrix and use the consuming process's vintage. `TimexLCA.edges_lcia()` is the single public entry point.

**Tech Stack:** Python ≥3.11, `edges` ≥1.4.1, `bw2data` ≥4.6, `bw2calc` ≥2.4, `scipy.sparse`, `pandas`, `numpy`, `pydantic` ≥2, `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-12-edges-integration-design.md` — read it before starting. It explains *why* the biosphere side must not go through `lca.inventory`.

## Global Constraints

- `edges` is an **optional** dependency. The core install and the core test suite must keep working without it. All `edges` imports live in `bw_timex/edges_lcia.py` and are triggered only from inside `TimexLCA.edges_lcia()`.
- `edges` requires Python `>=3.10,<3.13`; bw_timex requires `>=3.11`. The extra therefore installs on 3.11 and 3.12 only, expressed with the environment marker `python_version < '3.13'`.
- Minimum `edges` version: `1.4.1`.
- Biosphere characterisation MUST read `TimexLCA.dynamic_inventory` (or `dynamic_inventory_disaggregated`), never `TimexLCA.lca.inventory`. Going through the static expanded biosphere matrix silently characterises temporal-distribution-spread emissions at the process's year. Any test that would pass with process-time biosphere years is an insufficient test.
- Only this narrow `edges` surface may be used: the `EdgeLCIA` constructor kwargs, `lci()`, `map_exchanges()`, `map_aggregate_locations()`, `map_dynamic_locations()`, `map_contained_locations()`, `map_remaining_locations_to_global()`, `evaluate_cfs()`, the `characterization_matrices` attribute, `_uses_biosphere_supplier_matrix()`, `_uses_technosphere_supplier_matrix()`, `edges.utils.get_flow_matrix_positions`, and `edges.matrix_builders.build_technosphere_edges_matrix`. Do not reach for other private helpers.
- v1 supports `lci(expand_technosphere=True)` only. The timeline route raises `NotImplementedError`.
- CF uncertainty (`use_distributions=True`) is out of scope for v1.
- Do NOT import `bw_timex.edges_lcia` from `bw_timex/__init__.py` — that would make the optional dependency mandatory.
- Follow the existing code style: `loguru` logger, numpydoc docstrings, pydantic input models in `validation.py`, `black`-compatible formatting (line length 100).

## File Structure

| File | Responsibility |
|---|---|
| `bw_timex/edges_lcia.py` *(new)* | `TimexEdgeLCIA` adapter: guarded `edges` import, time-mapped flow translation, per-year CF evaluation, CF table. |
| `bw_timex/utils.py` *(modify)* | `year_from_time_mapped_timestamp()` — pure date helper, no `edges` dependency. |
| `bw_timex/validation.py` *(modify)* | `EdgesLCIAInputs` pydantic model. |
| `bw_timex/timex_lca.py` *(modify)* | Public `edges_lcia()` method, `edges_score` property, guard clauses. |
| `pyproject.toml` *(modify)* | `edges` optional extra. |
| `.github/workflows/python-test.yml` *(modify)* | Extra CI job running the `edges` tests on 3.11/3.12. |
| `tests/fixtures/edges_td_db_fixture.py` *(new)* | Fixture database whose biosphere exchange carries a TD spanning two calendar years. |
| `tests/fixtures/edges_methods/*.json` *(new)* | Two small `edges` method files: constant CF and year-dependent CF. |
| `tests/test_edges_lcia.py` *(new)* | All tests for this feature. |
| `CHANGES.md`, `docs/content/examples/` *(modify/new)* | Changelog entry and example notebook. |

---

### Task 1: Optional dependency, guarded import, input validation

**Files:**
- Create: `bw_timex/edges_lcia.py`
- Modify: `bw_timex/validation.py`
- Modify: `pyproject.toml`
- Modify: `.github/workflows/python-test.yml`
- Test: `tests/test_edges_lcia.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `bw_timex.edges_lcia.EDGES_IMPORT_ERROR` (str); module-level names `EdgeLCIA`, `build_technosphere_edges_matrix`, `get_flow_matrix_positions` re-exported from `edges`; `bw_timex.validation.EdgesLCIAInputs` with fields `method`, `parameters`, `scenario`, `weight`, `filepath`, `allowed_functions`, `regionalized`, `use_disaggregated_lci`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_edges_lcia.py`:

```python
"""Tests for the `edges` integration (TimexLCA.edges_lcia)."""

import importlib
import sys

import pytest
from pydantic import ValidationError


def test_missing_edges_gives_actionable_import_error(monkeypatch):
    """Without `edges` installed, importing the adapter names the extra and the Python ceiling."""
    monkeypatch.setitem(sys.modules, "edges", None)
    monkeypatch.delitem(sys.modules, "bw_timex.edges_lcia", raising=False)

    with pytest.raises(ImportError) as exc_info:
        importlib.import_module("bw_timex.edges_lcia")

    message = str(exc_info.value)
    assert "bw_timex[edges]" in message
    assert "3.13" in message


def test_edges_lcia_inputs_defaults():
    from bw_timex.validation import EdgesLCIAInputs

    inputs = EdgesLCIAInputs(method=("some", "method"))

    assert inputs.weight == "population"
    assert inputs.regionalized is True
    assert inputs.use_disaggregated_lci is False
    assert inputs.parameters is None


def test_edges_lcia_inputs_rejects_empty_method():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method=())


def test_edges_lcia_inputs_rejects_dict_without_exchanges():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method={"name": "no exchanges here"})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_edges_lcia.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bw_timex.edges_lcia'` and `ImportError: cannot import name 'EdgesLCIAInputs'`.

- [ ] **Step 3: Create the guarded-import module**

Create `bw_timex/edges_lcia.py`:

```python
"""
Characterisation of a bw_timex time-explicit inventory with the `edges` package.

`edges` (https://edges.readthedocs.io) characterises *exchanges* rather than flows, and its
characterisation factors can be symbolic expressions evaluated per scenario year. bw_timex knows
when each process runs and when each emission occurs, so the two combine into characterisation
factors evaluated at each exchange's own year.
"""

from __future__ import annotations

EDGES_IMPORT_ERROR = (
    "The `edges` package is required for TimexLCA.edges_lcia(). Install it with "
    "`pip install bw_timex[edges]`. Note that edges supports Python >=3.10,<3.13, while "
    "bw_timex itself also runs on 3.13 - on 3.13 the extra installs nothing."
)

try:
    from edges import EdgeLCIA
    from edges.matrix_builders import build_technosphere_edges_matrix
    from edges.utils import get_flow_matrix_positions
except ImportError as exc:
    raise ImportError(EDGES_IMPORT_ERROR) from exc
```

- [ ] **Step 4: Add the validation model**

In `bw_timex/validation.py`, add `from pathlib import Path` to the imports and append this class after `DynamicLCIAInputs`:

```python
class EdgesLCIAInputs(BaseModel):
    """Validates inputs to TimexLCA.edges_lcia"""

    model_config = {"arbitrary_types_allowed": True}

    method: Union[tuple, str, Path, dict]
    parameters: Optional[dict] = None
    scenario: Optional[str] = None
    weight: str = "population"
    filepath: Optional[Union[str, Path]] = None
    allowed_functions: Optional[dict] = None
    regionalized: bool = True
    use_disaggregated_lci: bool = False

    @field_validator("method")
    @classmethod
    def validate_method(cls, v):
        if isinstance(v, tuple) and not v:
            raise ValueError(
                "method must be a non-empty tuple, a path to an edges method JSON file, "
                "or a dict containing an 'exchanges' key."
            )
        if isinstance(v, dict) and "exchanges" not in v:
            raise ValueError(
                "An edges method given as a dict must contain an 'exchanges' key."
            )
        return v
```

- [ ] **Step 5: Add the optional extra**

In `pyproject.toml`, under `[project.optional-dependencies]`, add:

```toml
edges = [
    "edges>=1.4.1; python_version < '3.13'",
]
```

- [ ] **Step 6: Add the CI job**

In `.github/workflows/python-test.yml`, after the existing `build` job, add:

```yaml
  edges:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        py-version: ["3.11", "3.12"]

    steps:
      - uses: actions/checkout@v7

      - name: Install uv
        uses: astral-sh/setup-uv@08807647e7069bb48b6ef5acd8ec9567f424441b

      - name: Set up Python ${{ matrix.py-version }}
        run: uv python install ${{ matrix.py-version }}

      - name: Install dependencies
        run: uv sync --extra testing --extra edges

      - name: Test the edges integration
        run: uv run pytest tests/test_edges_lcia.py -v
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `pytest tests/test_edges_lcia.py -v`
Expected: PASS (4 passed). They pass whether or not `edges` is installed locally.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/edges_lcia.py bw_timex/validation.py pyproject.toml .github/workflows/python-test.yml tests/test_edges_lcia.py
git commit -m "feat: optional edges extra and input validation for edges_lcia"
```

---

### Task 2: Year extraction from time-mapped timestamps

**Files:**
- Modify: `bw_timex/utils.py`
- Test: `tests/test_utils.py`

**Interfaces:**
- Consumes: `bw_timex.utils.convert_date_string_to_datetime(temporal_grouping, date_string)`.
- Produces: `bw_timex.utils.year_from_time_mapped_timestamp(timestamp, temporal_grouping) -> int | None`. Returns `None` for the `"dynamic"` sentinel. Lives in `utils.py`, not in `edges_lcia.py`, so it is testable without `edges` installed.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_utils.py`:

```python
def test_year_from_time_mapped_timestamp_all_groupings():
    from bw_timex.utils import year_from_time_mapped_timestamp

    assert year_from_time_mapped_timestamp(2024, "year") == 2024
    assert year_from_time_mapped_timestamp(202403, "month") == 2024
    assert year_from_time_mapped_timestamp(20240315, "day") == 2024
    assert year_from_time_mapped_timestamp(2024031514, "hour") == 2024


def test_year_from_time_mapped_timestamp_dynamic_sentinel():
    """Foreground activities with unresolved timing carry the string 'dynamic'."""
    from bw_timex.utils import year_from_time_mapped_timestamp

    assert year_from_time_mapped_timestamp("dynamic", "year") is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_utils.py -k year_from_time_mapped -v`
Expected: FAIL with `ImportError: cannot import name 'year_from_time_mapped_timestamp'`.

- [ ] **Step 3: Implement the helper**

In `bw_timex/utils.py`, add directly after `convert_date_string_to_datetime`:

```python
def year_from_time_mapped_timestamp(
    timestamp: Union[int, str], temporal_grouping: str
) -> Optional[int]:
    """
    Extracts the calendar year from a timestamp of the `activity_time_mapping`.

    Timestamps are integers of the form YYYY, YYYYMM, YYYYMMDD or YYYYMMDDHH, depending on the
    `temporal_grouping`. Foreground activities whose timing is not yet resolved carry the string
    "dynamic" instead of an integer; for those, None is returned.

    Parameters
    ----------
    timestamp : int or str
        Timestamp from `activity_time_mapping`, or the string "dynamic".
    temporal_grouping : str
        Temporal grouping of the TimexLCA. Options are: 'year', 'month', 'day', 'hour'.

    Returns
    -------
    int or None
        The calendar year, or None if the timestamp is not time-resolved.
    """
    if isinstance(timestamp, str):
        return None
    return convert_date_string_to_datetime(temporal_grouping, str(timestamp)).year
```

Make sure `Union` and `Optional` are imported in `utils.py` (they already are; verify with `grep -n "^from typing" bw_timex/utils.py`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_utils.py -k year_from_time_mapped -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/utils.py tests/test_utils.py
git commit -m "feat: helper to extract the calendar year from time-mapped timestamps"
```

---

### Task 3: Test fixture with a biosphere temporal distribution across two years

This fixture is the backbone of every later test: without a biosphere exchange whose emissions fall in two different calendar years, none of the later tests can distinguish emission-time from process-time characterisation.

**Files:**
- Create: `tests/fixtures/edges_td_db_fixture.py`
- Create: `tests/fixtures/edges_methods/constant_cf.json`
- Create: `tests/fixtures/edges_methods/year_dependent_cf.json`
- Modify: `tests/conftest.py`
- Test: `tests/test_edges_lcia.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: pytest fixture `edges_td_db` (no return value; writes Brightway databases `bio`, `db_2020`, `foreground` and method `("GWP", "example")` into a fresh test project). Method files are addressed from tests as `Path(__file__).parent / "fixtures" / "edges_methods" / "<name>.json"`.

- [ ] **Step 1: Write the fixture database**

Create `tests/fixtures/edges_td_db_fixture.py`:

```python
import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def edges_td_db():
    """
    Minimal database for the edges integration.

    The foreground process emits 10 kg CO2, but the biosphere exchange carries a temporal
    distribution that splits the emission 40 % / 60 % across two consecutive years. That split is
    what distinguishes emission-time from process-time characterisation.
    """
    bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "categories": ("air",),
                "unit": "kilogram",
            },
        },
    )

    bd.Database("db_2020").write(
        {
            ("db_2020", "electricity"): {
                "name": "electricity production",
                "location": "CH",
                "reference product": "electricity",
                "unit": "kilowatt hour",
                "type": "process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("db_2020", "electricity"),
                    },
                    {
                        "amount": 2,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "heat"): {
                "name": "heat production",
                "location": "CH",
                "reference product": "heat",
                "unit": "megajoule",
                "type": "process",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "heat"),
                    },
                    {
                        "amount": 10,
                        "type": "biosphere",
                        "input": ("bio", "CO2"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([0, 1], dtype="timedelta64[Y]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                    {
                        "amount": 3,
                        "type": "technosphere",
                        "input": ("db_2020", "electricity"),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("bio", "CO2"), 1),
        ]
    )
```

- [ ] **Step 2: Register the fixture**

In `tests/conftest.py`, add alongside the other fixture imports:

```python
from .fixtures.edges_td_db_fixture import edges_td_db
```

- [ ] **Step 3: Write the constant-CF method file**

Create `tests/fixtures/edges_methods/constant_cf.json`:

```json
{
  "name": "test constant CF",
  "version": "1.0",
  "unit": "kg CO2-eq",
  "description": "Constant CF of 1.0 for carbon dioxide, used to prove conservation.",
  "exchanges": [
    {
      "supplier": {
        "name": "carbon dioxide",
        "categories": ["air"],
        "matrix": "biosphere"
      },
      "consumer": {
        "matrix": "technosphere"
      },
      "value": 1.0
    }
  ]
}
```

- [ ] **Step 4: Write the year-dependent method file**

Create `tests/fixtures/edges_methods/year_dependent_cf.json`:

```json
{
  "name": "test year-dependent CF",
  "version": "1.0",
  "unit": "kg CO2-eq",
  "description": "CF that doubles from one year to the next, to expose the CF evaluation year.",
  "interpolation": {
    "axis": "scenario_idx",
    "axis_type": "year",
    "method": "linear",
    "extrapolation": "nearest"
  },
  "exchanges": [
    {
      "supplier": {
        "name": "carbon dioxide",
        "categories": ["air"],
        "matrix": "biosphere"
      },
      "consumer": {
        "matrix": "technosphere"
      },
      "value": 1.0,
      "value_expression": "cf_co2"
    }
  ]
}
```

- [ ] **Step 5: Write the test that proves the fixture is time-explicit as intended**

Append to `tests/test_edges_lcia.py`:

```python
from datetime import datetime

import bw2data as bd


def test_fixture_emits_in_two_calendar_years(edges_td_db):
    """The biosphere TD must land emissions in two years, otherwise later tests prove nothing."""
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci()

    emissions = timex_lca.dynamic_inventory_df
    years = sorted(emissions.date.dt.year.unique())

    assert years == [2024, 2025]
    foreground_emissions = emissions.groupby(emissions.date.dt.year).amount.sum()
    assert foreground_emissions.loc[2025] == pytest.approx(6.0)
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `pytest tests/test_edges_lcia.py -k fixture_emits -v`
Expected: PASS. If it fails, the fixture, not the test, is wrong — check `starting_datetime` and the TD. Note that the 2024 total also contains the background electricity emissions (3 × 2 = 6 kg at the 2020 database's time, which the timeline places at the process's time), so only the 2025 value is asserted exactly.

- [ ] **Step 7: Commit**

```bash
git add tests/fixtures/edges_td_db_fixture.py tests/fixtures/edges_methods tests/conftest.py tests/test_edges_lcia.py
git commit -m "test: fixture database with a biosphere temporal distribution across two years"
```

---

### Task 4: Adapter construction and time-mapped flow translation

`edges`' own `lci()` calls `self.lca.lci(factorize=True)` and resolves `lca.dicts.activity` keys through `bw2data.get_activities()` (see `edges/utils.py:393`). Both are wrong here: the inventory is already solved, and bw_timex's time-mapped ids do not exist in bw2data. This task replaces that method.

**Files:**
- Modify: `bw_timex/edges_lcia.py`
- Test: `tests/test_edges_lcia.py`

**Interfaces:**
- Consumes: `bw_timex.utils.year_from_time_mapped_timestamp` (Task 2); the `edges_td_db` fixture (Task 3).
- Produces: `bw_timex.edges_lcia.TimexEdgeLCIA(timex_lca, method, **edge_kwargs)` with attributes `timex_lca`, `position_to_timestamp` (`dict[int, int | str]`), and a working `lci()` that populates `biosphere_flows`, `technosphere_flows`, `biosphere_edges`, `technosphere_edges`, `reversed_activity`, `reversed_biosphere`, `position_to_technosphere_flows_lookup`, and `position_to_biosphere_flows_lookup`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_edges_lcia.py`:

```python
edges = pytest.importorskip("edges")


@pytest.fixture
def timex_lca_with_lci(edges_td_db):
    """A TimexLCA with a finished time-explicit LCI, shared by the edges tests."""
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci()
    return timex_lca


def method_path(name):
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "edges_methods" / f"{name}.json"


def test_translation_gives_each_position_its_node_metadata(timex_lca_with_lci):
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca_with_lci,
        method=("test", "constant"),
        filepath=str(method_path("constant_cf")),
    )
    adapter.lci()

    names = {flow["name"] for flow in adapter.technosphere_flows}
    assert "heat production" in names
    assert "electricity production" in names
    assert all(flow["location"] == "CH" for flow in adapter.technosphere_flows)
    assert all("position" in flow for flow in adapter.technosphere_flows)

    # one entry per matrix column, including several vintages of the same node
    assert len(adapter.technosphere_flows) == len(timex_lca_with_lci.lca.dicts.activity)
    assert set(adapter.position_to_timestamp) == set(
        timex_lca_with_lci.lca.dicts.activity.values()
    )


def test_translation_does_not_resolve_time_mapped_ids_through_bw2data(timex_lca_with_lci):
    """Time-mapped ids do not exist in bw2data; edges' own lci() would raise a KeyError."""
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca_with_lci,
        method=("test", "constant"),
        filepath=str(method_path("constant_cf")),
    )
    adapter.lci()

    assert adapter.biosphere_edges, "biosphere edges should be populated"
    assert adapter.biosphere_flows, "biosphere flows should be populated"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_edges_lcia.py -k translation -v`
Expected: FAIL with `ImportError: cannot import name 'TimexEdgeLCIA'`.

- [ ] **Step 3: Implement the adapter constructor and `lci()`**

Append to `bw_timex/edges_lcia.py`:

```python
import bw2data as bd
import numpy as np
import pandas as pd
from loguru import logger

from .utils import round_datetime_series_to_year, year_from_time_mapped_timestamp


class TimexEdgeLCIA(EdgeLCIA):
    """
    `edges.EdgeLCIA` subclass that understands bw_timex's time-explicit matrices.

    Two things differ from plain `edges`:

    1. The columns of the expanded technosphere matrix are time-mapped ids (from
       `TimexLCA.activity_time_mapping`) rather than Brightway ids, so flow metadata is resolved
       through that mapping instead of through `bw2data`.
    2. Characterisation factors are evaluated per year, using each exchange's own year.

    Parameters
    ----------
    timex_lca : TimexLCA
        A TimexLCA whose `lci()` has already been calculated with `expand_technosphere=True`.
    method : tuple, str, pathlib.Path or dict
        The edges method, passed through to `edges.EdgeLCIA`.
    **edge_kwargs
        Further keyword arguments for `edges.EdgeLCIA` (`parameters`, `scenario`, `weight`,
        `filepath`, `allowed_functions`).
    """

    def __init__(self, timex_lca, method, **edge_kwargs):
        self.timex_lca = timex_lca
        self.position_to_timestamp = {}
        self.position_to_biosphere_flows_lookup = {}
        super().__init__(
            demand=timex_lca.fu,
            method=method,
            lca=timex_lca.lca,
            **edge_kwargs,
        )

    def lci(self) -> None:
        """
        Collects the exchanges and flow metadata of the time-explicit inventory.

        Replaces `edges.EdgeLCIA.lci`, which would re-solve the already-solved inventory and
        would try to resolve bw_timex's time-mapped ids through `bw2data`.
        """
        lca = self.lca
        if not hasattr(lca, "inventory"):
            raise AttributeError(
                "LCI not yet calculated. Call TimexLCA.lci() before TimexLCA.edges_lcia()."
            )

        self.biosphere_edges = set()
        self.technosphere_edges = set()
        self.biosphere_flows = None
        self.technosphere_flow_matrix = None

        if self._uses_technosphere_supplier_matrix():
            self.technosphere_flow_matrix = build_technosphere_edges_matrix(
                lca.technosphere_matrix, lca.supply_array
            )
            self.technosphere_edges = set(zip(*self.technosphere_flow_matrix.nonzero()))

        if self._uses_biosphere_supplier_matrix():
            self.biosphere_edges = set(zip(*lca.inventory.nonzero()))

        biosphere_dict = lca.dicts.biosphere
        activity_dict = lca.dicts.activity

        unique_biosphere_flows = {edge[0] for edge in self.biosphere_edges}
        if unique_biosphere_flows:
            self.biosphere_flows = get_flow_matrix_positions(
                {
                    key: position
                    for key, position in biosphere_dict.items()
                    if position in unique_biosphere_flows
                }
            )

        self.technosphere_flows = self._translate_time_mapped_activities(activity_dict)

        self.reversed_activity = {v: k for k, v in activity_dict.items()}
        self.reversed_biosphere = {v: k for k, v in biosphere_dict.items()}

        self.position_to_technosphere_flows_lookup = {
            flow["position"]: {k: v for k, v in flow.items() if k != "position"}
            for flow in self.technosphere_flows
        }
        self.position_to_biosphere_flows_lookup = {
            flow["position"]: {k: v for k, v in flow.items() if k != "position"}
            for flow in (self.biosphere_flows or [])
        }

        self._base_supplier_lookup_bio = None
        self._base_supplier_lookup_tech = None
        self._base_consumer_lookup = None
        self._flows_version = (
            len(self.biosphere_flows) if self.biosphere_flows else 0,
            len(self.technosphere_flows),
        )

    def _translate_time_mapped_activities(self, activity_dict) -> list:
        """
        Builds edges' flow-metadata dicts for the columns of the expanded technosphere matrix.

        Each column is a (process, time) combination, so several columns can share one underlying
        Brightway node. edges keys its lookups by matrix position, so repeated metadata is fine.
        """
        reversed_mapping = self.timex_lca.activity_time_mapping.reversed
        node_cache = {}
        flows = []

        for time_mapped_id, position in activity_dict.items():
            key, timestamp = reversed_mapping[time_mapped_id]
            if key not in node_cache:
                node_cache[key] = bd.get_node(database=key[0], code=key[1])
            node = node_cache[key]

            flows.append(
                {
                    "name": node.get("name"),
                    "reference product": node.get("reference product"),
                    "categories": node.get("categories"),
                    "unit": node.get("unit"),
                    "location": node.get("location"),
                    "classifications": node.get("classifications"),
                    "type": node.get("type"),
                    "position": position,
                }
            )
            self.position_to_timestamp[position] = timestamp

        return flows
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_edges_lcia.py -k translation -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/edges_lcia.py tests/test_edges_lcia.py
git commit -m "feat: resolve time-mapped activity metadata for edges"
```

---

### Task 5: Per-year characterisation of the dynamic inventory

The core of the feature. Biosphere entries come from `TimexLCA.dynamic_inventory`, whose rows are `(flow, date)` pairs — the only place where biosphere temporal distributions survive.

**Files:**
- Modify: `bw_timex/edges_lcia.py`
- Test: `tests/test_edges_lcia.py`

**Interfaces:**
- Consumes: `TimexEdgeLCIA.lci()` (Task 4); `edges`' `map_exchanges()`, `evaluate_cfs()`, `characterization_matrices`.
- Produces:
  - `TimexEdgeLCIA.run_mapping(regionalized: bool) -> None`
  - `TimexEdgeLCIA.characterize_time_explicit(use_disaggregated_lci: bool = False) -> pandas.DataFrame`, which sets `self.score` (float) and returns the CF table with columns `supplier`, `supplier categories`, `consumer`, `consumer location`, `activity`, `direction`, `date`, `year`, `amount`, `CF`, `impact`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_edges_lcia.py`:

```python
def _adapter(timex_lca, name, **kwargs):
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca,
        method=("test", name),
        filepath=str(method_path(name)),
        **kwargs,
    )
    adapter.lci()
    adapter.run_mapping(regionalized=False)
    return adapter


def test_constant_cf_reproduces_the_static_score(timex_lca_with_lci):
    """A CF of 1.0 per kg CO2 must give the same total as the static bw2calc score."""
    timex_lca_with_lci.static_lcia()

    adapter = _adapter(timex_lca_with_lci, "constant_cf")
    adapter.characterize_time_explicit()

    assert adapter.score == pytest.approx(timex_lca_with_lci.static_score, rel=1e-9)


def test_cf_is_evaluated_at_the_emission_year_not_the_process_year(timex_lca_with_lci):
    """
    The foreground emits 4 kg in 2024 and 6 kg in 2025, although the process itself runs in 2024.
    With CF 1 in 2024 and CF 2 in 2025, the foreground contribution is 4*1 + 6*2 = 16.
    Characterising at the process year would instead give (4 + 6) * 1 = 10.
    """
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    adapter = _adapter(
        timex_lca_with_lci,
        "year_dependent_cf",
        parameters=parameters,
        scenario="test",
    )
    table = adapter.characterize_time_explicit()

    foreground = table[table["consumer"] == "heat production"]
    assert foreground["impact"].sum() == pytest.approx(16.0)
    assert sorted(foreground["year"].unique()) == [2024, 2025]
    assert adapter.score == pytest.approx(table["impact"].sum())


def test_cf_table_carries_dates_and_time_mapped_activity(timex_lca_with_lci):
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    adapter = _adapter(
        timex_lca_with_lci,
        "year_dependent_cf",
        parameters=parameters,
        scenario="test",
    )
    table = adapter.characterize_time_explicit()

    for column in ("supplier", "consumer", "date", "year", "amount", "CF", "impact", "activity"):
        assert column in table.columns
    assert sorted(table["year"].unique()) == [2024, 2025]
    assert (table["impact"] == table["amount"] * table["CF"]).all()
    # `year` is the rounded year the CF was evaluated at, `date` the exact emission date;
    # dates from July 1st on round up, so the two only coincide for January dates like these.
    assert table["date"].dt.year.equals(table["year"])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_edges_lcia.py -k "constant_cf or emission_year or cf_table" -v`
Expected: FAIL with `AttributeError: 'TimexEdgeLCIA' object has no attribute 'run_mapping'`.

- [ ] **Step 3: Implement mapping and per-year characterisation**

Append to the `TimexEdgeLCIA` class in `bw_timex/edges_lcia.py`:

```python
    def run_mapping(self, regionalized: bool = True) -> None:
        """
        Matches the exchanges of the time-explicit inventory to the method's characterisation
        factors. Matching does not depend on the year, so it runs once for the whole timeline.

        Parameters
        ----------
        regionalized : bool
            If True, runs edges' location-mapping cascade after the direct matching, which fills
            aggregate, dynamic ("RoW"), contained and global regions. Default is True.
        """
        self.map_exchanges()

        if regionalized:
            self.map_aggregate_locations()
            self.map_dynamic_locations()
            self.map_contained_locations()
            self.map_remaining_locations_to_global()

    def characterize_time_explicit(self, use_disaggregated_lci: bool = False):
        """
        Characterises the time-explicit inventory, evaluating each exchange's characterisation
        factor at that exchange's own year.

        Biosphere exchanges are taken from the dynamic inventory, so emissions spread by a
        temporal distribution are characterised at the year they actually occur. Technosphere
        exchanges are characterised at the vintage of the consuming process, since bw_timex has no
        dynamic technosphere inventory.

        Parameters
        ----------
        use_disaggregated_lci : bool
            If True, uses the disaggregated dynamic inventory of the background. Default is False.

        Returns
        -------
        pandas.DataFrame
            One row per characterised exchange, with the date and year used for its CF.
        """
        slices = {}
        biosphere_entries = self._biosphere_entries(use_disaggregated_lci)
        if biosphere_entries is not None:
            slices["biosphere"] = biosphere_entries
        technosphere_entries = self._technosphere_entries()
        if technosphere_entries is not None:
            slices["technosphere"] = technosphere_entries

        years = sorted(
            {year for entries in slices.values() for year in entries["year"].unique()}
        )

        tables = []
        for year in years:
            self.evaluate_cfs(scenario_idx=str(year))
            for matrix_type, entries in slices.items():
                cf_matrix = self.characterization_matrices.get(matrix_type)
                if cf_matrix is None:
                    continue
                in_year = entries[entries["year"] == year]
                if in_year.empty:
                    continue
                tables.append(
                    self._characterize_entries(in_year, cf_matrix.tocsr(), matrix_type)
                )

        if not tables:
            logger.warning("No exchanges were characterized. The edges score is 0.")
            self.score = 0.0
            return pd.DataFrame(
                columns=[
                    "supplier",
                    "supplier categories",
                    "consumer",
                    "consumer location",
                    "activity",
                    "direction",
                    "date",
                    "year",
                    "amount",
                    "CF",
                    "impact",
                ]
            )

        table = pd.concat(tables, ignore_index=True).sort_values(
            by=["date", "impact"], ascending=[True, False]
        )
        table = table[table["CF"] != 0].reset_index(drop=True)
        self.score = float(table["impact"].sum())
        return table

    def _characterize_entries(self, entries, cf_matrix, matrix_type: str):
        """Looks up the CF of each entry in one year's characterisation matrix."""
        rows = entries["row"].to_numpy()
        columns = entries["column"].to_numpy()
        cfs = np.asarray(cf_matrix[rows, columns]).ravel()

        supplier_lookup = (
            self.position_to_biosphere_flows_lookup
            if matrix_type == "biosphere"
            else self.position_to_technosphere_flows_lookup
        )
        direction = (
            "biosphere-technosphere"
            if matrix_type == "biosphere"
            else "technosphere-technosphere"
        )

        return pd.DataFrame(
            {
                "supplier": [supplier_lookup[row].get("name") for row in rows],
                "supplier categories": [
                    supplier_lookup[row].get("categories") for row in rows
                ],
                "consumer": [
                    self.position_to_technosphere_flows_lookup[column].get("name")
                    for column in columns
                ],
                "consumer location": [
                    self.position_to_technosphere_flows_lookup[column].get("location")
                    for column in columns
                ],
                "activity": [self.reversed_activity[column] for column in columns],
                "direction": direction,
                "date": entries["date"].to_numpy(),
                "year": entries["year"].to_numpy(),
                "amount": entries["amount"].to_numpy(),
                "CF": cfs,
                "impact": entries["amount"].to_numpy() * cfs,
            }
        )

    def _biosphere_entries(self, use_disaggregated_lci: bool):
        """
        Returns the non-zero entries of the dynamic inventory as a DataFrame with the matrix
        positions and the year that edges should evaluate the CF at.

        The dynamic inventory is used rather than `lca.inventory`, because only it keeps the
        emission dates produced by temporal distributions on biosphere exchanges.
        """
        if not self._uses_biosphere_supplier_matrix():
            return None

        timex = self.timex_lca
        inventory = (
            timex.dynamic_inventory_disaggregated
            if use_disaggregated_lci
            else timex.dynamic_inventory
        )
        coo = inventory.tocoo()
        if coo.nnz == 0:
            return None

        reversed_time_mapping = timex.biosphere_time_mapping.reversed
        biosphere_positions = self.lca.dicts.biosphere

        unique_rows = np.unique(coo.row)
        resolved = {row: reversed_time_mapping[row] for row in unique_rows}
        row_to_position = {
            row: biosphere_positions[flow] for row, (flow, _) in resolved.items()
        }
        row_to_date = {row: date for row, (_, date) in resolved.items()}

        dates = pd.Series([row_to_date[row] for row in coo.row]).astype("datetime64[s]")

        return pd.DataFrame(
            {
                "row": [row_to_position[row] for row in coo.row],
                "column": coo.col,
                "amount": coo.data,
                "date": dates,
                "year": round_datetime_series_to_year(dates).dt.year,
            }
        )

    def _technosphere_entries(self):
        """
        Returns the non-zero entries of the technosphere flow matrix, dated by the vintage of the
        consuming process. There is no dynamic technosphere inventory, so this is the finest
        resolution available for technosphere characterisation factors.
        """
        if self.technosphere_flow_matrix is None:
            return None

        coo = self.technosphere_flow_matrix.tocoo()
        if coo.nnz == 0:
            return None

        temporal_grouping = self.timex_lca.temporal_grouping
        year_by_column = {
            column: year_from_time_mapped_timestamp(
                self.position_to_timestamp[column], temporal_grouping
            )
            for column in np.unique(coo.col)
        }

        resolved = np.array(
            [year_by_column[column] is not None for column in coo.col], dtype=bool
        )
        unresolved_count = int((~resolved).sum())
        if unresolved_count:
            logger.warning(
                f"{unresolved_count} technosphere exchanges have no resolved process time "
                f"(timestamp 'dynamic') and are not characterized."
            )
        if not resolved.any():
            return None

        years = np.array(
            [year_by_column[column] for column in coo.col[resolved]], dtype=int
        )
        dates = pd.to_datetime(pd.Series(years.astype(str)), format="%Y").astype(
            "datetime64[s]"
        )

        return pd.DataFrame(
            {
                "row": coo.row[resolved],
                "column": coo.col[resolved],
                "amount": coo.data[resolved],
                "date": dates.to_numpy(),
                "year": years,
            }
        )
```

Note on the year of a biosphere entry: `round_datetime_series_to_year` is used, matching what `dynamic_lcia()` already does — dates from July 1st onwards round up to the next year.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_edges_lcia.py -k "constant_cf or emission_year or cf_table" -v`
Expected: PASS (3 passed). If `test_cf_is_evaluated_at_the_emission_year_not_the_process_year` gives 10.0 instead of 16.0, the biosphere side is reading `lca.inventory` — go back to `_biosphere_entries`.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/edges_lcia.py tests/test_edges_lcia.py
git commit -m "feat: per-year characterisation of the dynamic inventory with edges"
```

---

### Task 6: Public `TimexLCA.edges_lcia()` and guard clauses

**Files:**
- Modify: `bw_timex/timex_lca.py` (add after `dynamic_lcia`, which ends at the `return self.characterized_inventory` around line 860; the `edges_score` property goes next to `dynamic_score`, around line 887)
- Test: `tests/test_edges_lcia.py`

**Interfaces:**
- Consumes: `TimexEdgeLCIA` (Tasks 4-5), `EdgesLCIAInputs` (Task 1).
- Produces: `TimexLCA.edges_lcia(method, parameters=None, scenario=None, weight="population", filepath=None, allowed_functions=None, regionalized=True, use_disaggregated_lci=False) -> pandas.DataFrame`; attributes `edges_characterized_inventory` (DataFrame), `edges_lcia_object` (`TimexEdgeLCIA`); property `edges_score` (float).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_edges_lcia.py`:

```python
def test_edges_lcia_end_to_end(timex_lca_with_lci):
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    table = timex_lca_with_lci.edges_lcia(
        method=("test", "year_dependent_cf"),
        filepath=str(method_path("year_dependent_cf")),
        parameters=parameters,
        scenario="test",
        regionalized=False,
    )

    assert timex_lca_with_lci.edges_score == pytest.approx(table["impact"].sum())
    assert timex_lca_with_lci.edges_characterized_inventory is table
    assert timex_lca_with_lci.edges_lcia_object.score == pytest.approx(
        timex_lca_with_lci.edges_score
    )


def test_edges_score_before_calculation_raises(timex_lca_with_lci):
    with pytest.raises(AttributeError, match="edges_lcia"):
        timex_lca_with_lci.edges_score


def test_edges_lcia_without_lci_raises(edges_td_db):
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))

    with pytest.raises(AttributeError, match="TimexLCA.lci"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )


def test_edges_lcia_requires_the_expanded_matrix(edges_td_db):
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci(expand_technosphere=False)

    with pytest.raises(NotImplementedError, match="expand_technosphere"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )


def test_edges_lcia_requires_the_dynamic_biosphere(edges_td_db):
    """Without the dynamic inventory, biosphere temporal distributions would be lost silently."""
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci(build_dynamic_biosphere=False)

    with pytest.raises(ValueError, match="build_dynamic_biosphere"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_edges_lcia.py -k "end_to_end or edges_score or without_lci or expanded_matrix or dynamic_biosphere" -v`
Expected: FAIL with `AttributeError: 'TimexLCA' object has no attribute 'edges_lcia'`.

- [ ] **Step 3: Implement the public method**

In `bw_timex/timex_lca.py`, add `EdgesLCIAInputs` to the existing import from `.validation`, then add this method directly after `dynamic_lcia`:

```python
    def edges_lcia(
        self,
        method,
        parameters: dict = None,
        scenario: str = None,
        weight: str = "population",
        filepath: str = None,
        allowed_functions: dict = None,
        regionalized: bool = True,
        use_disaggregated_lci: bool = False,
    ) -> pd.DataFrame:
        """
        Calculates LCIA with the `edges` package, evaluating each exchange's characterization
        factor at that exchange's own year.

        `edges` (https://edges.readthedocs.io) characterizes exchanges instead of flows, which
        allows regionalized characterization factors, characterization factors for technosphere
        exchanges, and characterization factors given as symbolic expressions that depend on the
        scenario year. Combined with the time-explicit inventory of bw_timex, each exchange is
        characterized with the characterization factor of the year in which it occurs.

        Biosphere exchanges are characterized at the date of the emission, taken from the dynamic
        inventory, so emissions spread out by a temporal distribution are characterized correctly.
        Technosphere exchanges are characterized at the vintage of the consuming process, as
        bw_timex does not build a dynamic technosphere inventory.

        This is orthogonal to `TimexLCA.dynamic_lcia()`: `edges` varies the characterization
        factor with the year of the exchange, while the dynamic characterization varies the impact
        with the time that has passed since the emission.

        Parameters
        ----------
        method : tuple, str, pathlib.Path or dict
            The edges method: a method tuple, a path to a method JSON file, or a dict with an
            "exchanges" key. This is not the same object as `TimexLCA.method`, which is a
            Brightway LCIA method.
        parameters : dict, optional
            Scenario parameters for symbolic characterization factors, in the form
            {scenario: {parameter: {year: value}}}. Default is None.
        scenario : str, optional
            Name of the scenario in `parameters` to evaluate. Default is None.
        weight : str, optional
            Weighting scheme edges uses when aggregating regional characterization factors.
            Default is "population".
        filepath : str, optional
            Path to a custom method JSON file. Default is None.
        allowed_functions : dict, optional
            Trusted functions that symbolic characterization factors may call. Default is None.
        regionalized : bool, optional
            Whether to run the location-mapping cascade of edges, which fills aggregate, dynamic,
            contained and global regions. Default is True.
        use_disaggregated_lci : bool, optional
            Whether to use the disaggregated background inventory. Default is False.

        Returns
        -------
        pandas.DataFrame
            One row per characterized exchange, including the date and year at which its
            characterization factor was evaluated.

        See also
        --------
        edges: Package handling the exchange-based characterization: https://edges.readthedocs.io
        """
        EdgesLCIAInputs(
            method=method,
            parameters=parameters,
            scenario=scenario,
            weight=weight,
            filepath=filepath,
            allowed_functions=allowed_functions,
            regionalized=regionalized,
            use_disaggregated_lci=use_disaggregated_lci,
        )

        if not hasattr(self, "lca"):
            raise AttributeError("LCI not yet calculated. Call TimexLCA.lci() first.")

        if not self.expanded_technosphere:
            raise NotImplementedError(
                "edges_lcia currently requires the expanded time-explicit matrices. Please call "
                "TimexLCA.lci(expand_technosphere=True) first."
            )

        if not hasattr(self, "dynamic_inventory"):
            raise ValueError(
                "edges_lcia characterizes biosphere flows at the date of the emission, which is "
                "only available in the dynamic inventory. Without it, temporal distributions on "
                "biosphere exchanges would be ignored. Please call "
                "TimexLCA.lci(build_dynamic_biosphere=True) first."
            )

        if use_disaggregated_lci and not hasattr(self, "dynamic_inventory_disaggregated"):
            logger.info("Disaggregating background LCI...")
            self.disaggregate_background_lci()
            logger.info("Background LCI's disaggregated.")

        from .edges_lcia import TimexEdgeLCIA

        edge_kwargs = {
            "parameters": parameters,
            "scenario": scenario,
            "weight": weight,
            "filepath": filepath,
            "allowed_functions": allowed_functions,
        }
        self.edges_lcia_object = TimexEdgeLCIA(self, method=method, **edge_kwargs)
        self.edges_lcia_object.lci()
        self.edges_lcia_object.run_mapping(regionalized=regionalized)
        self.edges_characterized_inventory = (
            self.edges_lcia_object.characterize_time_explicit(
                use_disaggregated_lci=use_disaggregated_lci
            )
        )

        return self.edges_characterized_inventory
```

- [ ] **Step 4: Add the score property**

In `bw_timex/timex_lca.py`, directly after the `dynamic_score` property:

```python
    @property
    def edges_score(self) -> float:
        """
        Score resulting from the characterization of the time-explicit inventory with `edges`.
        """
        if not hasattr(self, "edges_characterized_inventory"):
            raise AttributeError(
                "edges-characterized inventory not yet calculated. Call TimexLCA.edges_lcia() "
                "first."
            )
        return float(self.edges_characterized_inventory["impact"].sum())
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_edges_lcia.py -v`
Expected: PASS (all tests in the file).

- [ ] **Step 6: Run the full suite for regressions**

Run: `pytest -x -q`
Expected: PASS. The core suite must not have changed behaviour; `edges` stays an optional import.

- [ ] **Step 7: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_edges_lcia.py
git commit -m "feat: TimexLCA.edges_lcia for time-resolved characterization"
```

---

### Task 7: Technosphere characterization factors

Proves the technosphere side end to end, and pins the documented limitation that technosphere exchanges resolve no finer than the consuming process's vintage.

**Files:**
- Create: `tests/fixtures/edges_methods/technosphere_cf.json`
- Test: `tests/test_edges_lcia.py`
- Modify: `bw_timex/edges_lcia.py` (only if a defect shows up)

**Interfaces:**
- Consumes: everything from Tasks 4-6.
- Produces: no new API. Confirms `direction == "technosphere-technosphere"` rows in the CF table.

- [ ] **Step 1: Write the method file**

Create `tests/fixtures/edges_methods/technosphere_cf.json`:

```json
{
  "name": "test technosphere CF",
  "version": "1.0",
  "unit": "risk points",
  "description": "CF on the electricity exchange, to cover technosphere characterization.",
  "exchanges": [
    {
      "supplier": {
        "name": "electricity production",
        "reference product": "electricity",
        "location": "CH",
        "matrix": "technosphere"
      },
      "consumer": {
        "location": "CH",
        "matrix": "technosphere"
      },
      "value": 1.0,
      "value_expression": "cf_electricity"
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

Append to `tests/test_edges_lcia.py`:

```python
def test_technosphere_cfs_are_characterized_at_the_process_vintage(timex_lca_with_lci):
    """
    The heat process consumes 3 kWh of electricity and runs in 2024, so with a CF of 5 per kWh in
    2024 the technosphere impact is 15. Technosphere exchanges carry no emission dates, so the
    vintage of the consuming process is the year used.
    """
    parameters = {"test": {"cf_electricity": {"2024": 5.0, "2025": 50.0}}}

    table = timex_lca_with_lci.edges_lcia(
        method=("test", "technosphere_cf"),
        filepath=str(method_path("technosphere_cf")),
        parameters=parameters,
        scenario="test",
        regionalized=False,
    )

    assert set(table["direction"]) == {"technosphere-technosphere"}
    assert table["impact"].sum() == pytest.approx(15.0)
    assert sorted(table["year"].unique()) == [2024]
```

- [ ] **Step 3: Run the test**

Run: `pytest tests/test_edges_lcia.py -k technosphere_cfs -v`
Expected: PASS if Tasks 4-6 were implemented correctly. If it fails with an empty table, check `_uses_technosphere_supplier_matrix()` gating in `lci()`; if the year is wrong, check `position_to_timestamp` and `year_from_time_mapped_timestamp`.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/edges_methods/technosphere_cf.json tests/test_edges_lcia.py
git commit -m "test: technosphere characterization factors at the process vintage"
```

---

### Task 8: API-drift guard, changelog and example notebook

**Files:**
- Test: `tests/test_edges_lcia.py`
- Modify: `CHANGES.md`
- Create: `docs/content/examples/edges_time_resolved_characterization.ipynb`

**Interfaces:**
- Consumes: everything above.
- Produces: no new API.

- [ ] **Step 1: Write the drift test**

Append to `tests/test_edges_lcia.py`:

```python
def test_edges_public_surface_is_unchanged():
    """
    The adapter depends on a narrow slice of edges. If a future edges release moves any of these,
    this test should fail loudly rather than the integration failing subtly.
    """
    import inspect

    from edges import EdgeLCIA

    for name in (
        "lci",
        "map_exchanges",
        "map_aggregate_locations",
        "map_dynamic_locations",
        "map_contained_locations",
        "map_remaining_locations_to_global",
        "evaluate_cfs",
        "_uses_biosphere_supplier_matrix",
        "_uses_technosphere_supplier_matrix",
    ):
        assert hasattr(EdgeLCIA, name), f"edges.EdgeLCIA lost {name}"

    signature = inspect.signature(EdgeLCIA.__init__)
    for parameter in ("demand", "method", "parameters", "scenario", "filepath", "lca", "weight"):
        assert parameter in signature.parameters, f"EdgeLCIA.__init__ lost {parameter}"

    assert "scenario_idx" in inspect.signature(EdgeLCIA.evaluate_cfs).parameters

    from edges.matrix_builders import build_technosphere_edges_matrix  # noqa: F401
    from edges.utils import get_flow_matrix_positions  # noqa: F401
```

- [ ] **Step 2: Run it**

Run: `pytest tests/test_edges_lcia.py -k public_surface -v`
Expected: PASS.

- [ ] **Step 3: Add the changelog entry**

At the top of `CHANGES.md`, under a new unreleased section, add:

```markdown
### Added

- `TimexLCA.edges_lcia()`: characterization with the [`edges`](https://edges.readthedocs.io)
  package, evaluating each exchange's characterization factor at its own year. Biosphere flows
  are characterized at the date of the emission taken from the dynamic inventory, so temporal
  distributions on biosphere exchanges are respected; technosphere flows are characterized at the
  vintage of the consuming process. Install with `pip install bw_timex[edges]` (Python < 3.13).
```

- [ ] **Step 4: Write the example notebook**

Create `docs/content/examples/edges_time_resolved_characterization.ipynb` with these cells, following the style of the existing example notebooks:

1. Markdown: what the example shows — an `edges` method whose CF changes with the year, applied to a product system whose emissions are spread over time; and the difference to `dynamic_lcia()`.
2. Code: build a `TimexLCA` on one of the existing example databases, `build_timeline()`, `lci()`.
3. Code: define `parameters` with two years and call `edges_lcia()` with a small inline method dict.
4. Code: show the returned table grouped by year, `df.groupby("year")["impact"].sum()`, and compare `edges_score` to `static_score`.
5. Markdown: the limitation — technosphere CFs use the consuming process's vintage, since bw_timex has no dynamic technosphere inventory.

Verify it runs top to bottom: `jupyter nbconvert --to notebook --execute docs/content/examples/edges_time_resolved_characterization.ipynb --stdout > /dev/null`

- [ ] **Step 5: Run the whole suite one last time**

Run: `pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_edges_lcia.py CHANGES.md docs/content/examples/edges_time_resolved_characterization.ipynb
git commit -m "docs: changelog and example notebook for the edges integration"
```
