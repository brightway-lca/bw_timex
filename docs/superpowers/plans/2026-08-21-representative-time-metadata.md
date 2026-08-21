# Representative time as database metadata — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `TimexLCA` learns the point in time each background database represents from that database's own Brightway metadata (`representative_time`, as written by premise), so `database_dates` becomes an optional explicit override instead of a required argument.

**Architecture:** A new module `bw_timex/database_metadata.py` owns everything about database metadata: writing it (`set_database_metadata`) and resolving a `{database: datetime | "dynamic"}` mapping out of the project (`resolve_database_dates_from_metadata`), including scenario filtering and the ambiguity error. `TimexLCA.__init__` calls it in one place and is otherwise untouched: everything downstream still consumes `self.database_dates`.

**Tech Stack:** Python 3.10+, `bw2data` (database metadata lives in `bd.databases[name]`, a JSON-serialized dict), `pydantic` (input validation, see `bw_timex/validation.py`), `loguru`, `pytest` with `bw2data.tests.bw2test` fixtures.

**Spec:** `docs/superpowers/specs/2026-08-21-representative-time-metadata-design.md`

## Global Constraints

- Run everything with the project venv: `.venv/bin/python`, `.venv/bin/pytest`.
- `database_dates` semantics do not change. When it is passed, it is the whole mapping and metadata is never read.
- `bd.databases` is serialized to JSON. A `datetime` written into it breaks `bd.databases.flush()`. Every date stored in metadata is an ISO 8601 string.
- Metadata keys, exactly as premise writes them: `representative_time`, `iam_model`, `pathway`, `system_model`, `ecoinvent_version`, `premise_version`, `external_scenarios`, `scenarios`.
- Scenario identity keys (the ambiguity signature) are exactly: `("iam_model", "pathway", "system_model", "ecoinvent_version", "external_scenarios")`. `premise_version` is deliberately not one of them.
- `notebooks/examples/paper_case_study.ipynb` must not be modified by any task.
- Commit messages carry no AI attribution and no `Co-Authored-By` trailer.

---

### Task 1: `database_metadata` module — writing metadata

**Files:**
- Create: `bw_timex/database_metadata.py`
- Modify: `bw_timex/validation.py` (append a `DatabaseMetadataInputs` model)
- Modify: `bw_timex/__init__.py` (export `set_database_metadata`)
- Create: `tests/test_database_metadata.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `bw_timex.database_metadata.set_database_metadata(database: str | bd.Database, **metadata) -> dict`
  - constants `REPRESENTATIVE_TIME: str = "representative_time"`, `SCENARIOS: str = "scenarios"`, `DYNAMIC: str = "dynamic"`, `SCENARIO_SIGNATURE_KEYS: tuple[str, ...]`, `BRIGHTWAY_METADATA_KEYS: frozenset[str]`
  - `bw_timex.database_metadata._normalize_representative_time(value, database: str) -> datetime | str`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_database_metadata.py`:

```python
"""Tests for reading and writing what a Brightway database represents."""

from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import set_database_metadata

# ─── Tests for set_database_metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSetDatabaseMetadata:

    def test_datetime_is_stored_as_iso_string(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_iso_string_is_stored_as_given(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01"

    def test_dynamic_is_allowed(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert bd.databases["foreground"]["representative_time"] == "dynamic"

    def test_scenario_fields_are_stored(self):
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            iam_model="remind",
            pathway="SSP2-PkBudg500",
        )
        assert bd.databases["db_2022"]["iam_model"] == "remind"
        assert bd.databases["db_2022"]["pathway"] == "SSP2-PkBudg500"

    def test_database_object_is_accepted(self):
        set_database_metadata(
            bd.Database("db_2022"), representative_time=datetime(2022, 1, 1)
        )
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_existing_metadata_is_kept(self):
        before = bd.databases["db_2022"]["backend"]
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["backend"] == before

    def test_survives_flush_and_reload(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        bd.databases.__init__()  # re-read from disk
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_unregistered_database_raises(self):
        with pytest.raises(ValueError, match="not registered"):
            set_database_metadata("no_such_db", representative_time=datetime(2022, 1, 1))

    def test_unparseable_representative_time_raises(self):
        with pytest.raises(ValueError, match="representative_time"):
            set_database_metadata("db_2022", representative_time="whenever")

    def test_non_serializable_value_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            set_database_metadata("db_2022", pathway=object())

    def test_no_metadata_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            set_database_metadata("db_2022")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_database_metadata.py -v`
Expected: FAIL — `ImportError: cannot import name 'set_database_metadata' from 'bw_timex'`

- [ ] **Step 3: Write the module**

Create `bw_timex/database_metadata.py`:

```python
"""Read and write what a Brightway database represents.

`bw_timex` needs to know which point in time each background database stands
for. That information is stored in the database's own Brightway metadata
(`bw2data.databases[name]`), where premise also writes it when it exports a
prospective database:

```python
{
    "premise_version": "2.4.9.1",
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "representative_time": "2050-01-01T00:00:00",
    "ecoinvent_version": "3.10.1",
    "system_model": "cutoff",
}
```

Brightway stores this mapping as JSON, so dates are kept as ISO 8601 strings.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import bw2data as bd

REPRESENTATIVE_TIME = "representative_time"
SCENARIOS = "scenarios"
DYNAMIC = "dynamic"

#: Metadata keys that identify the scenario a database represents. Two
#: databases differing in any of these represent different scenarios.
#: `premise_version` is deliberately absent: re-running premise on the same
#: pathway must not look like a second scenario.
SCENARIO_SIGNATURE_KEYS = (
    "iam_model",
    "pathway",
    "system_model",
    "ecoinvent_version",
    "external_scenarios",
)

#: Keys Brightway maintains itself, filtered out when reporting to the user
#: which metadata a project's databases carry.
BRIGHTWAY_METADATA_KEYS = frozenset(
    {
        "backend",
        "depends",
        "dirty",
        "format",
        "geocollections",
        "modified",
        "number",
        "processed",
        "searchable",
    }
)


def _database_name(database: Any) -> str:
    """The name of a database given either as a name or as a `bd.Database`."""
    name = getattr(database, "name", database)
    if not isinstance(name, str):
        raise ValueError(
            f"database must be a database name or a bw2data Database, got "
            f"{type(database).__name__}."
        )
    return name


def _normalize_representative_time(value: Any, database: str) -> datetime | str:
    """Turn a stored `representative_time` into a datetime or `"dynamic"`."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        if value == DYNAMIC:
            return DYNAMIC
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(
                f"Database '{database}' has an invalid `{REPRESENTATIVE_TIME}` "
                f"metadata value: {value!r}. Expected an ISO 8601 datetime string "
                f"(e.g. '2030-01-01'), a datetime, or '{DYNAMIC}'."
            ) from None
    raise ValueError(
        f"Database '{database}' has an invalid `{REPRESENTATIVE_TIME}` metadata "
        f"value of type {type(value).__name__}: {value!r}. Expected an ISO 8601 "
        f"datetime string, a datetime, or '{DYNAMIC}'."
    )


def set_database_metadata(database: str | bd.Database, **metadata) -> dict:
    """
    Store what a database represents in its Brightway metadata.

    Use this for databases that don't bring the metadata themselves, e.g.
    databases you built yourself or that were exported by a premise version
    older than the one writing scenario metadata. `TimexLCA` reads
    `representative_time` from all databases of the project to map them to
    points in time, so this replaces passing `database_dates`.

    Parameters
    ----------
    database : str or bw2data.Database
        Name of the database, or the database itself. Must be registered.
    **metadata :
        Metadata to store. `representative_time` accepts a `datetime`, an ISO
        8601 string, or `"dynamic"` and is always stored as a string, because
        Brightway serializes database metadata to JSON. Any other key is stored
        as given and must be JSON-serializable. Keys that premise writes, and
        that `TimexLCA(scenario=...)` can select on, are `iam_model`,
        `pathway`, `system_model`, `ecoinvent_version` and `premise_version`.

    Returns
    -------
    dict
        The database's metadata after the update.

    Examples
    --------
    ```python
    set_database_metadata("db_2030", representative_time=datetime(2030, 1, 1))
    set_database_metadata(
        "my_2050_variant",
        representative_time="2050-01-01",
        iam_model="remind",
        pathway="SSP2-PkBudg500",
    )
    ```
    """
    from .validation import DatabaseMetadataInputs

    name = _database_name(database)
    DatabaseMetadataInputs(database=name, metadata=metadata)

    if name not in bd.databases:
        raise ValueError(
            f"Database '{name}' is not registered in this Brightway project. "
            f"Available databases: {sorted(bd.databases)}."
        )

    serialized = {}
    for key, value in metadata.items():
        if key == REPRESENTATIVE_TIME:
            normalized = _normalize_representative_time(value, name)
            serialized[key] = (
                normalized if normalized == DYNAMIC else normalized.isoformat()
            )
            continue
        try:
            json.dumps(value)
        except TypeError:
            raise ValueError(
                f"Metadata value for '{key}' is not JSON-serializable: {value!r}. "
                f"Brightway stores database metadata as JSON."
            ) from None
        serialized[key] = value

    bd.databases[name].update(serialized)
    bd.databases.flush()
    return bd.databases[name]
```

- [ ] **Step 4: Add the validation model**

Append to `bw_timex/validation.py`:

```python
class DatabaseMetadataInputs(BaseModel):
    """Validates inputs to set_database_metadata"""

    model_config = {"arbitrary_types_allowed": True}

    database: str
    metadata: dict

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, v: dict) -> dict:
        if not v:
            raise ValueError(
                "Provide at least one metadata field, e.g. "
                "`representative_time=datetime(2030, 1, 1)`."
            )
        for key in v:
            if not isinstance(key, str):
                raise ValueError(
                    f"Metadata keys must be strings, got {type(key).__name__}: {key}."
                )
        return v
```

- [ ] **Step 5: Export it**

In `bw_timex/__init__.py`, add the import next to the other helper imports and the name to `__all__` (in the `# utils` block, alphabetically after `plot_characterized_inventory_as_waterfall`):

```python
from .database_metadata import set_database_metadata
```

```python
    "set_database_metadata",
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_database_metadata.py -v`
Expected: PASS (12 tests)

- [ ] **Step 7: Commit**

```bash
git add bw_timex/database_metadata.py bw_timex/validation.py bw_timex/__init__.py tests/test_database_metadata.py
git commit -m "feat: add set_database_metadata to store what a database represents"
```

---

### Task 2: Resolve database dates from metadata

**Files:**
- Modify: `bw_timex/database_metadata.py`
- Modify: `tests/test_database_metadata.py`

**Interfaces:**
- Consumes: `REPRESENTATIVE_TIME`, `SCENARIOS`, `DYNAMIC`, `_normalize_representative_time`, `set_database_metadata` from Task 1.
- Produces: `resolve_database_dates_from_metadata(scenario: dict | None = None) -> dict[str, datetime | str]` — every registered database carrying `representative_time`, mapped to a `datetime` or `"dynamic"`. Multi-scenario databases are excluded. Scenario filtering and the ambiguity error come in Task 3; this task's version accepts the argument and ignores it.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_database_metadata.py`:

```python
from bw_timex.database_metadata import resolve_database_dates_from_metadata

# ─── Tests for resolving database dates from metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestResolveFromMetadata:

    def test_empty_project_metadata_resolves_to_nothing(self):
        assert resolve_database_dates_from_metadata() == {}

    def test_iso_strings_resolve_to_datetimes(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        assert resolve_database_dates_from_metadata() == {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
        }

    def test_dynamic_metadata_resolves_to_dynamic(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("foreground", representative_time="dynamic")
        resolved = resolve_database_dates_from_metadata()
        assert resolved["foreground"] == "dynamic"
        assert resolved["db_2022"] == datetime(2022, 1, 1)

    def test_databases_without_metadata_are_ignored(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        assert set(resolve_database_dates_from_metadata()) == {"db_2022"}

    def test_multi_scenario_database_is_skipped(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            scenarios=[
                {"pathway": "SSP2-Base", "representative_time": "2024-01-01"},
                {"pathway": "SSP2-PkBudg500", "representative_time": "2024-01-01"},
            ],
        )
        assert set(resolve_database_dates_from_metadata()) == {"db_2022"}

    def test_invalid_metadata_value_raises_naming_the_database(self):
        bd.databases["db_2022"]["representative_time"] = "whenever"
        bd.databases.flush()
        with pytest.raises(ValueError, match="db_2022"):
            resolve_database_dates_from_metadata()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_database_metadata.py::TestResolveFromMetadata -v`
Expected: FAIL — `ImportError: cannot import name 'resolve_database_dates_from_metadata'`

- [ ] **Step 3: Implement discovery**

Append to `bw_timex/database_metadata.py` (and add `from loguru import logger` to the imports):

```python
def _candidate_databases() -> dict[str, dict]:
    """Registered databases that declare a `representative_time`.

    Multi-scenario databases (superstructure and scenario-array exports, which
    carry a `scenarios` list) are skipped: `bw_timex` needs one technosphere
    per point in time and cannot pick a scenario out of such a database. They
    can still be used by naming them in `database_dates`.
    """
    candidates = {}
    for name in bd.databases:
        metadata = bd.databases[name]
        if REPRESENTATIVE_TIME not in metadata:
            continue
        if metadata.get(SCENARIOS):
            logger.info(
                f"Skipping database '{name}': it holds "
                f"{len(metadata[SCENARIOS])} scenarios, so the point in time it "
                f"represents is ambiguous. Map it explicitly with `database_dates` "
                f"if you want to use it anyway."
            )
            continue
        candidates[name] = metadata
    return candidates


def resolve_database_dates_from_metadata(
    scenario: dict | None = None,
) -> dict[str, datetime | str]:
    """
    Map the databases of the current project to the points in time they represent.

    Reads the `representative_time` metadata of every registered database (see
    [`set_database_metadata`][bw_timex.database_metadata.set_database_metadata]).

    Parameters
    ----------
    scenario : dict, optional
        Metadata a database must match to be included, e.g.
        `{"iam_model": "remind", "pathway": "SSP2-PkBudg500"}`. Databases that
        don't declare a filtered key at all are kept.

    Returns
    -------
    dict
        Mapping of database name to `datetime` or `"dynamic"`, ready to be used
        as `TimexLCA.database_dates`.
    """
    candidates = _candidate_databases()
    return {
        name: _normalize_representative_time(metadata[REPRESENTATIVE_TIME], name)
        for name, metadata in candidates.items()
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_database_metadata.py -v`
Expected: PASS (18 tests)

- [ ] **Step 5: Commit**

```bash
git add bw_timex/database_metadata.py tests/test_database_metadata.py
git commit -m "feat: resolve database dates from representative_time metadata"
```

---

### Task 3: Scenario filtering and the ambiguity error

**Files:**
- Modify: `bw_timex/database_metadata.py`
- Modify: `tests/test_database_metadata.py`

**Interfaces:**
- Consumes: `resolve_database_dates_from_metadata`, `_candidate_databases`, `SCENARIO_SIGNATURE_KEYS`, `BRIGHTWAY_METADATA_KEYS` from Tasks 1–2.
- Produces: `resolve_database_dates_from_metadata(scenario)` now filters, and raises `ValueError` on an unknown filter key or on several scenario sets. No new public names.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_database_metadata.py`:

```python
# ─── Tests for scenario selection ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestScenarioSelection:

    @pytest.fixture(autouse=True)
    def two_scenarios(self):
        """db_2022 and db_2024 hold the same year in two different pathways."""
        set_database_metadata(
            "db_2022",
            representative_time="2022-01-01",
            iam_model="remind",
            pathway="SSP2-PkBudg500",
            premise_version="2.4.9.1",
        )
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            iam_model="remind",
            pathway="SSP2-Base",
            premise_version="2.4.9.1",
        )

    def test_two_scenario_sets_without_selection_raises(self):
        with pytest.raises(ValueError, match="Several background scenarios"):
            resolve_database_dates_from_metadata()

    def test_error_names_the_differing_key_and_values(self):
        with pytest.raises(ValueError) as excinfo:
            resolve_database_dates_from_metadata()
        message = str(excinfo.value)
        assert "pathway" in message
        assert "SSP2-PkBudg500" in message
        assert "SSP2-Base" in message
        # iam_model is identical in both sets, so it isn't part of the report
        assert "iam_model" not in message

    def test_scenario_selects_one_set(self):
        resolved = resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-Base"}
        )
        assert resolved == {"db_2024": datetime(2024, 1, 1)}

    def test_databases_without_scenario_metadata_survive_the_filter(self):
        set_database_metadata("foreground", representative_time="dynamic")
        resolved = resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-Base"}
        )
        assert resolved == {
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_several_filter_keys_are_combined(self):
        resolved = resolve_database_dates_from_metadata(
            scenario={"iam_model": "remind", "pathway": "SSP2-Base"}
        )
        assert set(resolved) == {"db_2024"}

    def test_filter_matching_nothing_resolves_to_nothing(self):
        assert resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-PkBudg1150"}
        ) == {}

    def test_unknown_filter_key_raises_listing_available_keys(self):
        with pytest.raises(ValueError) as excinfo:
            resolve_database_dates_from_metadata(scenario={"pathwya": "SSP2-Base"})
        message = str(excinfo.value)
        assert "pathwya" in message
        assert "pathway" in message

    def test_same_scenario_from_two_premise_versions_is_not_ambiguous(self):
        set_database_metadata("db_2024", pathway="SSP2-PkBudg500")
        set_database_metadata("db_2024", premise_version="2.4.9.2")
        assert set(resolve_database_dates_from_metadata()) == {"db_2022", "db_2024"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_database_metadata.py::TestScenarioSelection -v`
Expected: FAIL — no error is raised, `resolve_database_dates_from_metadata` currently ignores `scenario`

- [ ] **Step 3: Implement filtering and the ambiguity check**

In `bw_timex/database_metadata.py`, add `from collections import defaultdict` to the imports and insert before `resolve_database_dates_from_metadata`:

```python
def _as_set(value: Any) -> set:
    """Compare list-valued metadata (e.g. `external_scenarios`) order-insensitively."""
    if isinstance(value, (list, tuple, set)):
        return {str(item) for item in value}
    return {str(value)}


def _values_match(declared: Any, wanted: Any) -> bool:
    if isinstance(declared, (list, tuple, set)) or isinstance(wanted, (list, tuple, set)):
        return _as_set(declared) == _as_set(wanted)
    return str(declared) == str(wanted)


def _check_filter_keys(scenario: dict, candidates: dict[str, dict]) -> None:
    """Reject filter keys no database declares, instead of silently matching nothing."""
    declared = set()
    for metadata in candidates.values():
        declared.update(set(metadata) - BRIGHTWAY_METADATA_KEYS)
    unknown = sorted(set(scenario) - declared)
    if not unknown:
        return
    available = ", ".join(sorted(declared)) or "none"
    raise ValueError(
        f"No database in this project declares the metadata key(s) "
        f"{unknown}. Keys declared by the databases of this project: {available}. "
        f"Add the metadata with `bw_timex.set_database_metadata`, or check the "
        f"spelling of your `scenario` filter."
    )


def _scenario_signature(metadata: dict) -> tuple:
    return tuple(
        (key, tuple(sorted(_as_set(metadata[key]))) if key in metadata else None)
        for key in SCENARIO_SIGNATURE_KEYS
    )


def _format_scenario_sets(groups: dict[tuple, list[str]]) -> str:
    """One line per scenario set, naming only the keys that actually differ."""
    differing = [
        key
        for index, key in enumerate(SCENARIO_SIGNATURE_KEYS)
        if len({signature[index][1] for signature in groups}) > 1
    ]
    lines = []
    for signature, names in groups.items():
        values = dict(signature)
        description = ", ".join(
            f"{key}={', '.join(values[key]) if values[key] else 'not set'}"
            for key in differing
        )
        lines.append(f"  {description}: {', '.join(sorted(names))}")
    return "\n".join(lines)


def _check_unambiguous(candidates: dict[str, dict]) -> None:
    groups = defaultdict(list)
    for name, metadata in candidates.items():
        if any(key in metadata for key in SCENARIO_SIGNATURE_KEYS):
            groups[_scenario_signature(metadata)].append(name)
    if len(groups) <= 1:
        return
    raise ValueError(
        f"Several background scenarios found in this project:\n"
        f"{_format_scenario_sets(groups)}\n"
        f"Select one, e.g. scenario={{'pathway': '...'}}, or map the databases "
        f"explicitly with `database_dates`."
    )
```

Then replace the body of `resolve_database_dates_from_metadata` with:

```python
    candidates = _candidate_databases()
    if scenario:
        _check_filter_keys(scenario, candidates)
        candidates = {
            name: metadata
            for name, metadata in candidates.items()
            if all(
                key not in metadata or _values_match(metadata[key], wanted)
                for key, wanted in scenario.items()
            )
        }
    _check_unambiguous(candidates)
    return {
        name: _normalize_representative_time(metadata[REPRESENTATIVE_TIME], name)
        for name, metadata in candidates.items()
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_database_metadata.py -v`
Expected: PASS (26 tests)

- [ ] **Step 5: Commit**

```bash
git add bw_timex/database_metadata.py tests/test_database_metadata.py
git commit -m "feat: select background scenarios by database metadata"
```

---

### Task 4: Wire it into `TimexLCA`

**Files:**
- Modify: `bw_timex/timex_lca.py` (imports, class docstring `Examples` block, `__init__` signature + docstring, the `database_dates` fallback block at `timex_lca.py:146-160`)
- Modify: `bw_timex/validation.py` (`TimexLCAInputs`)
- Modify: `tests/test_database_metadata.py`

**Interfaces:**
- Consumes: `resolve_database_dates_from_metadata(scenario)` from Task 3.
- Produces: `TimexLCA(demand, method, database_dates=None, scenario=None, use_global_lci_cache=True)`; `TimexLCA.scenario` holds the filter that was used; `TimexLCA.database_dates` is the resolved mapping, exactly as before for callers who pass `database_dates`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_database_metadata.py`:

```python
from bw_timex import TimexLCA

# ─── Tests for TimexLCA using database metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAFromMetadata:

    @pytest.fixture
    def fu(self):
        return bd.get_node(database="foreground", code="A")

    def test_no_arguments_uses_metadata(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates == {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_demand_database_metadata_is_respected(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("foreground", representative_time="dynamic")
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates["foreground"] == "dynamic"

    def test_scenario_is_forwarded(self, fu):
        set_database_metadata(
            "db_2022", representative_time="2022-01-01", pathway="SSP2-Base"
        )
        set_database_metadata(
            "db_2024", representative_time="2024-01-01", pathway="SSP2-PkBudg500"
        )
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            scenario={"pathway": "SSP2-Base"},
        )
        assert tlca.database_dates == {
            "db_2022": datetime(2022, 1, 1),
            "foreground": "dynamic",
        }

    def test_database_dates_is_exclusive(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates={
                "db_2024": datetime(2024, 1, 1),
                "foreground": "dynamic",
            },
        )
        assert tlca.database_dates == {
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_database_dates_with_scenario_raises(self, fu):
        with pytest.raises(ValueError, match="only applies when"):
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                database_dates={"foreground": "dynamic"},
                scenario={"pathway": "SSP2-Base"},
            )

    def test_no_metadata_anywhere_falls_back_to_dynamic_demand(self, fu):
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates == {"foreground": "dynamic"}

    def test_metadata_and_database_dates_give_the_same_score(self, fu):
        explicit = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates={
                "db_2022": datetime(2022, 1, 1),
                "db_2024": datetime(2024, 1, 1),
                "foreground": "dynamic",
            },
        )
        explicit.build_timeline(starting_datetime=datetime(2024, 1, 2))
        explicit.lci()
        explicit.static_lcia()

        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        from_metadata = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        from_metadata.build_timeline(starting_datetime=datetime(2024, 1, 2))
        from_metadata.lci()
        from_metadata.static_lcia()

        assert from_metadata.static_score == pytest.approx(explicit.static_score)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_database_metadata.py::TestTimexLCAFromMetadata -v`
Expected: FAIL — `TypeError: TimexLCA.__init__() got an unexpected keyword argument 'scenario'`, and `test_no_arguments_uses_metadata` fails because only the demand database is mapped

- [ ] **Step 3: Change the signature and resolution**

In `bw_timex/timex_lca.py`, add to the imports:

```python
from .database_metadata import resolve_database_dates_from_metadata
```

Change the signature:

```python
    def __init__(
        self,
        demand: dict,
        method: tuple,
        database_dates: dict = None,
        scenario: dict = None,
        use_global_lci_cache: bool = True,
    ) -> None:
```

Replace the `database_dates` docstring entry and add one for `scenario`:

```
        database_dates : dict, optional
                Dictionary mapping database names to the point in time they
                represent, as a `datetime`, or to `"dynamic"` for databases whose
                processes are distributed over time (typically the foreground).
                Several databases may share the same date, e.g. to keep your own
                modified copies of background processes in their own database
                instead of writing them into the shared background database for
                that vintage. If not given, the mapping is read from the
                databases' own `representative_time` metadata (which premise
                writes when exporting, and which you can set yourself with
                `bw_timex.set_database_metadata`). Passing this argument replaces
                the metadata entirely: only the databases listed here are used.
        scenario : dict, optional
                Metadata a background database must match to be used, e.g.
                `{"iam_model": "remind", "pathway": "SSP2-PkBudg500"}`. Only
                needed when the project holds several scenarios - `TimexLCA`
                raises and lists them otherwise. Databases that don't declare the
                filtered key (your foreground, a hand-built vintage) are always
                kept. Cannot be combined with `database_dates`.
```

Replace the fallback block (`self.database_dates = database_dates` through the `if not self.database_dates:` block) with:

```python
        self.scenario = scenario
        self.database_dates = self._resolve_database_dates(
            demand=demand, database_dates=database_dates, scenario=scenario
        )
```

Add the method right after `__init__`:

```python
    @staticmethod
    def _resolve_database_dates(
        demand: dict, database_dates: dict | None, scenario: dict | None
    ) -> dict:
        """Map databases to the points in time they represent.

        Either from the explicit `database_dates` argument, which is then the
        whole mapping, or from the databases' own `representative_time`
        metadata. Databases holding the demand default to `"dynamic"`.
        """
        if database_dates:
            if scenario:
                raise ValueError(
                    "`scenario` selects background databases by their metadata and "
                    "only applies when `database_dates` is not given. Pass one or "
                    "the other."
                )
            return dict(database_dates)

        resolved = resolve_database_dates_from_metadata(scenario)

        if not resolved:
            logger.info(
                "No database_dates provided, and no database in this project carries "
                "`representative_time` metadata. Treating the databases containing the "
                "functional unit as dynamic. No remapping of inventories to time "
                "explicit databases will be done."
            )

        for key in demand:
            database = bd.get_node(id=get_id(key))["database"]
            resolved.setdefault(database, "dynamic")

        return resolved
```

- [ ] **Step 4: Accept `scenario` in the input validation**

In `bw_timex/validation.py`, add the field and its validator to `TimexLCAInputs`:

```python
    scenario: Optional[dict] = None
```

```python
    @field_validator("scenario")
    @classmethod
    def validate_scenario(cls, v: Optional[dict]) -> Optional[dict]:
        if v is None:
            return v
        if not v:
            raise ValueError("scenario must be a non-empty dictionary if provided.")
        for key, value in v.items():
            if not isinstance(key, str):
                raise ValueError(
                    f"scenario keys must be strings (database metadata keys), got "
                    f"{type(key).__name__}."
                )
            if not isinstance(value, (str, int, float, bool, list, tuple)):
                raise ValueError(
                    f"scenario values must be scalars or lists of scalars, got "
                    f"{type(value).__name__} for key '{key}'."
                )
        return v
```

And pass it in `timex_lca.py`, where `TimexLCAInputs` is instantiated:

```python
        TimexLCAInputs(
            demand=self.demand,
            method=self.method,
            database_dates=self.database_dates,
            scenario=self.scenario,
        )
```

- [ ] **Step 5: Update the class docstring example**

In the `Examples` block of the `TimexLCA` class docstring, put the metadata path first and keep the explicit mapping as the alternative:

```python
    from bw_timex import TimexLCA, set_database_metadata

    demand = {("my_foreground_database", "my_process"): 1}
    method = ("some_method_family", "some_category", "some_method")

    # Databases exported by premise already know the point in time they
    # represent. For your own databases, say so once:
    set_database_metadata("my_background_database_one", representative_time=datetime(2020, 1, 1))
    set_database_metadata("my_background_database_two", representative_time=datetime(2030, 1, 1))

    tlca = TimexLCA(demand, method)

    # ... or map the databases explicitly, which then replaces the metadata:
    tlca = TimexLCA(
        demand,
        method,
        database_dates={
            "my_background_database_one": datetime(2020, 1, 1),
            "my_background_database_two": datetime(2030, 1, 1),
            # Several databases may share the same date, e.g. to keep your own
            # modified copies of background processes in their own database:
            "my_modified_background_2020": datetime(2020, 1, 1),
            "my_foreground_database": "dynamic",
        },
    )

    tlca.build_timeline()  # has many optional arguments
    tlca.lci()
    tlca.static_lcia()
    print(tlca.static_score)
    # also available: "GWP", "pGWP", "pGTP", "prospective_radiative_forcing"
    tlca.dynamic_lcia(metric="radiative_forcing")
    print(tlca.dynamic_score)
```

- [ ] **Step 6: Run the new tests**

Run: `.venv/bin/pytest tests/test_database_metadata.py -v`
Expected: PASS (33 tests)

- [ ] **Step 7: Run the whole suite to prove nothing regressed**

Run: `.venv/bin/pytest -x -q`
Expected: PASS, same count as on `main` plus the new tests. Every existing test passes `database_dates`, so the exclusive branch must keep them green.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/timex_lca.py bw_timex/validation.py tests/test_database_metadata.py
git commit -m "feat: read database timing from metadata by default in TimexLCA"
```

---

### Task 5: Documentation

**Files:**
- Create: `docs/content/background_database_metadata.md`
- Create: `docs/api/database_metadata.md`
- Modify: `zensical.toml` (User Guide nav, API nav)
- Modify: `docs/content/getting_started/quickstart.md:58-70`, `:96`, `:128-132`
- Modify: `docs/content/getting_started/adding_temporal_information.md:250-282`
- Modify: `docs/content/getting_started/build_process_timeline.md:11-21`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `set_database_metadata`, `TimexLCA(scenario=...)` from Tasks 1–4.
- Produces: no code.

- [ ] **Step 1: Write the new reference page**

Create `docs/content/background_database_metadata.md`:

````markdown
---
icon: lucide/calendar-clock
tags:
  - background databases
---

# What a database represents

`bw_timex` needs to know which point in time each background database stands for.
That information lives in the database's own Brightway metadata, so it only has to
be recorded once - not in every script.

```python
import bw2data as bd

bd.databases["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2050"]
```

```python
{
    # written by brightway
    "format": "Ecoinvent XML", "backend": "sqlite", "number": 43648, ...,
    # written by premise
    "premise_version": "2.4.9.1",
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "representative_time": "2050-01-01T00:00:00",
    "ecoinvent_version": "3.10.1",
    "system_model": "cutoff",
}
```

Only `representative_time` is required. `TimexLCA` reads it from every database of
your project, so a study on premise databases needs no timing argument at all:

```python
tlca = TimexLCA(demand={("foreground", "A"): 1}, method=("our", "method"))
```

!!! info "premise version"

    premise writes this metadata from the version following 2.4.9.2 onwards. For
    databases written by an earlier version, set it yourself as shown below - it is
    a one-liner per database.

## Setting it yourself

For databases you built yourself, use
[`set_database_metadata`][bw_timex.database_metadata.set_database_metadata]:

```python
from datetime import datetime
from bw_timex import set_database_metadata

set_database_metadata("background_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))
```

The value is stored as an ISO 8601 string, because Brightway keeps database
metadata as JSON. You only do this once per database: it is stored in the project,
not in your script.

Your foreground doesn't represent a point in time - its processes get distributed
over time. `TimexLCA` treats the databases holding your functional unit as
`"dynamic"` automatically, but you can also say so explicitly:

```python
set_database_metadata("foreground", representative_time="dynamic")
```

## Several databases for the same point in time

More than one database may carry the same date. This is useful when you modify
background processes: keep the modified copies in your own database per point in
time, instead of writing them into ecoinvent or premise.

```python
set_database_metadata("my_background_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("my_background_2030", representative_time=datetime(2030, 1, 1))
```

For each process, `bw_timex` interpolates only between the databases that actually
contain it, matched on `name`, `reference product` and `location`.

## Choosing a scenario

A project often holds more than one IAM scenario. `bw_timex` refuses to guess and
tells you what it found:

```
Several background scenarios found in this project:
  pathway=SSP2-PkBudg500: ei_..._2030, ei_..._2040, ei_..._2050
  pathway=SSP2-Base: ei_..._2030, ei_..._2040, ei_..._2050
Select one, e.g. scenario={'pathway': '...'}, or map the databases explicitly with
`database_dates`.
```

Pick one with the `scenario` argument, which filters the databases on their
metadata:

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    scenario={"pathway": "SSP2-PkBudg500"},
)
```

Any metadata key works - `iam_model`, `pathway`, `system_model`,
`ecoinvent_version`, `premise_version`, or anything you set yourself. Databases
that don't carry the key at all (your foreground, your own vintages) are never
filtered out.

Comparing scenarios is then a loop over filters:

```python
scores = {}
for pathway in ("SSP2-Base", "SSP2-PkBudg500"):
    tlca = TimexLCA(demand, method, scenario={"pathway": pathway})
    tlca.build_timeline()
    tlca.lci()
    tlca.static_lcia()
    scores[pathway] = tlca.static_score
```

!!! warning "Superstructure databases"

    Databases holding several scenarios at once (premise superstructure or
    scenario-array exports) are skipped: they have no single technosphere per point
    in time. Use one database per scenario and year.

## Mapping the databases explicitly

`database_dates` still does what it always did, and takes over completely: when you
pass it, metadata is not read at all and only the databases you list are used.

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    database_dates={
        "background": datetime(2020, 1, 1),
        "background_2030": datetime(2030, 1, 1),
        "foreground": "dynamic",
    },
)
```

Use it when you want to restrict a calculation to a subset of the databases in your
project, or when a database's metadata is wrong and you don't want to change it.
````

- [ ] **Step 2: Add both pages to the nav**

In `zensical.toml`, add the User Guide entry after the Walkthrough block (after the line `]},` that closes `Walkthrough`, before `{ "What LCA should I do?" ...`):

```toml
    { "What a database represents" = "content/background_database_metadata.md" },
```

And in the API nav block, next to the other API pages:

```toml
    { "Database metadata" = "api/database_metadata.md" },
```

Create `docs/api/database_metadata.md`, following `docs/api/utils.md`:

```markdown
---
icon: lucide/calendar-clock
tags:
  - api
---

# Database metadata

Reading and writing what a Brightway database represents: the point in time
(`representative_time`) and, for prospective databases, the scenario it was built
for.

::: bw_timex.database_metadata
```

- [ ] **Step 3: Update the quickstart**

In `docs/content/getting_started/quickstart.md`, replace step 3 and the `TimexLCA` call:

```python
# 3. Say what your time-specific background databases represent
#    (premise databases already know - skip this for them)
set_database_metadata("background", representative_time=datetime(2020, 1, 1))
set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))

# 4. Create the TimexLCA object
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
)
```

Add `set_database_metadata` to the `from bw_timex import ...` line at the top of that
code block. In the cheat sheet, change the background row to:

```
| *How the background changes* over time | one database per point in time, each with `representative_time` metadata | background databases |
```

And replace the trailing paragraph about `database_dates` (lines 128-132) with:

```markdown
Absolute dates (`dtype="datetime64[s]"`) are also allowed in a `TemporalDistribution`,
e.g. for the timing of the functional unit itself. Relative dates
(`dtype="timedelta64[Y]"`) are relative to the consuming process. Several databases may
represent the same point in time, e.g. if you keep modified copies of background
processes in their own database instead of writing them into the shared vintage. See
[What a database represents](../background_database_metadata.md) for scenario selection
and for mapping databases explicitly with `database_dates`.
```

- [ ] **Step 4: Update walkthrough step 1**

In `docs/content/getting_started/adding_temporal_information.md`, replace the paragraph and code block at lines 250-262 with:

````markdown
So, as you can see, the processes at specific time steps reside within a separate normal
Brightway database. `bw_timex` picks these up automatically, as long as each database
says which point in time it represents:

```python
from datetime import datetime
from bw_timex import set_database_metadata

set_database_metadata("background", representative_time=datetime(2020, 1, 1))
set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))
```

You only do this once per database - it is stored in your Brightway project. Databases
exported by [premise](https://premise.readthedocs.io/en/latest/introduction.html) bring
this metadata with them, so there is nothing to do for those. The foreground doesn't
represent a specific point in time and is distributed over time instead; `bw_timex`
treats the databases holding your functional unit that way automatically.
````

Replace the code block in the "Several databases for the same point in time" section
(lines 274-282) with:

```python
set_database_metadata("ecoinvent_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("ecoinvent_2030", representative_time=datetime(2030, 1, 1))
set_database_metadata("my_background_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("my_background_2030", representative_time=datetime(2030, 1, 1))
```

- [ ] **Step 5: Update walkthrough step 2**

In `docs/content/getting_started/build_process_timeline.md`, replace lines 11-21 with:

````markdown
With all the temporal information prepared, we can now instantiate our TimexLCA object.
This is just like a normal Brightway LCA object - the timing of the background databases
comes from their metadata:

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
)
```

If your project holds several scenarios, select one with
`scenario={"pathway": "SSP2-PkBudg500"}`; to map the databases by hand instead, pass
`database_dates`. Both are covered in
[What a database represents](../background_database_metadata.md).
````

- [ ] **Step 6: Add the changelog entry**

Under `## [Unreleased]` in `CHANGES.md`:

```markdown
* Added `representative_time` database metadata as the default timing source: `TimexLCA` now maps background databases to points in time by reading their Brightway metadata (as written by premise), making `database_dates` optional ([#217](https://github.com/brightway-lca/bw_timex/issues/217))
* Added `set_database_metadata` to record what a database represents (`representative_time`, and scenario fields such as `iam_model` or `pathway`) for databases that don't bring the metadata themselves
* Added `TimexLCA(scenario={...})` to select one background scenario when a project holds several; `TimexLCA` raises and lists the scenarios it found if the choice is ambiguous
```

- [ ] **Step 7: Verify the docs build**

Run: `.venv/bin/python -m zensical build 2>&1 | tail -20`
Expected: build succeeds, no warning about `background_database_metadata.md` or `api/database_metadata.md` being missing from the nav. If `zensical` is not installed in the venv, run `.venv/bin/python -c "import tomllib, pathlib; tomllib.loads(pathlib.Path('zensical.toml').read_text())"` to at least prove the nav edit is valid TOML, and say in the commit that the build was not run.

- [ ] **Step 8: Commit**

```bash
git add docs zensical.toml CHANGES.md
git commit -m "docs: document representative_time database metadata"
```

---

### Task 6: Tutorial notebooks

**Files:**
- Modify: `notebooks/tutorials/1_getting_started.ipynb`
- Modify: `notebooks/tutorials/2_electric_vehicle_from_scratch.ipynb`
- Modify: `notebooks/tutorials/3_dynamic_characterization.ipynb`
- Modify: `notebooks/tutorials/4_import_model_from_excel.ipynb`

**Interfaces:**
- Consumes: `set_database_metadata`, `TimexLCA()` without `database_dates` from Tasks 1–4.
- Produces: no code.

These notebooks build their own small databases, so they can be re-executed.

- [ ] **Step 1: Find every occurrence**

Run: `grep -n "database_dates" notebooks/tutorials/*.ipynb`
Note which cells build the mapping and which pass it to `TimexLCA`.

- [ ] **Step 2: Edit the cells**

In each notebook, use `NotebookEdit` to:
1. Replace the cell that builds `database_dates` with `set_database_metadata` calls, one per background database, keeping the surrounding markdown explanation in sync (it must no longer say "we define a dictionary that maps databases to dates").
2. Drop the `database_dates=database_dates` argument from the `TimexLCA(...)` call.
3. Add `set_database_metadata` to the `from bw_timex import ...` cell.

Pattern:

```python
# before
database_dates = {
    "db_2020": datetime.strptime("2020", "%Y"),
    "db_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
tlca = TimexLCA(demand={fu.key: 1}, method=method, database_dates=database_dates)

# after
set_database_metadata("db_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("db_2030", representative_time=datetime(2030, 1, 1))
tlca = TimexLCA(demand={fu.key: 1}, method=method)
```

- [ ] **Step 3: Re-execute each notebook**

Run, one notebook at a time:

```bash
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/tutorials/1_getting_started.ipynb
```

Expected: completes without error. If a notebook needs data that isn't in the repo, do
not execute it — leave the stored outputs, and note that in the commit message.

- [ ] **Step 4: Check the diff for accidental churn**

Run: `git diff --stat notebooks/tutorials`
Expected: only the edited cells plus their re-executed outputs. If execution rewrote
every cell id or bumped unrelated metadata, restore and re-run with
`--ClearMetadataPreprocessor.enabled=True` off, keeping the diff readable.

- [ ] **Step 5: Commit**

```bash
git add notebooks/tutorials
git commit -m "docs: use database metadata instead of database_dates in the tutorials"
```

---

### Task 7: Remaining notebooks

**Files:**
- Modify: `notebooks/advanced/background_temporal_distributions.ipynb`
- Modify: `notebooks/advanced/uncertainty_with_datapackages.ipynb`
- Modify: `notebooks/advanced/background_temporal_distributions_premise.ipynb`
- Modify: `notebooks/teaching/ev_walkthrough_premise.ipynb`
- Modify: `notebooks/teaching/exercise_ev_vs_petrol_solutions.ipynb`
- Modify: `notebooks/examples/electric_vehicle_premise.ipynb`
- Modify: `notebooks/examples/electric_vehicle_premise_detailed.ipynb`
- Modify: `notebooks/development/benchmarking.ipynb`
- **Do not touch:** `notebooks/examples/paper_case_study.ipynb`

**Interfaces:**
- Consumes: `set_database_metadata`, `TimexLCA(scenario=...)` from Tasks 1–4.
- Produces: no code.

The first two build their own databases and can be re-executed. The rest need premise
or ecoinvent databases that aren't in the repo: edit the cell sources only and leave
the stored outputs alone.

- [ ] **Step 1: Edit the two self-contained notebooks**

`background_temporal_distributions.ipynb` and `uncertainty_with_datapackages.ipynb`:
same replacement as Task 6 Step 2, then re-execute:

```bash
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/advanced/background_temporal_distributions.ipynb
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/advanced/uncertainty_with_datapackages.ipynb
```

- [ ] **Step 2: Edit the premise/ecoinvent notebooks**

For each of the six remaining notebooks, replace the `database_dates` cell. These use
premise databases, which carry the metadata already, so the mapping usually
disappears entirely:

```python
# before
database_dates = {
    "ei312_REMIND-EU_SSP2_NDC_2020": datetime.strptime("2020", "%Y"),
    "ei312_REMIND-EU_SSP2_NDC_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
tlca = TimexLCA(demand={fu.key: 1}, method=method, database_dates=database_dates)

# after
# The premise databases carry the point in time they represent in their
# metadata, so bw_timex finds them by itself.
tlca = TimexLCA(demand={fu.key: 1}, method=method)
```

Two things to get right per notebook:
- If the notebook creates its own modified copies of background processes in extra
  databases (the electric-vehicle notebooks do, e.g. `..., without EOL` copies), those
  copies need `set_database_metadata(..., representative_time=...)` with the same date
  as the vintage they were copied from, or they drop out of the mapping.
- If the notebook's project could hold more than one pathway, show the `scenario`
  argument in the markdown right below, e.g.
  `scenario={"pathway": "SSP2-PkBudg500"}`.

Update the surrounding markdown text wherever it explains `database_dates`.

- [ ] **Step 3: Verify no notebook lost its outputs**

Run: `git diff --stat notebooks`
Expected: for the six premise notebooks, only source cells change - no `outputs` churn.

- [ ] **Step 4: Confirm the paper case study is untouched**

Run: `git status --porcelain notebooks/examples/paper_case_study.ipynb`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add notebooks
git commit -m "docs: use database metadata instead of database_dates in the notebooks"
```

---

### Task 8: Final verification

**Files:** none

- [ ] **Step 1: Full test suite**

Run: `.venv/bin/pytest -q`
Expected: all pass.

- [ ] **Step 2: Nothing still teaches the old default**

Run: `grep -rn "database_dates" --include="*.md" --include="*.ipynb" docs notebooks | grep -v paper_case_study | grep -v superpowers`
Expected: only the places that deliberately document `database_dates` as the explicit
override — `background_database_metadata.md`, the quickstart's closing paragraph, and
step 2's pointer. Anything else is a leftover.

- [ ] **Step 3: Public API test still describes the namespace**

Run: `.venv/bin/pytest tests/test_public_api.py -v`
Expected: PASS. If it asserts an exact `__all__`, add `set_database_metadata` to it.

- [ ] **Step 4: Commit any fixes and push the branch**

```bash
git add -A
git commit -m "fix: address leftovers from the metadata migration"
git push -u origin feat/representative-time-metadata
```
