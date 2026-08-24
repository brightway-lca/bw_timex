# Premise scenario auto-creation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `TimexLCA(scenario={...}, create_missing=True)` names background vintages the project does not have, build them with premise (importing ecoinvent first if needed) instead of raising.

**Architecture:** A new module `bw_timex/scenario_builder.py` owns the whole "find or build" decision. It splits the `scenario` dict into *filter keys* (matched against database metadata, as today) and *build keys* (`years`, `sectors`, `source_database`), finds which years the project already satisfies, and builds only the rest. All premise/bw2io contact is confined to two logic-free seam functions that import their library inside the function body, so premise stays an optional extra and the rest of the module is testable without it. `TimexLCA.__init__` calls `ensure_scenario_databases` before `_resolve_database_dates`, then resolves dates from metadata exactly as it does today — one code path decides what a database represents.

**Tech Stack:** Python ≥3.11, bw2data ≥4.6, pydantic ≥2, pytest, loguru, premise ≥2.4.9.2 (optional extra), bw2io (via premise's `bw25` extra).

**Spec:** `docs/superpowers/specs/2026-08-23-premise-scenario-autocreation-design.md`

## Global Constraints

- Optional dependency, exact string: `premise = ["premise[bw25]>=2.4.9.2"]`. The `[bw25]` extra is mandatory — bare `premise` leaves `bw2data`/`bw2io` unpinned and can resolve to `bw2data==3.6.6`, contradicting bw_timex's `bw2data>=4.6`.
- `premise` and `bw2io` are imported **inside function bodies only**. No module-level import of either, anywhere in `bw_timex/`.
- Default behaviour is unchanged: `create_missing=False`. Without it, no premise import, no build, and the existing error text is preserved (one line added).
- Filter keys are exactly `("iam_model", "pathway", "system_model", "ecoinvent_version")`. Build keys are exactly `("years", "sectors", "source_database")`.
- Built database names: `ei_{system_model}_{ecoinvent_version}_{iam_model}_{pathway}_{year}`.
- Environment variable names, verbatim: `PREMISE_KEY`, `ECOINVENT_USERNAME`, `ECOINVENT_PASSWORD`. Explicit arguments always win over the environment.
- premise ≥ 2.4.9.2 writes `representative_time`, `iam_model`, `pathway`, `system_model`, `ecoinvent_version`, `premise_version` itself. Do **not** re-write those. Only `sectors` is written by us, and only when narrowed.
- No test may require premise, bw2io, a premise key, ecoinvent credentials, or network access.
- Existing code style: `loguru.logger` for user-facing progress, pydantic models in `bw_timex/validation.py` for argument validation, tests use `bw2data.tests.bw2test` fixtures from `tests/fixtures/`.

## File Structure

| File | Responsibility |
|---|---|
| `bw_timex/database_metadata.py` (modify) | Gains the scenario-key vocabulary (`SCENARIO_BUILD_KEYS`, `split_scenario`) and one reusable predicate (`database_matches_scenario`). Stays side-effect-free. |
| `bw_timex/scenario_builder.py` (create) | `ensure_scenario_databases` + credential resolution + the two premise/bw2io seams. The only module that knows premise exists. |
| `bw_timex/validation.py` (modify) | `ScenarioBuildInputs`; `TimexLCAInputs` gains the three new arguments and their combination rules. |
| `bw_timex/timex_lca.py` (modify) | Argument plumbing in `__init__` only, plus one added line in the "matched no database" error. |
| `bw_timex/__init__.py` (modify) | Re-export `ensure_scenario_databases`. |
| `tests/test_scenario_builder.py` (create) | Everything on this side of the seams, with the seams faked. |
| `tests/test_database_metadata.py` (modify) | Tests for the new metadata helpers. |
| `pyproject.toml`, `docs/`, `CHANGES.md` (modify) | Extra, documentation, changelog. Folded into the last task. |

---

### Task 1: Scenario key vocabulary and the match predicate

Build keys must be stripped before any metadata filtering, or `_check_filter_keys` raises "No database in this project declares the metadata key(s) ['years']" for every `create_missing` call. And the "is this year already satisfied?" question in Task 2 must use exactly the resolver's keep-rule, or it builds a duplicate vintage next to a database the resolver would have used.

**Files:**
- Modify: `bw_timex/database_metadata.py`
- Test: `tests/test_database_metadata.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `SCENARIO_FILTER_KEYS: tuple[str, ...]` = `("iam_model", "pathway", "system_model", "ecoinvent_version")`
  - `SCENARIO_BUILD_KEYS: tuple[str, ...]` = `("years", "sectors", "source_database")`
  - `split_scenario(scenario: dict | None) -> tuple[dict, dict]` returning `(filters, build)`
  - `database_matches_scenario(metadata: dict, scenario: dict | None) -> bool`
  - `resolve_database_dates_from_metadata(scenario)` now tolerates build keys in `scenario`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_database_metadata.py`:

```python
# ─── Tests for the scenario key vocabulary ───


class TestSplitScenario:

    def test_build_keys_are_separated(self):
        from bw_timex.database_metadata import split_scenario

        filters, build = split_scenario(
            {
                "iam_model": "remind",
                "pathway": "SSP2-PkBudg500",
                "years": [2020, 2030],
                "sectors": ["electricity"],
                "source_database": "my_ecoinvent",
            }
        )
        assert filters == {"iam_model": "remind", "pathway": "SSP2-PkBudg500"}
        assert build == {
            "years": [2020, 2030],
            "sectors": ["electricity"],
            "source_database": "my_ecoinvent",
        }

    def test_unknown_keys_are_treated_as_filters(self):
        from bw_timex.database_metadata import split_scenario

        filters, build = split_scenario({"my_own_key": "value"})
        assert filters == {"my_own_key": "value"}
        assert build == {}

    def test_none_scenario_gives_empty_dicts(self):
        from bw_timex.database_metadata import split_scenario

        assert split_scenario(None) == ({}, {})


class TestDatabaseMatchesScenario:

    def test_declared_and_equal_matches(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"pathway": "SSP2-PkBudg500"}, {"pathway": "SSP2-PkBudg500"})

    def test_declared_and_different_does_not_match(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert not database_matches_scenario({"pathway": "SSP2-Base"}, {"pathway": "SSP2-PkBudg500"})

    def test_undeclared_key_still_matches(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"representative_time": "2020-01-01"}, {"pathway": "SSP2-PkBudg500"})

    def test_list_values_compare_order_insensitively(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario(
            {"external_scenarios": ["b", "a"]}, {"external_scenarios": ["a", "b"]}
        )

    def test_empty_scenario_matches_everything(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"pathway": "SSP2-Base"}, None)


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestResolverIgnoresBuildKeys:

    def test_build_keys_do_not_reach_the_filter(self):
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            pathway="SSP2-PkBudg500",
        )
        resolved = resolve_database_dates_from_metadata(
            {"pathway": "SSP2-PkBudg500", "years": [2022], "sectors": ["electricity"]}
        )
        assert resolved == {"db_2022": datetime(2022, 1, 1)}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_database_metadata.py -k "SplitScenario or DatabaseMatchesScenario or ResolverIgnoresBuildKeys" -v`
Expected: FAIL with `ImportError: cannot import name 'split_scenario'` (and, for the last test, `ValueError: No database in this project declares the metadata key(s) ['sectors', 'years']`).

- [ ] **Step 3: Implement**

In `bw_timex/database_metadata.py`, below the existing `SCENARIO_SIGNATURE_KEYS` block, add:

```python
#: Metadata keys premise writes that identify which scenario a database belongs
#: to, and that `TimexLCA(scenario=...)` filters on.
SCENARIO_FILTER_KEYS = (
    "iam_model",
    "pathway",
    "system_model",
    "ecoinvent_version",
)

#: Keys of a `scenario` mapping that describe how to *build* a missing vintage
#: rather than what to match. They never reach the metadata filter: `years` is a
#: list, and no database's metadata could ever equal it.
SCENARIO_BUILD_KEYS = ("years", "sectors", "source_database")


def split_scenario(scenario: dict | None) -> tuple[dict, dict]:
    """Separate a `scenario` mapping into its filter keys and its build keys."""
    if not scenario:
        return {}, {}
    filters = {k: v for k, v in scenario.items() if k not in SCENARIO_BUILD_KEYS}
    build = {k: v for k, v in scenario.items() if k in SCENARIO_BUILD_KEYS}
    return filters, build


def database_matches_scenario(metadata: dict, scenario: dict | None) -> bool:
    """Whether a database's metadata survives a `scenario` filter.

    A database is kept unless it *declares* a filtered key with a different
    value: a hand-built vintage or a foreground carrying no scenario metadata
    belongs to every scenario, not to none.
    """
    if not scenario:
        return True
    return all(
        key not in metadata or _values_match(metadata[key], wanted)
        for key, wanted in scenario.items()
    )
```

Then rewrite the filtering block of `resolve_database_dates_from_metadata` to strip build keys and use the predicate:

```python
    candidates = _candidate_databases()
    scenario, _ = split_scenario(scenario)
    if scenario:
        _check_filter_keys(scenario, candidates)
        candidates = {
            name: metadata
            for name, metadata in candidates.items()
            if database_matches_scenario(metadata, scenario)
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_database_metadata.py -v`
Expected: PASS, including every pre-existing test in the file (the refactor must not change resolver behaviour).

- [ ] **Step 5: Run the whole suite**

Run: `pytest -q`
Expected: PASS, same count as before plus the new tests.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/database_metadata.py tests/test_database_metadata.py
git commit -m "feat: separate scenario build keys from filter keys"
```

---

### Task 2: Find what exists, build nothing

The steady-state path: a study re-run where every vintage is already there. It must not import premise, and it must be the same match rule the resolver uses.

**Files:**
- Create: `bw_timex/scenario_builder.py`
- Test: `tests/test_scenario_builder.py`

**Interfaces:**
- Consumes: `split_scenario`, `database_matches_scenario`, `_normalize_representative_time`, `REPRESENTATIVE_TIME`, `SCENARIOS`, `DYNAMIC` from `bw_timex.database_metadata`.
- Produces:
  - `find_existing_vintages(filters: dict) -> dict[int, str]` — year → database name
  - `ensure_scenario_databases(scenario: dict, premise_key: str | None = None, ecoinvent_credentials: tuple[str, str] | None = None) -> dict[str, datetime]`
  - `_run_premise(...)` and `_import_ecoinvent(...)`, the seams later tasks fill in and every test monkeypatches

- [ ] **Step 1: Write the failing tests**

Create `tests/test_scenario_builder.py`:

```python
"""Tests for finding or building the background vintages a scenario names.

premise is never installed, never called and never imported here: the two
functions that touch it are monkeypatched with fakes that register small
Brightway databases carrying premise-style metadata.
"""

import sys
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import set_database_metadata
from bw_timex.scenario_builder import ensure_scenario_databases, find_existing_vintages

SCENARIO = {
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "system_model": "cutoff",
    "ecoinvent_version": "3.10.1",
}


def write_minimal_database(name):
    """A one-process database, enough to be registered and carry metadata."""
    bd.Database(name).write(
        {
            (name, "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": (name, "C")},
                ],
            },
        },
    )


def write_vintage(name, year, **extra):
    """A database that looks like a premise export for `year`."""
    write_minimal_database(name)
    set_database_metadata(
        name,
        representative_time=datetime(year, 1, 1),
        premise_version="2.4.9.2",
        **{**SCENARIO, **extra},
    )


@pytest.fixture
def fake_premise(monkeypatch):
    """Records calls to the premise seam and writes what premise would write."""
    calls = []

    def fake_run_premise(**kwargs):
        calls.append(kwargs)
        years = [scenario["year"] for scenario in kwargs["scenarios"]]
        for name, year in zip(kwargs["names"], years):
            write_vintage(name, year)

    monkeypatch.setattr(
        "bw_timex.scenario_builder._run_premise", fake_run_premise, raising=True
    )
    return calls


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestFindExistingVintages:

    def test_matching_vintage_is_found_by_year(self):
        write_vintage("ei_2030", 2030)
        assert find_existing_vintages(SCENARIO) == {2030: "ei_2030"}

    def test_other_scenario_is_not_found(self):
        write_vintage("ei_2030", 2030, pathway="SSP2-Base")
        assert find_existing_vintages(SCENARIO) == {}

    def test_database_without_scenario_metadata_satisfies_its_year(self):
        write_minimal_database("hand_built_2020")
        set_database_metadata("hand_built_2020", representative_time=datetime(2020, 1, 1))
        assert find_existing_vintages(SCENARIO) == {2020: "hand_built_2020"}

    def test_dynamic_databases_are_ignored(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert find_existing_vintages(SCENARIO) == {}

    def test_multi_scenario_databases_are_ignored(self):
        write_vintage("superstructure", 2030)
        bd.databases["superstructure"]["scenarios"] = [{"year": 2030}, {"year": 2040}]
        bd.databases.flush()
        assert find_existing_vintages(SCENARIO) == {}


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestNothingToBuild:

    def test_all_years_present_builds_nothing(self, fake_premise):
        write_vintage("ei_2030", 2030)
        write_vintage("ei_2040", 2040)
        ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise == []

    def test_all_years_present_returns_the_mapping(self, fake_premise):
        write_vintage("ei_2030", 2030)
        result = ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert result == {"ei_2030": datetime(2030, 1, 1)}

    def test_premise_is_not_imported(self, fake_premise, monkeypatch):
        monkeypatch.delitem(sys.modules, "premise", raising=False)
        write_vintage("ei_2030", 2030)
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert "premise" not in sys.modules
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_scenario_builder.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bw_timex.scenario_builder'`.

- [ ] **Step 3: Implement**

Create `bw_timex/scenario_builder.py`:

```python
"""Find the background vintages a scenario names, or build them with premise.

`TimexLCA(scenario=...)` selects background databases by their metadata. When
the project does not hold them yet, `ensure_scenario_databases` builds the
missing ones with premise instead of leaving the user at a dead end.

premise and bw2io are imported inside `_run_premise` and `_import_ecoinvent`
only, so `bw_timex` keeps working without them installed and a run that finds
everything it needs never touches either.
"""

from __future__ import annotations

from datetime import datetime

import bw2data as bd
from loguru import logger

from .database_metadata import (
    DYNAMIC,
    REPRESENTATIVE_TIME,
    SCENARIOS,
    _normalize_representative_time,
    database_matches_scenario,
    split_scenario,
)


def find_existing_vintages(filters: dict) -> dict[int, str]:
    """Map each year the project already covers to the database covering it.

    A year is covered by a registered database whose `representative_time`
    falls in it and that the scenario filter keeps. "Keeps" is the resolver's
    own rule (`database_matches_scenario`): a database is dropped only if it
    declares a filtered key with a different value. Any stricter rule would
    build a second database for a year `TimexLCA` already resolves.
    """
    found = {}
    for name in bd.databases:
        metadata = bd.databases[name]
        if REPRESENTATIVE_TIME not in metadata or metadata.get(SCENARIOS):
            continue
        value = _normalize_representative_time(metadata[REPRESENTATIVE_TIME], name)
        if value == DYNAMIC or not isinstance(value, datetime):
            continue
        if not database_matches_scenario(metadata, filters):
            continue
        found.setdefault(value.year, name)
    return found


def _run_premise(**kwargs) -> None:
    """Filled in by Task 5. Every premise call happens here and nowhere else."""
    raise NotImplementedError


def _import_ecoinvent(**kwargs) -> str:
    """Filled in by Task 4. Every bw2io call happens here and nowhere else."""
    raise NotImplementedError


def ensure_scenario_databases(
    scenario: dict,
    premise_key: str | None = None,
    ecoinvent_credentials: tuple[str, str] | None = None,
) -> dict[str, datetime]:
    """
    Make sure every year of `scenario` has a background database, building what is missing.

    Parameters
    ----------
    scenario : dict
        The same mapping `TimexLCA` takes, plus the build keys `years`
        (required), `sectors` and `source_database`.
    premise_key : str, optional
        premise decryption key. Falls back to `$PREMISE_KEY`.
    ecoinvent_credentials : tuple, optional
        `(username, password)`, used only if ecoinvent has to be imported.
        Falls back to `$ECOINVENT_USERNAME` / `$ECOINVENT_PASSWORD`.

    Returns
    -------
    dict
        Database name to the point in time it represents, for the vintages
        found or built.
    """
    filters, build = split_scenario(scenario)
    years = build["years"]

    existing = find_existing_vintages(filters)
    missing = [year for year in years if year not in existing]

    if not missing:
        logger.info(
            f"All {len(years)} requested background vintage(s) already exist in "
            f"this project. Nothing to build."
        )
        return {existing[year]: datetime(year, 1, 1) for year in years}

    raise NotImplementedError  # building is added in Tasks 3-5
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_scenario_builder.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/scenario_builder.py tests/test_scenario_builder.py
git commit -m "feat: find the background vintages a scenario already covers"
```

---

### Task 3: Argument validation and credentials

Everything that can be rejected must be rejected here, before a multi-gigabyte build starts.

**Files:**
- Modify: `bw_timex/validation.py`
- Modify: `bw_timex/scenario_builder.py`
- Test: `tests/test_scenario_builder.py`

**Interfaces:**
- Consumes: `ensure_scenario_databases`, `SCENARIO_FILTER_KEYS`.
- Produces:
  - `bw_timex.validation.ScenarioBuildInputs` (pydantic model, fields `scenario: dict`)
  - `bw_timex.scenario_builder._resolve_premise_key(premise_key: str | None) -> str`
  - `bw_timex.scenario_builder._resolve_ecoinvent_credentials(credentials: tuple | None) -> tuple[str, str]`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scenario_builder.py`:

```python
@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestValidation:

    def test_missing_years_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases(SCENARIO)

    def test_empty_years_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases({**SCENARIO, "years": []})

    def test_non_integer_year_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases({**SCENARIO, "years": ["2030"]})

    def test_missing_filter_keys_are_named(self, fake_premise):
        with pytest.raises(ValueError, match="ecoinvent_version"):
            ensure_scenario_databases(
                {"iam_model": "remind", "pathway": "SSP2-PkBudg500", "system_model": "cutoff",
                 "years": [2030]}
            )

    def test_no_scenario_raises(self, fake_premise):
        with pytest.raises(ValueError, match="scenario"):
            ensure_scenario_databases(None)


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestCredentials:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self):
        # Without a source database the run would stop at the ecoinvent
        # credentials before ever reaching the premise key.
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_premise_key_argument_wins_over_environment(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "from-environment")
        ensure_scenario_databases({**SCENARIO, "years": [2030]}, premise_key="explicit")
        assert fake_premise[0]["key"] == "explicit"

    def test_premise_key_falls_back_to_environment(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "from-environment")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["key"] == "from-environment"

    def test_missing_premise_key_names_the_variable(self, fake_premise, monkeypatch):
        monkeypatch.delenv("PREMISE_KEY", raising=False)
        with pytest.raises(ValueError, match="PREMISE_KEY"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})
```

Note: the three `TestCredentials` tests reach the build path, which Tasks 4 and 5 complete. They are expected to fail until Task 5 lands; run them with `-k` as noted in each task's verification step.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_scenario_builder.py -k "TestValidation" -v`
Expected: FAIL — `NotImplementedError` instead of `ValueError`, and `AttributeError: 'NoneType' object has no attribute 'items'` for the `None` case.

- [ ] **Step 3: Implement the validation model**

Append to `bw_timex/validation.py`:

```python
class ScenarioBuildInputs(BaseModel):
    """Validates the scenario mapping handed to ensure_scenario_databases"""

    model_config = {"arbitrary_types_allowed": True}

    scenario: dict

    @field_validator("scenario")
    @classmethod
    def validate_scenario(cls, v: dict) -> dict:
        from .database_metadata import SCENARIO_FILTER_KEYS

        if not v:
            raise ValueError(
                "scenario must be a non-empty dictionary describing the background "
                "to build, e.g. {'iam_model': 'remind', 'pathway': 'SSP2-PkBudg500', "
                "'system_model': 'cutoff', 'ecoinvent_version': '3.10.1', "
                "'years': [2030, 2040]}."
            )
        years = v.get("years")
        if not years or not isinstance(years, (list, tuple)):
            raise ValueError(
                "scenario must contain a non-empty `years` list to build background "
                "databases, e.g. scenario={..., 'years': [2030, 2040]}. premise "
                "builds one database per year."
            )
        if not all(isinstance(year, int) and not isinstance(year, bool) for year in years):
            raise ValueError(
                f"scenario `years` must be integer years, e.g. [2030, 2040], got "
                f"{list(years)}."
            )
        missing = [key for key in SCENARIO_FILTER_KEYS if key not in v]
        if missing:
            raise ValueError(
                f"scenario is missing {missing}, which premise needs to build a "
                f"database. Provide all of {list(SCENARIO_FILTER_KEYS)}."
            )
        return v
```

- [ ] **Step 4: Implement credential resolution and wire in the validation**

In `bw_timex/scenario_builder.py`, add `import os` to the imports, `from .validation import ScenarioBuildInputs`, and:

```python
def _resolve_premise_key(premise_key: str | None) -> str:
    key = premise_key or os.environ.get("PREMISE_KEY")
    if not key:
        raise ValueError(
            "No premise decryption key. Pass `premise_key=...` or set the "
            "environment variable PREMISE_KEY. The key is needed to read "
            "premise's bundled IAM scenarios; see "
            "https://premise.readthedocs.io for how to request one."
        )
    return key


def _resolve_ecoinvent_credentials(
    credentials: tuple[str, str] | None,
) -> tuple[str, str]:
    if credentials:
        username, password = credentials
    else:
        username = os.environ.get("ECOINVENT_USERNAME")
        password = os.environ.get("ECOINVENT_PASSWORD")
    missing = [
        name
        for name, value in (
            ("ECOINVENT_USERNAME", username),
            ("ECOINVENT_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            f"No ecoinvent credentials, needed to import the source database "
            f"premise builds from. Pass `ecoinvent_credentials=(username, "
            f"password)` or set {' and '.join(missing)}."
        )
    return username, password
```

and make `ensure_scenario_databases` validate first:

```python
    ScenarioBuildInputs(scenario=scenario)
    filters, build = split_scenario(scenario)
```

`ScenarioBuildInputs(scenario=None)` raises a pydantic `ValidationError`, which subclasses `ValueError` and whose message contains `scenario`, satisfying `test_no_scenario_raises`.

- [ ] **Step 5: Run the validation tests to verify they pass**

Run: `pytest tests/test_scenario_builder.py -k "TestValidation or TestFindExistingVintages or TestNothingToBuild" -v`
Expected: PASS. (`TestCredentials` still fails at `NotImplementedError` — Task 5 completes it.)

- [ ] **Step 6: Commit**

```bash
git add bw_timex/validation.py bw_timex/scenario_builder.py tests/test_scenario_builder.py
git commit -m "feat: validate scenario build arguments and resolve credentials"
```

---

### Task 4: Source database, biosphere, and the overwrite guard

premise builds *from* an ecoinvent already in the project. Finding it, importing it if absent, and refusing to overwrite an unrelated database are all decisions that must happen before the build.

**Files:**
- Modify: `bw_timex/scenario_builder.py`
- Test: `tests/test_scenario_builder.py`

**Interfaces:**
- Consumes: `_resolve_ecoinvent_credentials` (Task 3).
- Produces:
  - `vintage_name(filters: dict, year: int) -> str`
  - `_resolve_source_database(filters: dict, build: dict, ecoinvent_credentials) -> tuple[str, str]` returning `(source_database, biosphere_name)`
  - `_check_no_collisions(names: list[str], filters: dict) -> None`
  - `_import_ecoinvent(version: str, system_model: str, credentials: tuple[str, str]) -> str` (real body)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scenario_builder.py`:

```python
@pytest.fixture
def fake_ecoinvent_import(monkeypatch):
    """Records ecoinvent imports and registers what bw2io would register."""
    calls = []

    def fake_import(version, system_model, credentials):
        calls.append((version, system_model, credentials))
        name = f"ecoinvent-{version}-{system_model}"
        write_minimal_database(name)
        write_minimal_database(f"ecoinvent-{version}-biosphere")
        return name

    monkeypatch.setattr(
        "bw_timex.scenario_builder._import_ecoinvent", fake_import, raising=True
    )
    return calls


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestVintageName:

    def test_name_is_deterministic(self):
        from bw_timex.scenario_builder import vintage_name

        assert (
            vintage_name(SCENARIO, 2030)
            == "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        )


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSourceDatabase:

    def test_existing_ecoinvent_is_used(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_ecoinvent_import == []
        assert fake_premise[0]["source_database"] == "ecoinvent-3.10.1-cutoff"
        assert fake_premise[0]["biosphere"] == "ecoinvent-3.10.1-biosphere"

    def test_missing_ecoinvent_is_imported(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        monkeypatch.setenv("ECOINVENT_USERNAME", "user")
        monkeypatch.setenv("ECOINVENT_PASSWORD", "secret")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_ecoinvent_import == [("3.10.1", "cutoff", ("user", "secret"))]

    def test_missing_ecoinvent_without_credentials_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        monkeypatch.delenv("ECOINVENT_USERNAME", raising=False)
        monkeypatch.delenv("ECOINVENT_PASSWORD", raising=False)
        with pytest.raises(ValueError, match="ECOINVENT_USERNAME"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

    def test_explicit_source_database_is_used(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("my_own_ecoinvent")
        write_minimal_database("biosphere3")
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "source_database": "my_own_ecoinvent"}
        )
        assert fake_ecoinvent_import == []
        assert fake_premise[0]["source_database"] == "my_own_ecoinvent"
        assert fake_premise[0]["biosphere"] == "biosphere3"

    def test_unregistered_explicit_source_database_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        with pytest.raises(ValueError, match="no_such_db"):
            ensure_scenario_databases(
                {**SCENARIO, "years": [2030], "source_database": "no_such_db"}
            )


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestOverwriteGuard:

    def test_foreign_database_under_target_name_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError, match="already exists"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

    def test_nothing_is_built_when_a_name_collides(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError):
            ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_scenario_builder.py -k "TestVintageName or TestSourceDatabase or TestOverwriteGuard" -v`
Expected: FAIL — `ImportError: cannot import name 'vintage_name'`, and `NotImplementedError` from the others.

- [ ] **Step 3: Implement**

In `bw_timex/scenario_builder.py`:

```python
def vintage_name(filters: dict, year: int) -> str:
    """The database name a built vintage gets."""
    return (
        f"ei_{filters['system_model']}_{filters['ecoinvent_version']}_"
        f"{filters['iam_model']}_{filters['pathway']}_{year}"
    )


def _check_no_collisions(names: list[str], filters: dict) -> None:
    """Refuse to build over a database that is not ours.

    `write_db_to_brightway` deletes and rewrites a database of the same name
    without asking. A name that exists here belongs to someone else: a name
    that matched the scenario would have satisfied its year already, and its
    year would not be in the build list.
    """
    colliding = [name for name in names if name in bd.databases]
    if colliding:
        raise ValueError(
            f"Database(s) {colliding} already exist(s) in this project but do(es) "
            f"not match scenario {filters!r}, and premise would overwrite them. "
            f"Rename or delete them, or map them yourself with `database_dates`."
        )


def _resolve_source_database(
    filters: dict, build: dict, ecoinvent_credentials
) -> tuple[str, str]:
    """The ecoinvent database premise builds from, and its biosphere.

    Importing ecoinvent takes a while and needs a licence, so it happens only
    when there is nothing to build from.
    """
    version = filters["ecoinvent_version"]
    system_model = filters["system_model"]
    default_biosphere = f"ecoinvent-{version}-biosphere"

    source = build.get("source_database")
    if source is not None:
        if source not in bd.databases:
            raise ValueError(
                f"source_database '{source}' is not registered in this project. "
                f"Available databases: {sorted(bd.databases)}."
            )
    else:
        source = f"ecoinvent-{version}-{system_model}"
        if source not in bd.databases:
            logger.info(
                f"No database '{source}' in this project. Importing ecoinvent "
                f"{version} ({system_model}) first; this takes a while and needs "
                f"an ecoinvent licence."
            )
            source = _import_ecoinvent(
                version=version,
                system_model=system_model,
                credentials=_resolve_ecoinvent_credentials(ecoinvent_credentials),
            )

    for candidate in (default_biosphere, "biosphere3"):
        if candidate in bd.databases:
            return source, candidate
    raise ValueError(
        f"No biosphere database found: expected '{default_biosphere}' or "
        f"'biosphere3'. premise needs one to link elementary flows."
    )
```

and give `_import_ecoinvent` its real body:

```python
def _import_ecoinvent(version: str, system_model: str, credentials: tuple[str, str]) -> str:
    """Import an ecoinvent release. The only place bw2io is called."""
    try:
        from bw2io import import_ecoinvent_release
    except ImportError as error:
        raise ImportError(
            'bw2io is needed to import ecoinvent. Install it with: pip install '
            '"bw_timex[premise]"'
        ) from error

    username, password = credentials
    import_ecoinvent_release(
        version=version,
        system_model=system_model,
        username=username,
        password=password,
    )
    return f"ecoinvent-{version}-{system_model}"
```

Replace the `raise NotImplementedError` at the end of `ensure_scenario_databases` with:

```python
    names = {year: vintage_name(filters, year) for year in missing}
    _check_no_collisions(list(names.values()), filters)

    key = _resolve_premise_key(premise_key)
    source_database, biosphere = _resolve_source_database(
        filters, build, ecoinvent_credentials
    )

    raise NotImplementedError  # the premise run is added in Task 5
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_scenario_builder.py -k "TestVintageName or TestOverwriteGuard" -v`
Expected: PASS.

Run: `pytest tests/test_scenario_builder.py -k "TestSourceDatabase" -v`
Expected: `test_missing_ecoinvent_without_credentials_raises` and `test_unregistered_explicit_source_database_raises` PASS; the other three still fail at `NotImplementedError`, completed by Task 5.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/scenario_builder.py tests/test_scenario_builder.py
git commit -m "feat: resolve the premise source database and guard against overwrites"
```

---

### Task 5: Run premise and record what was built

**Files:**
- Modify: `bw_timex/scenario_builder.py`
- Test: `tests/test_scenario_builder.py`

**Interfaces:**
- Consumes: everything from Tasks 2-4.
- Produces: `_run_premise(*, scenarios, source_database, source_version, system_model, biosphere, sectors, names, key) -> None` (keyword-only; every test fake matches this signature), and `ensure_scenario_databases` returning the full name → `datetime` mapping.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scenario_builder.py`:

```python
@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestBuilding:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_only_missing_years_are_built(self, fake_premise):
        write_vintage("ei_2030", 2030)
        ensure_scenario_databases({**SCENARIO, "years": [2020, 2030, 2040]})
        assert len(fake_premise) == 1
        assert [s["year"] for s in fake_premise[0]["scenarios"]] == [2020, 2040]

    def test_premise_scenarios_carry_model_and_pathway(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["scenarios"] == [
            {"model": "remind", "pathway": "SSP2-PkBudg500", "year": 2030}
        ]

    def test_source_version_and_system_model_are_passed(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["source_version"] == "3.10.1"
        assert fake_premise[0]["system_model"] == "cutoff"

    def test_names_line_up_with_scenarios(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise[0]["names"] == [
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030",
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2040",
        ]

    def test_all_sectors_by_default(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["sectors"] is None

    def test_narrowed_sectors_are_passed_through(self, fake_premise):
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "sectors": ["electricity", "steel"]}
        )
        assert fake_premise[0]["sectors"] == ["electricity", "steel"]

    def test_narrowed_sectors_are_recorded_in_metadata(self, fake_premise):
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        )
        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        assert bd.databases[name]["sectors"] == ["electricity"]

    def test_no_sectors_metadata_when_all_sectors(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        assert "sectors" not in bd.databases[name]

    def test_built_and_existing_vintages_are_returned(self, fake_premise):
        write_vintage("ei_2030", 2030)
        result = ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert result == {
            "ei_2030": datetime(2030, 1, 1),
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2040": datetime(2040, 1, 1),
        }

    def test_missing_representative_time_after_write_raises(self, monkeypatch):
        def fake_run_premise(**kwargs):
            for name in kwargs["names"]:
                write_minimal_database(name)  # no metadata: an old premise

        monkeypatch.setattr(
            "bw_timex.scenario_builder._run_premise", fake_run_premise, raising=True
        )
        with pytest.raises(RuntimeError, match="2.4.9.2"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestPremiseNotInstalled:

    def test_import_error_names_the_extra(self, monkeypatch):
        from bw_timex import scenario_builder

        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")
        monkeypatch.setitem(sys.modules, "premise", None)  # forces ImportError
        with pytest.raises(ImportError, match=r'bw_timex\[premise\]'):
            scenario_builder.ensure_scenario_databases({**SCENARIO, "years": [2030]})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_scenario_builder.py -k "TestBuilding or TestPremiseNotInstalled" -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement the seam**

Replace the `_run_premise` stub in `bw_timex/scenario_builder.py`:

```python
def _run_premise(
    *,
    scenarios: list[dict],
    source_database: str,
    source_version: str,
    system_model: str,
    biosphere: str,
    sectors: list[str] | None,
    names: list[str],
    key: str,
) -> None:
    """Build and write one prospective database per scenario.

    The only place premise is called. One `NewDatabase` for all scenarios, not
    one per year: premise caches the extracted source database, so separate
    runs would re-extract ecoinvent every time.
    """
    try:
        from premise import NewDatabase
    except ImportError as error:
        raise ImportError(
            'premise is needed to build background databases. Install it with: '
            'pip install "bw_timex[premise]"'
        ) from error

    ndb = NewDatabase(
        scenarios=scenarios,
        source_db=source_database,
        source_version=source_version,
        system_model=system_model,
        biosphere_name=biosphere,
        key=key,
    )
    if sectors:
        ndb.update(sectors)
    else:
        ndb.update()
    ndb.write_db_to_brightway(name=names)
```

- [ ] **Step 4: Implement the build path**

Replace the trailing `raise NotImplementedError` in `ensure_scenario_databases` with:

```python
    sectors = build.get("sectors")
    logger.info(
        f"Building {len(missing)} background database(s) for year(s) {missing} with "
        f"premise ({filters['iam_model']}, {filters['pathway']}, "
        f"{'all sectors' if not sectors else ', '.join(sectors)}). Each is a full "
        f"copy of ecoinvent, so expect tens of minutes and roughly 2-4 GB per year."
    )

    _run_premise(
        scenarios=[
            {
                "model": filters["iam_model"],
                "pathway": filters["pathway"],
                "year": year,
            }
            for year in missing
        ],
        source_database=source_database,
        source_version=filters["ecoinvent_version"],
        system_model=filters["system_model"],
        biosphere=biosphere,
        sectors=sectors,
        names=[names[year] for year in missing],
        key=key,
    )

    for year in missing:
        name = names[year]
        if REPRESENTATIVE_TIME not in bd.databases.get(name, {}):
            raise RuntimeError(
                f"premise wrote '{name}' without `{REPRESENTATIVE_TIME}` metadata, so "
                f"`TimexLCA` cannot tell what point in time it represents. This "
                f"metadata is written by premise >= 2.4.9.2; check your installed "
                f"version, or set it yourself with `bw_timex.set_database_metadata`."
            )
        if sectors:
            # premise does not record which sectors were updated, and two runs of
            # the same pathway with different sectors would otherwise look identical
            # to the scenario filter.
            set_database_metadata(name, sectors=list(sectors))

    logger.info(f"Built {len(missing)} background database(s).")

    resolved = {existing[year]: datetime(year, 1, 1) for year in years if year in existing}
    resolved.update({names[year]: datetime(year, 1, 1) for year in missing})
    return resolved
```

Add `set_database_metadata` to the `.database_metadata` import at the top of the module.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_scenario_builder.py -v`
Expected: PASS — every class in the file, including `TestCredentials` and `TestSourceDatabase` from Tasks 3 and 4.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/scenario_builder.py tests/test_scenario_builder.py
git commit -m "feat: build missing background vintages with premise"
```

---

### Task 6: Wire it into TimexLCA

**Files:**
- Modify: `bw_timex/timex_lca.py:129-201` (signature, docstring, `__init__` body) and `bw_timex/timex_lca.py:283-352` (`_resolve_database_dates`)
- Modify: `bw_timex/validation.py` (`TimexLCAInputs`)
- Modify: `bw_timex/__init__.py`
- Test: `tests/test_scenario_builder.py`

**Interfaces:**
- Consumes: `ensure_scenario_databases` (Task 5).
- Produces: `TimexLCA(demand, method, database_dates=None, scenario=None, create_missing=False, premise_key=None, ecoinvent_credentials=None, use_global_lci_cache=True)`; `bw_timex.ensure_scenario_databases`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scenario_builder.py`:

```python
@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAIntegration:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_public_export(self):
        import bw_timex

        assert bw_timex.ensure_scenario_databases is ensure_scenario_databases

    def test_create_missing_builds_and_resolves(self, fake_premise):
        from bw_timex import TimexLCA

        tlca = TimexLCA(
            demand={("foreground", "A"): 1},
            method=("GWP", "example"),
            scenario={**SCENARIO, "years": [2030]},
            create_missing=True,
        )
        assert (
            tlca.database_dates["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"]
            == datetime(2030, 1, 1)
        )
        assert tlca.database_dates["foreground"] == "dynamic"

    def test_default_does_not_build(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                scenario={**SCENARIO, "years": [2030]},
            )
        assert fake_premise == []

    def test_error_without_create_missing_mentions_it(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="create_missing"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                scenario={**SCENARIO, "years": [2030]},
            )

    def test_create_missing_with_database_dates_raises(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="database_dates"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                database_dates={"foreground": "dynamic"},
                create_missing=True,
            )
        assert fake_premise == []

    def test_create_missing_without_scenario_raises(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="scenario"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                create_missing=True,
            )
        assert fake_premise == []
```

The `temporal_grouping_db_monthly` fixture registers a `foreground` database with a process `A` and the method `("GWP", "example")`; check the fixture and use whatever demand key and method it actually provides, rather than inventing one.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_scenario_builder.py -k "TestTimexLCAIntegration" -v`
Expected: FAIL — `AttributeError: module 'bw_timex' has no attribute 'ensure_scenario_databases'` and `TypeError: __init__() got an unexpected keyword argument 'create_missing'`.

- [ ] **Step 3: Extend the validation model**

In `bw_timex/validation.py`, add to `TimexLCAInputs`:

```python
    create_missing: bool = False
    premise_key: Optional[str] = None
    ecoinvent_credentials: Optional[tuple] = None
```

and a validator on the model:

```python
    @model_validator(mode="after")
    def validate_create_missing(self) -> "TimexLCAInputs":
        if not self.create_missing:
            return self
        if self.database_dates is not None:
            raise ValueError(
                "`create_missing` builds the background databases a `scenario` "
                "describes, and only applies when `database_dates` is not given. "
                "Pass one or the other."
            )
        if self.scenario is None:
            raise ValueError(
                "`create_missing=True` needs a `scenario` describing what to build, "
                "e.g. scenario={'iam_model': 'remind', 'pathway': 'SSP2-PkBudg500', "
                "'system_model': 'cutoff', 'ecoinvent_version': '3.10.1', "
                "'years': [2030, 2040]}."
            )
        if self.ecoinvent_credentials is not None and len(self.ecoinvent_credentials) != 2:
            raise ValueError(
                "`ecoinvent_credentials` must be a (username, password) tuple."
            )
        return self
```

- [ ] **Step 4: Plumb the arguments through TimexLCA**

In `bw_timex/timex_lca.py`, extend the signature:

```python
    def __init__(
        self,
        demand: dict,
        method: tuple,
        database_dates: dict = None,
        scenario: dict = None,
        create_missing: bool = False,
        premise_key: str = None,
        ecoinvent_credentials: tuple = None,
        use_global_lci_cache: bool = True,
    ) -> None:
```

Add to the docstring's Parameters section, after the `scenario` entry:

```
        create_missing : bool, optional
                If True, background databases the `scenario` names but that this
                project does not hold yet are built with `premise`, and ecoinvent
                is imported first if it is missing. The `scenario` then also needs
                a `years` list (and may narrow `sectors` or name a
                `source_database`). Needs the optional dependency:
                `pip install "bw_timex[premise]"`. Building takes tens of minutes
                and roughly 2-4 GB per year. Default is False, which raises
                instead of building. Cannot be combined with `database_dates`.
        premise_key : str, optional
                premise decryption key, used only when building. Falls back to the
                environment variable `PREMISE_KEY`.
        ecoinvent_credentials : tuple, optional
                `(username, password)`, used only when ecoinvent itself has to be
                imported. Falls back to the environment variables
                `ECOINVENT_USERNAME` and `ECOINVENT_PASSWORD`.
```

Replace the assignment block at the start of `__init__` (currently `self.demand = demand` through the `TimexLCAInputs(...)` call) with validation first, then the build, then resolution:

```python
        self.demand = demand
        self.method = method
        self.scenario = scenario

        TimexLCAInputs(
            demand=demand,
            method=method,
            database_dates=database_dates,
            scenario=scenario,
            create_missing=create_missing,
            premise_key=premise_key,
            ecoinvent_credentials=ecoinvent_credentials,
        )

        if create_missing:
            from .scenario_builder import ensure_scenario_databases

            ensure_scenario_databases(
                scenario,
                premise_key=premise_key,
                ecoinvent_credentials=ecoinvent_credentials,
            )

        self.database_dates = self._resolve_database_dates(
            demand=demand, database_dates=database_dates, scenario=scenario
        )
```

Note the reordering: `TimexLCAInputs` now runs *before* `_resolve_database_dates`, because a bad `create_missing` combination must be rejected before anything is built. The old call passed `self.database_dates`; the new one passes the raw `database_dates` argument, which is what the model validates (`validate_demand_in_dynamic_databases` short-circuits when it is `None`, as it does today for the metadata path).

In `_resolve_database_dates`, extend the "matched no database" error with a line about building:

```python
            raise ValueError(
                f"scenario={scenario!r} matched no database in this project. "
                f"Values actually declared for its key(s) by this project's "
                f"databases: {details}. Check for a typo in the filter, or pass "
                f"`create_missing=True` (with a `years` list in the scenario) to "
                f"build the databases with premise."
            )
```

- [ ] **Step 5: Export the helper**

In `bw_timex/__init__.py`, add `from .scenario_builder import ensure_scenario_databases` next to the `database_metadata` import, and `"ensure_scenario_databases"` to `__all__` in the `utils` block (alphabetically, before `get_exchange`).

`scenario_builder` imports no third-party library at module level, so this does not make premise a hard dependency.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pytest tests/test_scenario_builder.py -v`
Expected: PASS.

Run: `pytest tests/test_public_api.py tests/test_timex_lca.py tests/test_database_metadata.py -v`
Expected: PASS. `test_public_api.py` may assert on `__all__`; update it there if it enumerates the exports.

- [ ] **Step 7: Run the whole suite**

Run: `pytest -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/timex_lca.py bw_timex/validation.py bw_timex/__init__.py tests/
git commit -m "feat: let TimexLCA build the background databases a scenario names"
```

---

### Task 7: Packaging and documentation

**Files:**
- Modify: `pyproject.toml`
- Modify: `docs/content/installation.md`
- Modify: `docs/content/getting_started/quickstart.md`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: the finished feature.
- Produces: the `bw_timex[premise]` extra.

- [ ] **Step 1: Add the optional dependency**

In `pyproject.toml`, under `[project.optional-dependencies]`, next to the existing `solvers` entry:

```toml
premise = ["premise[bw25]>=2.4.9.2"]
```

- [ ] **Step 2: Verify the extra resolves**

Run: `python -m pip install --dry-run ".[premise]" 2>&1 | tail -20`
Expected: a resolution that includes `premise>=2.4.9.2` and a `bw2data` ≥ 4.6. If the resolver reports a conflict with bw_timex's `bw2data>=4.6`, stop and report it rather than loosening the pin.

- [ ] **Step 3: Document installation**

In `docs/content/installation.md`, after the existing install instructions:

```markdown
## Building background databases automatically

`TimexLCA` can build the prospective background databases a scenario names, using
[premise](https://github.com/polca/premise). That needs the optional extra:

```bash
pip install "bw_timex[premise]"
```

and credentials, which `bw_timex` reads from the environment unless you pass them:

| variable | needed for |
|---|---|
| `PREMISE_KEY` | decrypting premise's bundled IAM scenarios |
| `ECOINVENT_USERNAME`, `ECOINVENT_PASSWORD` | importing ecoinvent, if the project has none yet |
```

- [ ] **Step 4: Document usage**

In `docs/content/getting_started/quickstart.md`, next to the existing `scenario` documentation:

````markdown
If the project does not hold the scenario's databases yet, `bw_timex` can build them
with premise instead of raising. Add the years to the scenario and pass
`create_missing=True`:

```python
tlca = TimexLCA(
    demand={("foreground", "ev"): 1},
    method=("EF v3.1", "climate change", "global warming potential (GWP100)"),
    scenario={
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.10.1",
        "years": [2020, 2030, 2040],
    },
    create_missing=True,
)
```

Only missing years are built, so running this again builds nothing. ecoinvent is
imported first if the project has none. Each vintage is a full copy of ecoinvent:
expect tens of minutes and roughly 2-4 GB per year. Two optional scenario keys tune
the build: `sectors` narrows what premise updates (all sectors by default), and
`source_database` names the ecoinvent to build from, if it is not the one
`import_ecoinvent_release` writes.
````

- [ ] **Step 5: Changelog**

Add one bullet at the top of the current unreleased section of `CHANGES.md`:

```markdown
* Added `TimexLCA(scenario={..., "years": [...]}, create_missing=True)`, which builds background databases the scenario names but the project does not hold, using premise (optional extra: `pip install "bw_timex[premise]"`). Only missing years are built, ecoinvent is imported first if absent, and credentials are read from `PREMISE_KEY` / `ECOINVENT_USERNAME` / `ECOINVENT_PASSWORD` unless passed explicitly. Also available standalone as `bw_timex.ensure_scenario_databases`
```

- [ ] **Step 6: Verify the docs build**

Run: `python docs/convert_notebooks.py --help 2>/dev/null; ls docs`
Expected: no error. If the project has a docs build command in `zensical.toml` or CI, run that instead and expect a clean build.

- [ ] **Step 7: Run the whole suite once more**

Run: `pytest -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml docs CHANGES.md
git commit -m "docs: document building background databases with premise"
```

---

## Manual verification (not part of CI)

Once, by hand, in a project with an ecoinvent licence and a premise key:

```python
import bw2data as bd
from bw_timex import ensure_scenario_databases

bd.projects.set_current("premise_autocreate_check")
ensure_scenario_databases(
    {
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.10.1",
        "years": [2030],
        "sectors": ["electricity"],   # keep the check short
    }
)
print(bd.databases["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"])
```

Confirm: the database exists, its metadata carries `representative_time`,
`iam_model`, `pathway`, `system_model`, `ecoinvent_version` from premise plus
`sectors` from us, and a second call to the same function builds nothing.
