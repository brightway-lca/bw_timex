# Prospective Characterization Scenario Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a prospective LCIA metric take its Watanabe scenario from the premise background scenario it is run against, so `TimexLCA.compare()` can vary both together, and make both scenario catalogues discoverable.

**Architecture:** `dynamic_characterization` gains a per-call scoped scenario (`characterize(scenario=...)`), which removes the module-global that blocks per-row variation. `bw_timex` gains one public table bridging premise `(iam_model, pathway)` to Watanabe `(iam, ssp, rcp)`, a `characterization_scenario` LCIA setting that overrides it, and an `available_scenarios()` listing that shows both catalogues side by side. Nothing existing is renamed.

**Tech Stack:** Python 3.10+, pandas, pydantic v2, pytest, loguru, bw2data/bw2calc, premise (optional extra), Jupyter notebooks + zensical docs.

**Spec:** `docs/superpowers/specs/2026-09-22-prospective-scenario-integration-design.md` (in the `bw_timex` repo)

## Global Constraints

- **Two repositories.** `/Users/timodiepers/Documents/Coding/dynamic_characterization` (Tasks 1-3) and `/Users/timodiepers/Documents/Coding/bw_timex` (Tasks 4-12). `bw_timex` depends on `dynamic_characterization`, never the reverse. Do Part A first.
- **Python tooling is `uv`.** Run tests with `uv run pytest`, never bare `pytest`, `pip` or `conda`.
- **Work on feature branches.** `bw_timex` is already on `feature/prospective-scenario-integration`. Create `feature/per-call-prospective-scenario` in `dynamic_characterization` before Task 1.
- **No commit attribution lines.** Do not add `Co-Authored-By:` or `Claude-Session` trailers to any commit.
- **Nothing existing is renamed or removed.** `set_scenario()`, the metric strings `"pGWP"` / `"pGTP"` / `"prospective_radiative_forcing"`, and the `TimexLCASettings.scenario` key names (`iam_model`, `pathway`, `system_model`, `ecoinvent_version`, `years`, `sectors`) all keep working with unchanged meaning.
- **The Watanabe pairings are fixed:** IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4, REMIND-SSP5. RCPs `"2.6"`, `"4.5"`, `"6.0"`, `"8.5"`, not all available for every IAM. The authoritative set is `dynamic_characterization.prospective.VALID_SCENARIOS`.
- **Never substitute a near-miss pairing.** If `(iam_model, pathway)` has no exact entry, raise. Do not fall back to "same IAM, different SSP" or "same SSP, different IAM".
- **During Part B development**, point `bw_timex` at the local `dynamic_characterization` checkout: `uv pip install -e /Users/timodiepers/Documents/Coding/dynamic_characterization`.

---

# Part A — `dynamic_characterization`

### Task 1: Per-call scenario scoping

**Files:**
- Modify: `dynamic_characterization/prospective/config.py` (add `scenario_context`)
- Modify: `dynamic_characterization/prospective/__init__.py` (export it)
- Modify: `dynamic_characterization/dynamic_characterization.py:54-66` (add `scenario` parameter), body at line ~125 onwards (wrap)
- Test: `tests/test_prospective_scenario_scoping.py` (create)

**Interfaces:**
- Consumes: `get_scenario`, `set_scenario`, `reset_scenario`, `VALID_SCENARIOS` from `dynamic_characterization.prospective.config`
- Produces:
  - `dynamic_characterization.prospective.scenario_context(scenario: dict | None) -> ContextManager[None]`
  - `dynamic_characterization.characterize(..., scenario: dict | None = None)` — keyword-only in effect (appended last); `scenario` is a dict with keys `iam`, `ssp`, `rcp`

- [ ] **Step 1: Write the failing test**

Create `tests/test_prospective_scenario_scoping.py`:

```python
"""The prospective scenario can be scoped to one call instead of a session."""

import pytest

from dynamic_characterization.prospective import (
    get_scenario,
    reset_scenario,
    scenario_context,
    set_scenario,
)

IMAGE = {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}
MESSAGE = {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}


@pytest.fixture(autouse=True)
def clean_scenario():
    reset_scenario()
    yield
    reset_scenario()


def test_context_applies_scenario_inside_block():
    with scenario_context(IMAGE):
        assert get_scenario() == IMAGE


def test_context_restores_previous_scenario():
    set_scenario(**MESSAGE)
    with scenario_context(IMAGE):
        assert get_scenario() == IMAGE
    assert get_scenario() == MESSAGE


def test_context_restores_unset_state():
    with scenario_context(IMAGE):
        pass
    with pytest.raises(RuntimeError):
        get_scenario()


def test_context_restores_on_exception():
    set_scenario(**MESSAGE)
    with pytest.raises(ValueError):
        with scenario_context(IMAGE):
            raise ValueError("boom")
    assert get_scenario() == MESSAGE


def test_context_with_none_is_a_no_op():
    set_scenario(**MESSAGE)
    with scenario_context(None):
        assert get_scenario() == MESSAGE
    assert get_scenario() == MESSAGE


def test_context_validates_the_scenario():
    with pytest.raises(ValueError):
        with scenario_context({"iam": "REMIND", "ssp": "SSP2", "rcp": "2.6"}):
            pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/timodiepers/Documents/Coding/dynamic_characterization && uv run pytest tests/test_prospective_scenario_scoping.py -v`
Expected: FAIL — `ImportError: cannot import name 'scenario_context'`

- [ ] **Step 3: Implement `scenario_context`**

Append to `dynamic_characterization/prospective/config.py` (the `contextmanager` import goes at the top of the file):

```python
from contextlib import contextmanager


@contextmanager
def scenario_context(scenario: Optional[Dict[str, str]]):
    """
    Apply a scenario for the duration of a block, then restore what was set before.

    Parameters
    ----------
    scenario : dict or None
        Scenario with keys `iam`, `ssp`, `rcp`, validated like `set_scenario`.
        `None` is a no-op, so callers can pass an optional scenario straight
        through without branching.

    Notes
    -----
    This overrides module-level state, so it is not thread-safe: two threads
    characterizing under different scenarios at the same time will interfere.
    The module's scenario has always been process-wide; this only narrows when
    a given value applies.
    """
    global _current_scenario

    if scenario is None:
        yield
        return

    previous = _current_scenario
    set_scenario(**scenario)
    try:
        yield
    finally:
        _current_scenario = previous
```

Add `"scenario_context"` to the imports and to `__all__` in
`dynamic_characterization/prospective/__init__.py`:

```python
from .config import (
    VALID_SCENARIOS,
    get_scenario,
    reset_scenario,
    scenario_context,
    set_scenario,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_prospective_scenario_scoping.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Write the failing test for `characterize(scenario=...)`**

Append to `tests/test_prospective_scenario_scoping.py`. It reuses the existing
fixtures — check `tests/conftest.py` for the project fixture the other
prospective tests use (`test_prospective.py` shows the pattern) and use the same
one, named `electric_vehicle_lci` below only as a placeholder if the real
fixture has another name; the assertions do not depend on which inventory it is.

```python
import pandas as pd

from dynamic_characterization import characterize


def _inventory(flow_id):
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2030-01-01"]),
            "amount": [1.0],
            "flow": [flow_id],
            "activity": [0],
        }
    )


def test_characterize_scenario_argument_matches_session_scenario(co2_flow_id, method):
    set_scenario(**IMAGE)
    from_session = characterize(
        _inventory(co2_flow_id), metric="pGWP", base_lcia_method=method
    )
    reset_scenario()

    from_argument = characterize(
        _inventory(co2_flow_id), metric="pGWP", base_lcia_method=method, scenario=IMAGE
    )

    pd.testing.assert_frame_equal(from_session, from_argument)


def test_characterize_scenario_argument_does_not_leak(co2_flow_id, method):
    set_scenario(**MESSAGE)
    characterize(
        _inventory(co2_flow_id), metric="pGWP", base_lcia_method=method, scenario=IMAGE
    )
    assert get_scenario() == MESSAGE


def test_two_scenarios_in_one_process_differ(ch4_flow_id, method):
    low = characterize(
        _inventory(ch4_flow_id),
        metric="prospective_radiative_forcing",
        base_lcia_method=method,
        scenario=IMAGE,
    )
    high = characterize(
        _inventory(ch4_flow_id),
        metric="prospective_radiative_forcing",
        base_lcia_method=method,
        scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
    )
    assert low["amount"].sum() != high["amount"].sum()


def test_pgtp_identity_dispatch_survives_scoping(co2_flow_id, method):
    """CO2's pGTP is 1.0 by definition - proves the AGTP branch still matched."""
    result = characterize(
        _inventory(co2_flow_id), metric="pGTP", base_lcia_method=method, scenario=IMAGE
    )
    assert result["amount"].sum() == pytest.approx(1.0, rel=1e-6)


def test_prospective_metric_without_any_scenario_still_raises(co2_flow_id, method):
    with pytest.raises(RuntimeError):
        characterize(_inventory(co2_flow_id), metric="pGWP", base_lcia_method=method)
```

Before running, replace `co2_flow_id`, `ch4_flow_id` and `method` with the
fixtures the existing prospective tests use — read
`tests/test_prospective.py` and `tests/conftest.py` first and follow whatever
they do to get a biosphere flow id and an LCIA method.

- [ ] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/test_prospective_scenario_scoping.py -v`
Expected: FAIL — `characterize() got an unexpected keyword argument 'scenario'`

- [ ] **Step 7: Add the parameter and wrap the body**

In `dynamic_characterization/dynamic_characterization.py`, add the parameter to
`characterize` (after `characterize_biogenic_uptake`, line 65):

```python
    characterize_biogenic_uptake: bool = True,
    scenario: Dict[str, str] = None,
) -> pd.DataFrame:
```

Document it in the docstring's Parameters section:

```
    scenario : dict, optional
        Prospective scenario for this call only, with keys `iam`, `ssp` and
        `rcp`, e.g. `{"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}`. Applies for
        the duration of the call and is then restored, so different calls in one
        process can use different scenarios. Only affects the prospective
        metrics. Default is None, which uses whatever
        `dynamic_characterization.prospective.set_scenario()` last set.
```

Rename the existing body to a private function and make `characterize` a thin
wrapper, which keeps the diff small and the scoping impossible to bypass:

```python
def characterize(
    ...,
    scenario: Dict[str, str] = None,
) -> pd.DataFrame:
    """<docstring stays here>"""
    with scenario_context(scenario):
        return _characterize(
            dynamic_inventory_df=dynamic_inventory_df,
            metric=metric,
            characterization_functions=characterization_functions,
            base_lcia_method=base_lcia_method,
            time_horizon=time_horizon,
            fixed_time_horizon=fixed_time_horizon,
            time_horizon_start=time_horizon_start,
            characterization_function_co2=characterization_function_co2,
            time_varying_re=time_varying_re,
            fallback_to_ipcc=fallback_to_ipcc,
            characterize_biogenic_uptake=characterize_biogenic_uptake,
        )
```

`_characterize` is the current function body verbatim, with the same signature
minus `scenario`. Import `scenario_context` at the top of the module:

```python
from dynamic_characterization.prospective import scenario_context
```

- [ ] **Step 8: Run the new tests**

Run: `uv run pytest tests/test_prospective_scenario_scoping.py -v`
Expected: PASS

- [ ] **Step 9: Run the whole suite for regressions**

Run: `uv run pytest -q`
Expected: all pass. The wrapper changes no defaults, so any failure here is a
real regression — fix it before committing.

- [ ] **Step 10: Commit**

```bash
git add dynamic_characterization/prospective/config.py \
        dynamic_characterization/prospective/__init__.py \
        dynamic_characterization/dynamic_characterization.py \
        tests/test_prospective_scenario_scoping.py
git commit -m "feat: scope the prospective scenario to a single characterize call"
```

---

### Task 2: Deduplicate out-of-bounds emission-year warnings

**Files:**
- Modify: `dynamic_characterization/prospective/radiative_forcing.py:45-68` (`_get_year_index`)
- Test: `tests/test_prospective_scenario_scoping.py` (append)

**Interfaces:**
- Consumes: nothing new
- Produces: `_get_year_index` unchanged in signature and return value; only its warning behaviour changes

- [ ] **Step 1: Write the failing test**

Append to `tests/test_prospective_scenario_scoping.py`:

```python
import warnings

import numpy as np

from dynamic_characterization.prospective.radiative_forcing import _get_year_index


def test_out_of_bounds_year_warns_once_per_bound():
    years = np.arange(2020, 2151)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(50):
            _get_year_index(1990, years)
    assert len(caught) == 1
    assert "clamping to 2020" in str(caught[0].message)


def test_both_bounds_warn_separately():
    years = np.arange(2020, 2151)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(10):
            _get_year_index(1990, years)
            _get_year_index(2200, years)
    assert len(caught) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_prospective_scenario_scoping.py -k out_of_bounds -v`
Expected: FAIL — 50 warnings caught, not 1

- [ ] **Step 3: Implement deduplication**

Replace `_get_year_index` in `dynamic_characterization/prospective/radiative_forcing.py`:

```python
# Bounds already warned about, so characterizing a large inventory does not emit
# one identical warning per row. Keyed on (bound_year, direction).
_WARNED_BOUNDS: set = set()


def _reset_bound_warnings() -> None:
    """Forget which bounds have been warned about. Used by the tests."""
    _WARNED_BOUNDS.clear()


def _get_year_index(emission_year: int, years: np.ndarray) -> int:
    """
    Get index for emission year in RE data, with clamping and warnings.

    The bounds are taken from the RE data itself (currently 2020-2150), so they
    follow the data instead of the narrower range reported in the paper tables.

    Each bound is warned about once per process: a dynamic inventory can hold
    many thousands of rows outside the data range, and one warning per row
    drowns everything else.
    """
    min_year, max_year = int(years[0]), int(years[-1])

    if emission_year < min_year:
        if ("below", min_year) not in _WARNED_BOUNDS:
            _WARNED_BOUNDS.add(("below", min_year))
            warnings.warn(
                f"Emission year {emission_year} < {min_year}, clamping to {min_year}. "
                "Further emissions before this bound are clamped without warning."
            )
        emission_year = min_year
    elif emission_year > max_year:
        if ("above", max_year) not in _WARNED_BOUNDS:
            _WARNED_BOUNDS.add(("above", max_year))
            warnings.warn(
                f"Emission year {emission_year} > {max_year}, clamping to {max_year}. "
                "Further emissions after this bound are clamped without warning."
            )
        emission_year = max_year

    idx = np.searchsorted(years, emission_year)
    return idx
```

Add `_reset_bound_warnings()` to the autouse fixture in the test file so tests
do not suppress each other's warnings:

```python
@pytest.fixture(autouse=True)
def clean_scenario():
    reset_scenario()
    _reset_bound_warnings()
    yield
    reset_scenario()
    _reset_bound_warnings()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_prospective_scenario_scoping.py -v`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass. If an existing test asserted on warning counts, update it to
the new once-per-bound behaviour.

- [ ] **Step 6: Commit**

```bash
git add dynamic_characterization/prospective/radiative_forcing.py \
        tests/test_prospective_scenario_scoping.py
git commit -m "fix: warn once per clamped emission-year bound, not once per row"
```

---

### Task 3: `dynamic_characterization` docs

**Files:**
- Modify: `README.md` (the prospective section)
- Modify: `dynamic_characterization/__init__.py:11-12` (module docstring)
- Modify: `dynamic_characterization/prospective/config.py:44-56` (`NO_SCENARIO_MESSAGE`)
- Modify: the prospective notebook under `notebooks/` (find it with `grep -rl set_scenario notebooks`)

**Interfaces:**
- Consumes: `characterize(scenario=...)`, `scenario_context` from Task 1
- Produces: nothing code depends on

- [ ] **Step 1: Update the package docstring**

In `dynamic_characterization/__init__.py`, replace the prospective note:

```python
"""
...
For prospective metrics (pGWP, pGTP), choose a scenario. Per call:
    characterize(..., metric="pGWP", scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"})

Or once for the session:
    import dynamic_characterization.prospective as prospective
    prospective.set_scenario(iam="IMAGE", ssp="SSP1", rcp="2.6")
"""
```

- [ ] **Step 2: Update `NO_SCENARIO_MESSAGE`**

In `dynamic_characterization/prospective/config.py`, lead the message with the
per-call form, keeping the rest of the text as it is:

```python
NO_SCENARIO_MESSAGE = """No scenario set.

The prospective characterization factors (Watanabe et al. 2026) used for the metrics
"pGWP", "pGTP" and "prospective_radiative_forcing" depend on a future scenario, so you
have to choose one before calculating them. Either per call:

    characterize(..., scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"})

or once for the whole session:

    from dynamic_characterization.prospective import set_scenario
    set_scenario(iam="IMAGE", ssp="SSP1", rcp="2.6")

If you are calling this through bw_timex, the scenario normally comes from the
background scenario of your TimexLCA - see bw_timex.available_scenarios().

Each IAM comes with one SSP: IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4,
REMIND-SSP5. The available RCPs are "2.6", "4.5", "6.0" and "8.5", but not for every
IAM - see dynamic_characterization.prospective.VALID_SCENARIOS for the full list."""
```

- [ ] **Step 3: Update the README and the notebook**

In `README.md`, the prospective section leads with `characterize(scenario=...)`
and presents `set_scenario` as the session default. In the notebook found by
`grep -rl set_scenario notebooks`, add one cell after the existing
`set_scenario` cell showing the same characterization run with `scenario=`
passed directly, with a markdown cell saying that this is what lets one process
use several scenarios.

- [ ] **Step 4: Verify nothing broke**

Run: `uv run pytest -q`
Expected: all pass. (`test_prospective.py` may assert on the message text — if
so, update the assertion to match the new wording.)

- [ ] **Step 5: Commit**

```bash
git add README.md dynamic_characterization/__init__.py \
        dynamic_characterization/prospective/config.py notebooks/
git commit -m "docs: lead with the per-call prospective scenario"
```

---

# Part B — `bw_timex`

### Task 4: `PROSPECTIVE_SCENARIO_MAP` and scenario resolution

**Files:**
- Create: `bw_timex/prospective_scenarios.py`
- Test: `tests/test_prospective_scenarios.py` (create)

**Interfaces:**
- Consumes: `dynamic_characterization.prospective.VALID_SCENARIOS`
- Produces:
  - `PROSPECTIVE_SCENARIO_MAP: dict[tuple[str, str], dict[str, str]]` — keys are `(iam_model_lowercase, pathway)`, values are `{"iam": ..., "ssp": ..., "rcp": ...}`
  - `PROSPECTIVE_METRICS: frozenset[str]` — `{"pGWP", "pGTP", "prospective_radiative_forcing"}`
  - `lookup_prospective_scenario(scenario: dict | None) -> dict | None` — table lookup, `None` when the scenario is `None`/empty or has no entry
  - `resolve_characterization_scenario(metric: str, characterization_scenario: dict | None, scenario: dict | None) -> dict | None` — full precedence, raises `ValueError` when a prospective metric cannot be resolved

- [ ] **Step 1: Write the failing test**

Create `tests/test_prospective_scenarios.py`:

```python
"""The bridge between premise background scenarios and Watanabe CF scenarios."""

import pytest

from bw_timex.prospective_scenarios import (
    PROSPECTIVE_SCENARIO_MAP,
    lookup_prospective_scenario,
    resolve_characterization_scenario,
)

IMAGE_LOW = {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}


def test_every_table_entry_is_a_valid_watanabe_scenario():
    from dynamic_characterization.prospective import VALID_SCENARIOS

    for key, value in PROSPECTIVE_SCENARIO_MAP.items():
        triple = (value["iam"], value["ssp"], value["rcp"])
        assert triple in VALID_SCENARIOS, f"{key} maps to invalid {triple}"


def test_lookup_hits_a_known_pairing():
    assert lookup_prospective_scenario(
        {"iam_model": "image", "pathway": "SSP1-RCP26"}
    ) == IMAGE_LOW


def test_lookup_is_case_insensitive_on_iam_model():
    assert lookup_prospective_scenario(
        {"iam_model": "IMAGE", "pathway": "SSP1-RCP26"}
    ) == IMAGE_LOW


def test_lookup_ignores_extra_scenario_keys():
    assert lookup_prospective_scenario(
        {
            "iam_model": "image",
            "pathway": "SSP1-RCP26",
            "system_model": "cutoff",
            "ecoinvent_version": "3.10.1",
            "years": [2020, 2030],
        }
    ) == IMAGE_LOW


def test_lookup_misses_a_pairing_without_an_exact_counterpart():
    assert lookup_prospective_scenario(
        {"iam_model": "remind", "pathway": "SSP2-PkBudg500"}
    ) is None


def test_lookup_of_no_scenario_is_none():
    assert lookup_prospective_scenario(None) is None
    assert lookup_prospective_scenario({}) is None


def test_lookup_returns_a_copy():
    first = lookup_prospective_scenario({"iam_model": "image", "pathway": "SSP1-RCP26"})
    first["rcp"] = "8.5"
    assert lookup_prospective_scenario(
        {"iam_model": "image", "pathway": "SSP1-RCP26"}
    ) == IMAGE_LOW


def test_non_prospective_metric_resolves_to_none():
    assert resolve_characterization_scenario(
        metric="GWP",
        characterization_scenario=None,
        scenario={"iam_model": "remind", "pathway": "SSP2-PkBudg500"},
    ) is None


def test_explicit_scenario_wins_over_the_table():
    explicit = {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario=explicit,
        scenario={"iam_model": "image", "pathway": "SSP1-RCP26"},
    ) == explicit


def test_table_is_used_when_nothing_explicit_is_given():
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario=None,
        scenario={"iam_model": "image", "pathway": "SSP1-RCP26"},
    ) == IMAGE_LOW


def test_session_default_is_used_below_the_table():
    from dynamic_characterization.prospective import reset_scenario, set_scenario

    set_scenario(iam="MESSAGE", ssp="SSP2", rcp="4.5")
    try:
        # table wins over the session default
        assert resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "image", "pathway": "SSP1-RCP26"},
        ) == IMAGE_LOW
        # session default is used when there is nothing to derive from
        assert resolve_characterization_scenario(
            metric="pGWP", characterization_scenario=None, scenario=None
        ) == {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}
    finally:
        reset_scenario()


def test_unmappable_pairing_raises_with_the_override_spelled_out():
    from dynamic_characterization.prospective import reset_scenario

    reset_scenario()
    with pytest.raises(ValueError) as error:
        resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "remind", "pathway": "SSP2-PkBudg500"},
        )
    message = str(error.value)
    assert "remind" in message and "SSP2-PkBudg500" in message
    assert "characterization_scenario" in message
    assert "available_scenarios" in message
    assert "REMIND-SSP5" in message


def test_no_background_scenario_at_all_raises():
    from dynamic_characterization.prospective import reset_scenario

    reset_scenario()
    with pytest.raises(ValueError) as error:
        resolve_characterization_scenario(
            metric="pGTP", characterization_scenario=None, scenario=None
        )
    assert "characterization_scenario" in str(error.value)


def test_explicit_scenario_is_validated():
    with pytest.raises(ValueError):
        resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario={"iam": "REMIND", "ssp": "SSP2", "rcp": "2.6"},
            scenario=None,
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/timodiepers/Documents/Coding/bw_timex && uv run pytest tests/test_prospective_scenarios.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bw_timex.prospective_scenarios'`

- [ ] **Step 3: Implement the module**

Create `bw_timex/prospective_scenarios.py`:

```python
"""Bridging premise background scenarios to prospective characterization scenarios.

The two packages describe a future differently. premise labels a database with
`iam_model` and `pathway` (`"image"`, `"SSP1-RCP26"`); the prospective
characterization factors of Watanabe et al. (2026) are indexed by `iam`, `ssp`
and `rcp` (`"IMAGE"`, `"SSP1"`, `"2.6"`). This module holds the one table that
relates them, and the rule for deciding which characterization scenario a run
uses.

The table has an entry only where the IAM and the SSP line up exactly. Watanabe
et al. pair each IAM with a single SSP - IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3,
GCAM4-SSP4, REMIND-SSP5 - and premise does not. A premise REMIND-SSP2 database
has no counterpart there, and bw_timex will not substitute REMIND-SSP5 (wrong
socioeconomic story) or MESSAGE-SSP2 (wrong model) on the user's behalf: that
choice belongs in the study, not in a lookup nobody reads.
"""

from typing import Dict, Optional, Tuple

from dynamic_characterization.prospective import VALID_SCENARIOS, get_scenario
from loguru import logger

#: Metrics that need a prospective characterization scenario.
PROSPECTIVE_METRICS = frozenset(
    {"pGWP", "pGTP", "prospective_radiative_forcing"}
)

#: premise `(iam_model, pathway)` -> the Watanabe scenario to characterize with.
#:
#: Keys use a lowercase `iam_model`; lookups lowercase before matching. Entries
#: exist only for exact IAM+SSP pairings, so this table is deliberately much
#: shorter than premise's scenario catalogue - see `available_scenarios()` for
#: both sides at once.
#:
#: Climate targets map to the closest RCP available for that IAM in Watanabe et
#: al.: the RCP-named premise pathways map to their own RCP, `Base` to the
#: highest available, and the carbon-budget pathways to the RCP whose forcing
#: level they correspond to.
PROSPECTIVE_SCENARIO_MAP: Dict[Tuple[str, str], Dict[str, str]] = {
    # IMAGE - SSP1
    ("image", "SSP1-RCP19"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    ("image", "SSP1-RCP26"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    ("image", "SSP1-PkBudg500"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    ("image", "SSP1-PkBudg1150"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
    ("image", "SSP1-NPi"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
    ("image", "SSP1-NDC"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
    ("image", "SSP1-Base"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
    # REMIND - SSP5
    ("remind", "SSP5-PkBudg500"): {"iam": "REMIND", "ssp": "SSP5", "rcp": "2.6"},
    ("remind", "SSP5-PkBudg1150"): {"iam": "REMIND", "ssp": "SSP5", "rcp": "4.5"},
    ("remind", "SSP5-NPi"): {"iam": "REMIND", "ssp": "SSP5", "rcp": "6.0"},
    ("remind", "SSP5-NDC"): {"iam": "REMIND", "ssp": "SSP5", "rcp": "4.5"},
    ("remind", "SSP5-Base"): {"iam": "REMIND", "ssp": "SSP5", "rcp": "8.5"},
    # MESSAGE - SSP2
    ("message", "SSP2-RCP26"): {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"},
    ("message", "SSP2-RCP45"): {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"},
    ("message", "SSP2-Base"): {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "8.5"},
}

_PAIRINGS = "IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4, REMIND-SSP5"


def _validate(scenario: Dict[str, str]) -> Dict[str, str]:
    """Reject a characterization scenario Watanabe et al. do not provide."""
    missing = {"iam", "ssp", "rcp"} - set(scenario)
    if missing:
        raise ValueError(
            f"A characterization scenario needs the keys 'iam', 'ssp' and 'rcp'; "
            f"missing {sorted(missing)} in {scenario!r}."
        )
    triple = (scenario["iam"], scenario["ssp"], scenario["rcp"])
    if triple not in VALID_SCENARIOS:
        raise ValueError(
            f"{triple} is not a scenario provided by Watanabe et al. (2026). "
            f"Each IAM is paired with one SSP there: {_PAIRINGS}. "
            f"See dynamic_characterization.prospective.VALID_SCENARIOS, or "
            f"bw_timex.available_scenarios(usable_for='prospective')."
        )
    return {"iam": scenario["iam"], "ssp": scenario["ssp"], "rcp": scenario["rcp"]}


def lookup_prospective_scenario(
    scenario: Optional[dict],
) -> Optional[Dict[str, str]]:
    """The characterization scenario a premise background scenario maps to.

    Parameters
    ----------
    scenario : dict or None
        A `TimexLCASettings.scenario`, i.e. premise metadata. Only `iam_model`
        and `pathway` are read; build keys like `years` are ignored.

    Returns
    -------
    dict or None
        `{"iam": ..., "ssp": ..., "rcp": ...}`, or None when the scenario is
        empty or has no exact counterpart.
    """
    if not scenario:
        return None
    iam_model = scenario.get("iam_model")
    pathway = scenario.get("pathway")
    if not iam_model or not pathway:
        return None
    found = PROSPECTIVE_SCENARIO_MAP.get((str(iam_model).lower(), pathway))
    return dict(found) if found else None


def resolve_characterization_scenario(
    metric: str,
    characterization_scenario: Optional[dict],
    scenario: Optional[dict],
) -> Optional[Dict[str, str]]:
    """Decide which prospective scenario a run characterizes with.

    Precedence, highest first: an explicit `characterization_scenario`, the
    background scenario's entry in `PROSPECTIVE_SCENARIO_MAP`, the session
    default set with `dynamic_characterization.prospective.set_scenario`.

    The session default sits below the table on purpose: someone who set it once
    in a notebook and then compares two backgrounds should get the two matching
    factor sets, not one stale set carried across both.

    Returns None for a non-prospective metric, which then never consults the
    table and never raises over a missing entry.
    """
    if metric not in PROSPECTIVE_METRICS:
        return None

    if characterization_scenario:
        resolved = _validate(characterization_scenario)
        logger.info(
            f"{metric}: characterization scenario "
            f"{resolved['iam']}-{resolved['ssp']}-RCP{resolved['rcp']} "
            f"(given as characterization_scenario)"
        )
        return resolved

    from_table = lookup_prospective_scenario(scenario)
    if from_table:
        logger.info(
            f"{metric}: characterization scenario "
            f"{from_table['iam']}-{from_table['ssp']}-RCP{from_table['rcp']} "
            f"from background {scenario['iam_model']} / {scenario['pathway']} "
            f"(PROSPECTIVE_SCENARIO_MAP)"
        )
        return from_table

    try:
        from_session = get_scenario()
    except RuntimeError:
        from_session = None

    if from_session:
        resolved = _validate(from_session)
        logger.info(
            f"{metric}: characterization scenario "
            f"{resolved['iam']}-{resolved['ssp']}-RCP{resolved['rcp']} "
            f"from the session default set with set_scenario()"
        )
        return resolved

    raise ValueError(_unresolved_message(metric, scenario))


def _unresolved_message(metric: str, scenario: Optional[dict]) -> str:
    """What to tell someone whose prospective metric has no scenario."""
    override = (
        f"    lcia={{\"metric\": \"{metric}\",\n"
        f"          \"characterization_scenario\": "
        f"{{\"iam\": \"MESSAGE\", \"ssp\": \"SSP2\", \"rcp\": \"2.6\"}}}}"
    )
    if scenario and scenario.get("iam_model") and scenario.get("pathway"):
        return (
            f"metric={metric!r} needs a prospective characterization scenario, and "
            f"the background scenario {scenario['iam_model']} / {scenario['pathway']} "
            f"has no entry in bw_timex.PROSPECTIVE_SCENARIO_MAP (Watanabe et al. 2026 "
            f"pairs each IAM with one SSP: {_PAIRINGS}).\n\n"
            f"Choose one explicitly:\n\n{override}\n\n"
            f"bw_timex.available_scenarios() lists both catalogues side by side."
        )
    return (
        f"metric={metric!r} needs a prospective characterization scenario, and this "
        f"TimexLCA has no background scenario to derive one from (it was built with "
        f"database_dates, or with no scenario at all).\n\n"
        f"Choose one explicitly:\n\n{override}\n\n"
        f"bw_timex.available_scenarios(usable_for='prospective') lists the options."
    )
```

Note on the table's contents: the entries above are the pairings that exist in
both catalogues. When running Task 9's notebook, confirm each premise pathway
name used there actually exists in the installed premise version; if a name
differs, correct the table entry and its test, do not add a near-miss pairing.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_prospective_scenarios.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add bw_timex/prospective_scenarios.py tests/test_prospective_scenarios.py
git commit -m "feat: map premise background scenarios to prospective CF scenarios"
```

---

### Task 5: `available_scenarios()`

**Files:**
- Modify: `bw_timex/prospective_scenarios.py` (append)
- Test: `tests/test_prospective_scenarios.py` (append)

**Interfaces:**
- Consumes: `PROSPECTIVE_SCENARIO_MAP` from Task 4
- Produces: `available_scenarios(usable_for: str | None = None) -> pandas.DataFrame` with columns `iam_model`, `pathway`, `iam`, `ssp`, `rcp`, `premise`, `prospective`, `in_project`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_prospective_scenarios.py`:

```python
from bw_timex.prospective_scenarios import available_scenarios

EXPECTED_COLUMNS = [
    "iam_model",
    "pathway",
    "iam",
    "ssp",
    "rcp",
    "premise",
    "prospective",
    "in_project",
]


def test_columns_and_shape():
    table = available_scenarios()
    assert list(table.columns) == EXPECTED_COLUMNS
    assert len(table) > 0


def test_a_shared_pairing_is_ticked_on_both_sides():
    table = available_scenarios()
    row = table[
        (table.iam_model == "image") & (table.pathway == "SSP1-RCP26")
    ].iloc[0]
    assert row.premise and row.prospective
    assert (row.iam, row.ssp, row.rcp) == ("IMAGE", "SSP1", "2.6")


def test_prospective_only_rows_exist_and_have_no_premise_side():
    table = available_scenarios()
    prospective_only = table[~table.premise & table.prospective]
    assert len(prospective_only) > 0
    assert prospective_only.iam_model.isna().all()
    assert prospective_only.pathway.isna().all()


def test_usable_for_premise_returns_only_premise_rows():
    table = available_scenarios(usable_for="premise")
    assert table.premise.all()


def test_usable_for_prospective_returns_only_prospective_rows():
    table = available_scenarios(usable_for="prospective")
    assert table.prospective.all()


def test_usable_for_both_returns_only_shared_rows():
    table = available_scenarios(usable_for="both")
    assert (table.premise & table.prospective).all()
    # Every shared row is a table entry. The reverse need not hold: a mapped
    # pairing whose premise scenario the installed premise does not ship is
    # simply absent from the premise catalogue.
    shared = {(row.iam_model, row.pathway) for row in table.itertuples()}
    assert shared <= set(PROSPECTIVE_SCENARIO_MAP)
    assert len(shared) > 0


def test_usable_for_rejects_an_unknown_value():
    with pytest.raises(ValueError):
        available_scenarios(usable_for="nonsense")


def test_in_project_is_empty_without_matching_databases():
    # The test project holds no premise-built vintages.
    table = available_scenarios(usable_for="both")
    assert (table.in_project == "").all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_prospective_scenarios.py -k available -v`
Expected: FAIL — `ImportError: cannot import name 'available_scenarios'`

- [ ] **Step 3: Implement it**

Append to `bw_timex/prospective_scenarios.py`:

```python
import pandas as pd

_USABLE_FOR = {None, "premise", "prospective", "both"}


def _premise_catalogue() -> Tuple[set, bool]:
    """Every `(iam_model, pathway)` premise can build, and whether that is complete.

    premise bundles one file per IAM scenario under `data/iam_output_files`,
    named `<iam_model>_<pathway>.csv`. The names are readable without the
    decryption key, so the catalogue costs nothing to enumerate. When premise is
    not installed, the table falls back to the pairings this module knows about
    and says so.
    """
    try:
        import premise
    except ImportError:
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False

    from pathlib import Path

    directory = Path(premise.__file__).parent / "data" / "iam_output_files"
    if not directory.is_dir():
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False

    catalogue = set()
    for path in directory.iterdir():
        stem = path.stem
        if "_" not in stem:
            continue
        iam_model, pathway = stem.split("_", 1)
        catalogue.add((iam_model.lower(), pathway))
    if not catalogue:
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False
    return catalogue, True


def _years_in_project(iam_model: str, pathway: str) -> str:
    """The vintage years this project already holds for a pairing, as text."""
    import bw2data as bd

    years = []
    for name in bd.databases:
        metadata = bd.databases[name]
        if str(metadata.get("iam_model", "")).lower() != iam_model:
            continue
        if metadata.get("pathway") != pathway:
            continue
        representative_time = metadata.get("representative_time")
        if hasattr(representative_time, "year"):
            years.append(representative_time.year)
        elif isinstance(representative_time, str) and representative_time[:4].isdigit():
            years.append(int(representative_time[:4]))
    return ", ".join(str(year) for year in sorted(set(years)))


def available_scenarios(usable_for: Optional[str] = None) -> pd.DataFrame:
    """Every scenario known to either side, and which side knows it.

    A background scenario and a prospective characterization scenario are
    different things, and plenty of studies want only one of them: a premise
    background characterized with IPCC AR6 factors, or the prospective factors
    on a hand-built background. This lists both catalogues, so a row says what
    it can be used for rather than only what both support.

    Parameters
    ----------
    usable_for : str, optional
        `"premise"` keeps the scenarios premise can build, `"prospective"` those
        with prospective characterization factors, `"both"` those usable
        together with no manual pairing. The default lists everything.

    Returns
    -------
    pandas.DataFrame
        Columns `iam_model`, `pathway` (the premise side, NA when premise has no
        such scenario), `iam`, `ssp`, `rcp` (the Watanabe side, NA when there is
        no exact pairing), the flags `premise` and `prospective`, and
        `in_project`: the vintage years this project already holds, as text.

    Examples
    --------
    ```python
    import bw_timex

    bw_timex.available_scenarios(usable_for="both")
    ```
    """
    if usable_for not in _USABLE_FOR:
        raise ValueError(
            f"`usable_for` must be one of 'premise', 'prospective', 'both' or None, "
            f"not {usable_for!r}."
        )

    catalogue, complete = _premise_catalogue()
    rows = []

    for iam_model, pathway in sorted(catalogue):
        mapped = PROSPECTIVE_SCENARIO_MAP.get((iam_model, pathway))
        rows.append(
            {
                "iam_model": iam_model,
                "pathway": pathway,
                "iam": mapped["iam"] if mapped else pd.NA,
                "ssp": mapped["ssp"] if mapped else pd.NA,
                "rcp": mapped["rcp"] if mapped else pd.NA,
                "premise": True,
                "prospective": mapped is not None,
                "in_project": _years_in_project(iam_model, pathway),
            }
        )

    paired = {
        (value["iam"], value["ssp"], value["rcp"])
        for key, value in PROSPECTIVE_SCENARIO_MAP.items()
        if key in catalogue
    }
    for iam, ssp, rcp in sorted(VALID_SCENARIOS):
        if (iam, ssp, rcp) in paired:
            continue
        rows.append(
            {
                "iam_model": pd.NA,
                "pathway": pd.NA,
                "iam": iam,
                "ssp": ssp,
                "rcp": rcp,
                "premise": False,
                "prospective": True,
                "in_project": "",
            }
        )

    table = pd.DataFrame(rows, columns=[
        "iam_model", "pathway", "iam", "ssp", "rcp",
        "premise", "prospective", "in_project",
    ])

    if not complete:
        logger.info(
            "premise is not installed, so the premise side of this table lists only "
            "the scenarios bw_timex maps to prospective characterization factors. "
            'Install it with: pip install "bw_timex[premise]"'
        )

    if usable_for == "premise":
        table = table[table.premise]
    elif usable_for == "prospective":
        table = table[table.prospective]
    elif usable_for == "both":
        table = table[table.premise & table.prospective]

    return table.reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_prospective_scenarios.py -v`
Expected: PASS. If premise is installed in the dev environment, some premise
pathway names may differ from the table's — that is information, not a failure:
correct the table entries in Task 4 to the real names and rerun.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/prospective_scenarios.py tests/test_prospective_scenarios.py
git commit -m "feat: list premise and prospective scenario catalogues side by side"
```

---

### Task 6: Settings and `dynamic_lcia` plumbing

**Files:**
- Modify: `bw_timex/timex_lca.py:123-133` (`STAGE_GROUPS["lcia"]`), `:164-180` (LCIA fields), `:2084-2094` (`dynamic_lcia` signature), `:2222-2231` (the `characterize` call)
- Modify: `bw_timex/validation.py` (`DynamicLCIAInputs`)
- Test: `tests/test_prospective_lcia_settings.py` (create)

**Interfaces:**
- Consumes: `resolve_characterization_scenario` from Task 4
- Produces:
  - `TimexLCASettings.characterization_scenario: Optional[dict] = None`
  - `TimexLCASettings.time_varying_re: bool = False`, `.fallback_to_ipcc: bool = True`, `.characterize_biogenic_uptake: bool = True`
  - `TimexLCA.dynamic_lcia(..., characterization_scenario=None, time_varying_re=False, fallback_to_ipcc=True, characterize_biogenic_uptake=True)`
  - `TimexLCA.current_characterization_scenario: Optional[dict]` — set by `dynamic_lcia`, read by Task 7

- [ ] **Step 1: Write the failing test**

Create `tests/test_prospective_lcia_settings.py`. Use the existing electric
vehicle fixture — read `tests/test_timex_lca_run.py` and `tests/conftest.py`
first and use the same fixture name and setup it uses.

```python
"""The prospective scenario travels with the settings, not with module state."""

from dataclasses import replace
from unittest.mock import patch

import pandas as pd
import pytest

from bw_timex import TimexLCA, TimexLCASettings


def test_characterization_scenario_is_an_lcia_group_setting():
    settings = TimexLCASettings(
        demand={1: 1},
        method=("a", "b"),
        lcia={
            "metric": "pGWP",
            "characterization_scenario": {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
        },
    )
    assert settings.characterization_scenario == {
        "iam": "IMAGE",
        "ssp": "SSP1",
        "rcp": "2.6",
    }
    assert settings.metric == "pGWP"


def test_forwarded_characterize_arguments_are_lcia_settings():
    settings = TimexLCASettings(
        demand={1: 1},
        method=("a", "b"),
        lcia={
            "time_varying_re": True,
            "fallback_to_ipcc": False,
            "characterize_biogenic_uptake": False,
        },
    )
    assert settings.time_varying_re is True
    assert settings.fallback_to_ipcc is False
    assert settings.characterize_biogenic_uptake is False


def test_characterization_scenario_in_the_wrong_group_is_rejected():
    with pytest.raises(TypeError):
        TimexLCASettings(
            demand={1: 1},
            method=("a", "b"),
            timeline={"characterization_scenario": {"iam": "IMAGE"}},
        )


def test_dynamic_lcia_passes_the_resolved_scenario_to_characterize(electric_vehicle_lci):
    """The scenario reaches `characterize` as an argument, per call."""
    tlca = electric_vehicle_lci
    explicit = {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}

    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.dynamic_lcia(metric="pGWP", characterization_scenario=explicit)

    assert characterize.call_args.kwargs["scenario"] == explicit
    assert tlca.current_characterization_scenario == explicit


def test_dynamic_lcia_passes_no_scenario_for_a_static_metric(electric_vehicle_lci):
    tlca = electric_vehicle_lci

    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.dynamic_lcia(metric="GWP")

    assert characterize.call_args.kwargs["scenario"] is None
    assert tlca.current_characterization_scenario is None


def test_dynamic_lcia_forwards_the_three_previously_dropped_arguments(
    electric_vehicle_lci,
):
    tlca = electric_vehicle_lci

    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.dynamic_lcia(
            metric="radiative_forcing",
            time_varying_re=True,
            fallback_to_ipcc=False,
            characterize_biogenic_uptake=False,
        )

    kwargs = characterize.call_args.kwargs
    assert kwargs["time_varying_re"] is True
    assert kwargs["fallback_to_ipcc"] is False
    assert kwargs["characterize_biogenic_uptake"] is False


def test_characterization_scenario_may_change_between_runs(electric_vehicle_lci):
    """It does not select databases, so it is not a fixed field."""
    tlca = electric_vehicle_lci
    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.run(
            metric="pGWP",
            characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
        )
        tlca.run(
            metric="pGWP",
            characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
        )
    assert tlca.current_characterization_scenario["rcp"] == "8.5"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_prospective_lcia_settings.py -v`
Expected: FAIL — `characterization_scenario` is not a setting

- [ ] **Step 3: Add the settings fields**

In `bw_timex/timex_lca.py`, extend `STAGE_GROUPS["lcia"]` (line 123):

```python
        "lcia": (
            "static_lcia_enabled",
            "dynamic_lcia_enabled",
            "metric",
            "time_horizon",
            "fixed_time_horizon",
            "time_horizon_start",
            "characterization_functions",
            "characterization_function_co2",
            "characterization_scenario",
            "time_varying_re",
            "fallback_to_ipcc",
            "characterize_biogenic_uptake",
            "use_disaggregated_lci",
        ),
```

and the flat fields (after line 179):

```python
    characterization_function_co2: Optional[dict] = None
    #: Prospective characterization scenario, as `{"iam": ..., "ssp": ...,
    #: "rcp": ...}`. Only used by the prospective metrics. `None` (the default)
    #: derives it from `scenario` via `PROSPECTIVE_SCENARIO_MAP`. Unlike
    #: `scenario` this does not select databases, so it may vary between runs of
    #: one `TimexLCA`.
    characterization_scenario: Optional[dict] = None
    #: Passed to `dynamic_characterization.characterize`. Use a radiative
    #: efficiency that evolves over the decay period instead of a fixed one from
    #: the emission year. Prospective metrics only.
    time_varying_re: bool = False
    #: Passed to `dynamic_characterization.characterize`. Characterize GHGs
    #: without prospective factors (e.g. CO) with IPCC AR6 ones instead of
    #: skipping them. Prospective metrics only.
    fallback_to_ipcc: bool = True
    #: Passed to `dynamic_characterization.characterize`. Include
    #: characterization functions for biogenic uptake flows.
    characterize_biogenic_uptake: bool = True
    use_disaggregated_lci: bool = False
```

- [ ] **Step 4: Extend `dynamic_lcia`**

In `bw_timex/timex_lca.py`, add to the signature (after
`characterization_function_co2`, line 2092):

```python
        characterization_function_co2: dict = None,
        characterization_scenario: dict = None,
        time_varying_re: bool = False,
        fallback_to_ipcc: bool = True,
        characterize_biogenic_uptake: bool = True,
        use_disaggregated_lci: bool = False,
```

Document them in the docstring's Parameters section, mirroring the field
comments above, and add to the metric description that the prospective metrics
take their scenario from `TimexLCASettings.scenario` unless
`characterization_scenario` says otherwise.

Resolve the scenario right after `self.current_time_horizon = time_horizon`
(line ~2172):

```python
        self.current_metric = metric
        self.current_time_horizon = time_horizon
        self.current_characterization_scenario = resolve_characterization_scenario(
            metric=metric,
            characterization_scenario=characterization_scenario,
            scenario=self.settings.scenario if hasattr(self, "settings") else None,
        )
```

Check how the object stores its settings before writing that line — read
`TimexLCA.__init__` and `from_settings` and use whatever attribute actually
holds them (`self.settings`, `self._settings`, ...); use that name instead of
the guess above and drop the `hasattr` guard if the attribute is always set.

Pass everything to `characterize` (line 2222):

```python
        self.characterized_inventory = characterize(
            dynamic_inventory_df=inventory_in_time_horizon,
            metric=metric,
            characterization_functions=characterization_functions,
            base_lcia_method=self.method,
            time_horizon=time_horizon,
            fixed_time_horizon=fixed_time_horizon,
            time_horizon_start=time_horizon_start,
            characterization_function_co2=characterization_function_co2,
            time_varying_re=time_varying_re,
            fallback_to_ipcc=fallback_to_ipcc,
            characterize_biogenic_uptake=characterize_biogenic_uptake,
            scenario=self.current_characterization_scenario,
        )
```

Add the import at the top of `bw_timex/timex_lca.py`:

```python
from .prospective_scenarios import resolve_characterization_scenario
```

Confirm `run()` forwards the new settings to `dynamic_lcia` — read how it calls
the stage methods and add the four new names wherever it lists the LCIA
arguments explicitly.

- [ ] **Step 5: Extend validation**

In `bw_timex/validation.py`, add to `DynamicLCIAInputs`:

```python
    characterization_scenario: Optional[dict] = None
    time_varying_re: bool = False
    fallback_to_ipcc: bool = True
    characterize_biogenic_uptake: bool = True

    @field_validator("characterization_scenario")
    @classmethod
    def validate_characterization_scenario(cls, v: Optional[dict]) -> Optional[dict]:
        if v is None:
            return v
        missing = {"iam", "ssp", "rcp"} - set(v)
        if missing:
            raise ValueError(
                f"characterization_scenario needs the keys 'iam', 'ssp' and 'rcp'; "
                f"missing {sorted(missing)}."
            )
        return v
```

and pass them in the `DynamicLCIAInputs(...)` construction at the top of
`dynamic_lcia` (line 2154).

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_prospective_lcia_settings.py -v`
Expected: PASS

- [ ] **Step 7: Run the full suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/timex_lca.py bw_timex/validation.py \
        tests/test_prospective_lcia_settings.py
git commit -m "feat: carry the prospective scenario in TimexLCASettings"
```

---

### Task 7: Comparison reporting

**Files:**
- Modify: `bw_timex/timex_lca.py:926-935` (`_background_key`), `:937-979` (`_result_row`)
- Test: `tests/test_prospective_comparison.py` (create)

**Interfaces:**
- Consumes: `TimexLCA.current_characterization_scenario` from Task 6
- Produces: `ComparisonResult.summary` columns `cf_iam`, `cf_ssp`, `cf_rcp`

- [ ] **Step 1: Write the failing test**

Create `tests/test_prospective_comparison.py`, using the same electric vehicle
fixture as Task 6:

```python
"""Comparisons record and vary the characterization scenario."""

from dataclasses import replace
from unittest.mock import patch

import pandas as pd

from bw_timex import TimexLCA, TimexLCASettings


def test_summary_records_the_characterization_scenario(electric_vehicle_settings):
    base = replace(
        electric_vehicle_settings,
        metric="pGWP",
        characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    )
    comparison = TimexLCA.compare([replace(base, label="low")])
    row = comparison.summary.iloc[0]
    assert (row.cf_iam, row.cf_ssp, row.cf_rcp) == ("IMAGE", "SSP1", "2.6")


def test_static_metric_rows_have_no_cf_columns_filled(electric_vehicle_settings):
    comparison = TimexLCA.compare(
        [replace(electric_vehicle_settings, metric="GWP", label="static")]
    )
    assert pd.isna(comparison.summary.iloc[0].cf_iam)


def test_two_characterization_scenarios_share_one_object(electric_vehicle_settings):
    base = replace(electric_vehicle_settings, metric="pGWP")
    comparison = TimexLCA.compare(
        [
            replace(
                base,
                characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
                label="RCP2.6",
            ),
            replace(
                base,
                characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
                label="RCP8.5",
            ),
        ],
        keep_objects=True,
    )
    assert comparison.objects["RCP2.6"] is comparison.objects["RCP8.5"]
    assert list(comparison.summary.cf_rcp) == ["2.6", "8.5"]
    scores = comparison.summary.dynamic_score
    assert scores.iloc[0] != scores.iloc[1]


def test_settings_differing_only_in_create_missing_do_not_share_an_object(
    electric_vehicle_settings,
):
    key_a = TimexLCA._background_key(replace(electric_vehicle_settings, create_missing=False))
    key_b = TimexLCA._background_key(replace(electric_vehicle_settings, create_missing=True))
    assert key_a != key_b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_prospective_comparison.py -v`
Expected: FAIL — no `cf_iam` column; the `create_missing` keys are equal

- [ ] **Step 3: Implement**

In `bw_timex/timex_lca.py`, add `create_missing` to `_background_key`:

```python
    @staticmethod
    def _background_key(settings: TimexLCASettings) -> tuple:
        """Hashable identity of the background a settings object asks for.

        Two calculations can share one `TimexLCA` exactly when these match.
        Mirrors `TimexLCASettings.FIXED_FIELDS`: a field that cannot change
        between runs of one object must also decide whether two settings can
        share one.
        """
        return (
            tuple(sorted((k, str(v)) for k, v in (settings.database_dates or {}).items())),
            tuple(sorted((k, str(v)) for k, v in (settings.scenario or {}).items())),
            settings.create_missing,
            settings.use_global_lci_cache,
        )
```

In `_result_row`, record the resolved scenario after the scores:

```python
        characterization_scenario = getattr(
            self, "current_characterization_scenario", None
        ) or {}
        row.update(
            {
                "cf_iam": characterization_scenario.get("iam", float("nan")),
                "cf_ssp": characterization_scenario.get("ssp", float("nan")),
                "cf_rcp": characterization_scenario.get("rcp", float("nan")),
            }
        )
```

placed just before the `for key, value in (settings.scenario or {}).items()`
loop, so the `cf_*` columns sit next to the `scenario_*` ones.

Update `ComparisonResult`'s docstring (line 259) to mention that `cf_iam` /
`cf_ssp` / `cf_rcp` say which prospective factors produced a row, and are NaN
for static metrics.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_prospective_comparison.py -v`
Expected: PASS

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_prospective_comparison.py
git commit -m "feat: report the characterization scenario in comparison summaries"
```

---

### Task 8: Public API

**Files:**
- Modify: `bw_timex/__init__.py`
- Modify: `tests/test_public_api.py`
- Modify: `docs/api/timex_lca.md`
- Create: `docs/api/prospective_scenarios.md`
- Modify: `zensical.toml` (the api nav list)

**Interfaces:**
- Consumes: Tasks 4 and 5
- Produces: `bw_timex.PROSPECTIVE_SCENARIO_MAP`, `bw_timex.VALID_SCENARIOS`, `bw_timex.available_scenarios`

- [ ] **Step 1: Write the failing test**

Read `tests/test_public_api.py` first — it checks `__all__` against the module's
attributes. Add the three names to whichever list drives that check, and add:

```python
def test_prospective_scenario_helpers_are_exposed():
    assert bw_timex.available_scenarios is not None
    assert ("image", "SSP1-RCP26") in bw_timex.PROSPECTIVE_SCENARIO_MAP
    assert ("IMAGE", "SSP1", "2.6") in bw_timex.VALID_SCENARIOS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: FAIL — `module 'bw_timex' has no attribute 'available_scenarios'`

- [ ] **Step 3: Export them**

In `bw_timex/__init__.py`, add the import and the `__all__` entries:

```python
from .prospective_scenarios import (
    PROSPECTIVE_SCENARIO_MAP,
    VALID_SCENARIOS,
    available_scenarios,
)
```

```python
    # prospective characterization scenarios
    "PROSPECTIVE_SCENARIO_MAP",
    "VALID_SCENARIOS",
    "available_scenarios",
```

`VALID_SCENARIOS` is re-exported from `bw_timex.prospective_scenarios`, which
already imports it from `dynamic_characterization.prospective`.

- [ ] **Step 4: Add the API docs page**

Create `docs/api/prospective_scenarios.md`:

```markdown
---
icon: lucide/table
tags:
  - api
---

# Prospective scenarios

A premise background scenario and a prospective characterization scenario
describe the same future in two different vocabularies. This module holds the
table relating them, and the rule deciding which characterization scenario a run
uses.

`available_scenarios()` lists both catalogues side by side, including the
scenarios only one of them knows about.

::: bw_timex.prospective_scenarios
```

Add it to the api nav in `zensical.toml`, next to the other api pages, as
`{ "Prospective Scenarios" = "docs/api/prospective_scenarios.md" }` — match the
exact path style the neighbouring entries use.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add bw_timex/__init__.py tests/test_public_api.py docs/api/ zensical.toml
git commit -m "feat: expose the prospective scenario table and listing"
```

---

### Task 9: Scenario comparison tutorial

**Files:**
- Modify: `notebooks/tutorials/5_scenario_comparison.ipynb`
- Regenerate: `docs/content/examples/tutorials/scenario_comparison.md` and its `_files/` directory

**Interfaces:**
- Consumes: everything from Tasks 4-8
- Produces: nothing code depends on

**Note:** this task runs premise and builds several full ecoinvent copies. It
needs `PREMISE_KEY`, `ECOINVENT_USERNAME` and `ECOINVENT_PASSWORD` in the
environment, and takes hours. Confirm with the user before starting it.

- [ ] **Step 1: Switch the notebook's scenario**

Throughout `notebooks/tutorials/5_scenario_comparison.ipynb`, replace the
background scenario `{"iam_model": "remind-eu", "pathway": "SSP2-PkBudg650"}`
with `{"iam_model": "image", "pathway": "SSP1-RCP26"}`, and the second pathway
in the existing comparison (`SSP2-PkBudg1000`) with `"SSP1-Base"`. Keep
`system_model`, `ecoinvent_version` and `years` as they are. The point of the
switch: `image`/`SSP1-*` has exact prospective counterparts, so the new section
can demonstrate the automatic path rather than only the override.

Before running, check the pathway names against the installed premise:

```python
import bw_timex
bw_timex.available_scenarios(usable_for="both").query("iam_model == 'image'")
```

If premise names them differently, use premise's names and correct
`PROSPECTIVE_SCENARIO_MAP` (Task 4) and its test to match.

- [ ] **Step 2: Add the prospective comparison section**

Append a new markdown cell and code cells at the end of the notebook:

Markdown cell:

```markdown
## Prospective characterization factors

The scenario we picked does not only decide which background databases are used
- it also decides how the resulting emissions are characterized, if we ask for a
prospective metric. The radiative efficiency of a tonne of CO2 depends on how
much is already in the atmosphere, which is exactly what an SSP-RCP scenario
describes.

`pGWP`, `pGTP` and `prospective_radiative_forcing` use the scenario-dependent
factors of [Watanabe et al. (2026)](https://doi.org/10.1021/acs.est.5b01118).
Switching `scenario` moves both halves at once: the background vintages and the
characterization factors.
```

Code cell:

```python
prospective_comparison = TimexLCA.compare(
    [
        replace(
            base_settings,
            scenario={**base_settings.scenario, "pathway": pathway},
            lcia={"metric": "pGWP", "time_horizon": 100},
            label=pathway,
        )
        for pathway in ("SSP1-RCP26", "SSP1-Base")
    ]
)

prospective_comparison.summary[
    ["label", "static_score", "dynamic_score", "cf_iam", "cf_ssp", "cf_rcp"]
]
```

Adapt `base_settings` to whatever the notebook actually named its base settings
object earlier.

Markdown cell after it:

```markdown
The `cf_*` columns record which characterization factors produced each row - here
RCP2.6 for the mitigation pathway and RCP8.5 for the baseline, both derived from
the background scenario without our saying so. The log lines above say the same
thing while the calculation runs.
```

Code cell showing the catalogue:

```python
bw_timex.available_scenarios(usable_for="both")
```

Final markdown cell on the pairings that do not exist:

```markdown
Not every premise scenario has prospective factors. Watanabe et al. pair each IAM
with one SSP - IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4, REMIND-SSP5 - so a
REMIND-SSP2 background, which premise is happy to build, has no counterpart.
bw_timex will not silently substitute a near-miss there; it raises and asks you to
choose:

```python
lcia={
    "metric": "pGWP",
    "characterization_scenario": {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"},
}
```

That choice belongs in your study, not in a lookup table nobody reads.
```

- [ ] **Step 3: Run the notebook end to end**

Run it from a clean kernel so the stored outputs match the new scenario. Every
cell must execute; the notebook builds its own databases.

- [ ] **Step 4: Regenerate the docs page**

Regenerate `docs/content/examples/tutorials/scenario_comparison.md` and its
`scenario_comparison_files/` directory from the executed notebook, the same way
the existing page was produced. Check `git log --oneline -- docs/content/examples/tutorials/scenario_comparison.md`
and the repo's docs tooling for the command; the generated file keeps its YAML
front matter (`icon`, `tags`) and the `data-source-edit-url` div.

- [ ] **Step 5: Verify the page**

Confirm the regenerated markdown shows the new scenario names, the `cf_*`
columns and the `available_scenarios()` table, and that no `remind-eu` /
`SSP2-PkBudg650` strings survive.

Run: `grep -c "SSP2-PkBudg650" docs/content/examples/tutorials/scenario_comparison.md`
Expected: `0`

- [ ] **Step 6: Commit**

```bash
git add notebooks/tutorials/5_scenario_comparison.ipynb \
        docs/content/examples/tutorials/scenario_comparison.md \
        docs/content/examples/tutorials/scenario_comparison_files/
git commit -m "docs: compare prospective scenarios in the comparison tutorial"
```

---

### Task 10: Dynamic characterization tutorial

**Files:**
- Modify: `notebooks/tutorials/3_dynamic_characterization.ipynb`
- Regenerate: `docs/content/examples/tutorials/dynamic_characterization.md` and its `_files/` directory

**Interfaces:**
- Consumes: Tasks 4-8
- Produces: nothing code depends on

- [ ] **Step 1: Add the prospective metrics section**

Append to `notebooks/tutorials/3_dynamic_characterization.ipynb`, after the
existing `GWP` / `radiative_forcing` material.

Markdown cell:

```markdown
## Prospective characterization factors

The characterization factors used so far are the IPCC AR6 ones: fixed values,
derived from today's atmosphere. But the radiative efficiency of a greenhouse gas
depends on the concentrations it is added to, and those change over the century.
[Watanabe et al. (2026)](https://doi.org/10.1021/acs.est.5b01118) provide
radiative efficiencies, and for CO2 an impulse response function, that vary by
IAM-SSP-RCP scenario and by year of emission.

bw_timex exposes them as three extra metrics:

| metric | unit | factors |
|---|---|---|
| `radiative_forcing` | W/m² | IPCC AR6 |
| `GWP` | kg CO₂-eq | IPCC AR6 |
| `prospective_radiative_forcing` | W/m² | Watanabe et al. (2026) |
| `pGWP` | kg CO₂-eq | Watanabe et al. (2026) |
| `pGTP` | kg CO₂-eq | Watanabe et al. (2026) |

They need a scenario, given as `iam`, `ssp` and `rcp`:
```

Code cell:

```python
tlca.dynamic_lcia(
    metric="pGWP",
    time_horizon=100,
    characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
)
tlca.dynamic_score
```

Markdown cell:

```markdown
When the background comes from premise, the scenario is normally not written out
at all: it is taken from the background scenario, so the emissions and the factors
describe the same future. See the
[scenario comparison tutorial](scenario_comparison.md) for that.

Not every IAM-SSP combination exists - each IAM is paired with one SSP:
```

Code cell:

```python
import bw_timex

bw_timex.available_scenarios(usable_for="prospective")
```

- [ ] **Step 2: Run the notebook end to end**

Run from a clean kernel. This notebook uses the built-in example, so it needs no
premise credentials.

- [ ] **Step 3: Regenerate the docs page**

Same procedure as Task 9 Step 4, for
`docs/content/examples/tutorials/dynamic_characterization.md`.

- [ ] **Step 4: Verify**

Run: `grep -c "pGWP" docs/content/examples/tutorials/dynamic_characterization.md`
Expected: a non-zero count

- [ ] **Step 5: Commit**

```bash
git add notebooks/tutorials/3_dynamic_characterization.ipynb \
        docs/content/examples/tutorials/dynamic_characterization.md \
        docs/content/examples/tutorials/dynamic_characterization_files/
git commit -m "docs: introduce the prospective metrics where characterization is taught"
```

---

### Task 11: Narrative documentation

**Files:**
- Modify: `docs/content/getting_started/lcia.md`
- Modify: `docs/content/getting_started/configured_runs.md`
- Modify: `docs/content/create_premise_dbs.md`

**Interfaces:**
- Consumes: Tasks 4-8
- Produces: nothing code depends on

- [ ] **Step 1: Extend the LCIA page**

In `docs/content/getting_started/lcia.md`, add the prospective metrics to the
list of available metrics, using the same table as Task 10 Step 1, followed by:

```markdown
The prospective metrics need a scenario. With a premise background it is derived
from the `scenario` the `TimexLCA` was built with, so there is nothing extra to
pass; otherwise give it explicitly:

```python
tlca.dynamic_lcia(
    metric="pGWP",
    time_horizon=100,
    characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
)
```

`bw_timex.available_scenarios()` lists which scenarios have prospective factors,
which premise can build, and which support both.
```

- [ ] **Step 2: Extend the configured runs page**

In `docs/content/getting_started/configured_runs.md`, add
`characterization_scenario` to the `lcia` group shown in the settings example
(around line 34) with a trailing comment, and one paragraph after it:

```markdown
For the prospective metrics (`pGWP`, `pGTP`, `prospective_radiative_forcing`) the
characterization factors depend on a scenario too. It is taken from `scenario` -
the same setting that picks the background databases - so a comparison that
varies the scenario varies both halves together. `characterization_scenario`
overrides that when the two should differ, or when there is no background
scenario to derive from.
```

- [ ] **Step 3: Extend the premise page**

In `docs/content/create_premise_dbs.md`, add a short note near where the pathway
is chosen:

```markdown
!!! note "The pathway also decides the characterization factors"

    If you later run a prospective metric (`pGWP`, `pGTP`,
    `prospective_radiative_forcing`), the scenario you pick here also selects the
    scenario-dependent characterization factors. Not every premise pathway has a
    counterpart - `bw_timex.available_scenarios()` shows which do.
```

- [ ] **Step 4: Check the docs build**

Build the docs the way the repo does (see `zensical.toml` and the deploy
workflow) and confirm the three pages render with no broken links.

- [ ] **Step 5: Commit**

```bash
git add docs/content/getting_started/lcia.md \
        docs/content/getting_started/configured_runs.md \
        docs/content/create_premise_dbs.md
git commit -m "docs: document prospective metrics and their scenario"
```

---

### Task 12: Changelog and dependency floor

**Files:**
- Modify: `CHANGES.md` (bw_timex)
- Modify: `pyproject.toml:40` (bw_timex)
- Modify: `CHANGES.md` (dynamic_characterization)

**Interfaces:**
- Consumes: every earlier task
- Produces: nothing code depends on

- [ ] **Step 1: Raise the dependency floor**

`bw_timex` now needs `characterize(scenario=...)`, which Part A adds. In
`bw_timex/pyproject.toml`, raise the floor to the version
`dynamic_characterization` releases with Task 1 — read its `CHANGES.md` and
`__init__.py:26` for the version it is going to, and pin
`"dynamic_characterization>=<that version>"`.

- [ ] **Step 2: Write the changelog entries**

In `dynamic_characterization/CHANGES.md`, following the existing format:

```markdown
### Added
- `characterize(scenario=...)` applies a prospective scenario for one call only,
  so several scenarios can be used in one process
- `prospective.scenario_context()` for scoping the scenario around a block

### Changed
- Out-of-bounds emission years warn once per bound instead of once per row
```

In `bw_timex/CHANGES.md`:

```markdown
### Added
- Prospective characterization scenarios are derived from the background
  scenario: `metric="pGWP"` with a premise `scenario` now characterizes with the
  matching Watanabe et al. (2026) factors, with no second place to keep in sync
- `characterization_scenario` LCIA setting to override that, or to use a
  prospective metric without a premise background
- `available_scenarios()`, listing the premise and prospective scenario
  catalogues side by side, and `PROSPECTIVE_SCENARIO_MAP` relating them
- `cf_iam`, `cf_ssp` and `cf_rcp` columns in `ComparisonResult.summary`, so a
  comparison records which factors produced each row
- `time_varying_re`, `fallback_to_ipcc` and `characterize_biogenic_uptake` are
  forwarded to `dynamic_characterization.characterize`; they were accepted there
  but never passed

### Fixed
- `create_missing` is part of a comparison's background identity, matching the
  guard in `run()`: two settings differing only in it no longer share one object
```

- [ ] **Step 3: Run the full suite one last time, in both repos**

```bash
cd /Users/timodiepers/Documents/Coding/dynamic_characterization && uv run pytest -q
cd /Users/timodiepers/Documents/Coding/bw_timex && uv run pytest -q
```

Expected: all pass in both.

- [ ] **Step 4: Commit**

```bash
git add CHANGES.md pyproject.toml
git commit -m "chore: changelog and dynamic_characterization floor"
```
