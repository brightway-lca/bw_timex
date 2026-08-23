"""Find the background vintages a scenario names, or build them with premise.

`TimexLCA(scenario=...)` selects background databases by their metadata. When
the project does not hold them yet, `ensure_scenario_databases` builds the
missing ones with premise instead of leaving the user at a dead end.

premise and bw2io are imported inside `_run_premise` and `_import_ecoinvent`
only, so `bw_timex` keeps working without them installed and a run that finds
everything it needs never touches either.
"""

from __future__ import annotations

import os
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
from .validation import ScenarioBuildInputs


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
    ScenarioBuildInputs(scenario=scenario)
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
