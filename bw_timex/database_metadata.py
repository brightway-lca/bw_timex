"""Read and write what a Brightway database represents.

`bw_timex` needs to know which point in time each background database stands
for. That information is stored in the database's own Brightway metadata
(`bw2data.databases[name]`), where premise also writes it when it exports a
prospective database:

```python
{
    "premise_version": "2.4.9.3",
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
from collections import defaultdict
from datetime import datetime
from typing import Any

import bw2data as bd
from loguru import logger

from .validation import DatabaseMetadataInputs

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
            if isinstance(value, str):
                # already a string (an ISO 8601 date or "dynamic"): store as given
                serialized[key] = value
            else:
                serialized[key] = normalized.isoformat()
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


def resolve_database_dates_from_metadata(
    scenario: dict | None = None,
) -> dict[str, datetime | str]:
    """
    Map the databases of the current project to the points in time they represent.

    Reads the `representative_time` metadata of every registered database (see
    [`set_database_metadata`][bw_timex.database_metadata.set_database_metadata]).

    If the project holds databases from more than one scenario (differing in
    any of `SCENARIO_SIGNATURE_KEYS`, e.g. two premise pathways), this raises
    a `ValueError` unless `scenario` narrows the selection down to one.

    Parameters
    ----------
    scenario : dict, optional
        Metadata a database must match to be included, e.g.
        `{"iam_model": "remind", "pathway": "SSP2-PkBudg500"}`. Databases that
        don't declare a filtered key at all are kept, so a filter narrows down
        an ambiguous project without excluding databases that carry no
        scenario metadata (e.g. a dynamic foreground). Raises `ValueError` if
        a filter key is not declared by any database in the project.

    Returns
    -------
    dict
        Mapping of database name to `datetime` or `"dynamic"`, ready to be used
        as `TimexLCA.database_dates`.
    """
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
