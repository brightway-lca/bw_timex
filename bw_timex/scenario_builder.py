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
    REPRESENTATIVE_TIME,
    SCENARIOS,
    _normalize_representative_time,
    database_matches_scenario,
    database_matches_sectors,
    set_database_metadata,
    split_scenario,
)
from .validation import ScenarioBuildInputs


def find_existing_vintages(
    filters: dict, sectors: list[str] | None = None
) -> dict[int, tuple[str, datetime]]:
    """Map each year the project already covers to the database covering it.

    A year is covered by a registered database whose `representative_time`
    falls in it and that the scenario filter keeps. "Keeps" is the resolver's
    own rule (`database_matches_scenario`): a database is dropped only if it
    declares a filtered key with a different value. Any stricter rule would
    build a second database for a year `TimexLCA` already resolves.

    `sectors` is not part of that filter - it never reaches
    `TimexLCA(scenario=...)` - but a vintage built for other sectors is not the
    vintage this build asked for, so it is compared separately
    (`database_matches_sectors`).

    Returns the database's own `representative_time`, not a date derived from
    the year: a hand-built vintage need not sit on 1 January.
    """
    found = {}
    for name in bd.databases:
        metadata = bd.databases[name]
        if REPRESENTATIVE_TIME not in metadata or metadata.get(SCENARIOS):
            continue
        value = _normalize_representative_time(metadata[REPRESENTATIVE_TIME], name)
        if not isinstance(value, datetime):  # "dynamic"
            continue
        if not database_matches_scenario(metadata, filters):
            continue
        if not database_matches_sectors(metadata, sectors):
            continue
        found.setdefault(value.year, (name, value))
    return found


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


def vintage_name(filters: dict, year: int) -> str:
    """The database name a built vintage gets."""
    return (
        f"ei_{filters['system_model']}_{filters['ecoinvent_version']}_"
        f"{filters['iam_model']}_{filters['pathway']}_{year}"
    )


def _check_no_collisions(
    names: dict[int, str], filters: dict, sectors: list[str] | None = None
) -> None:
    """Refuse to build over a database that is not ours.

    `write_db_to_brightway` deletes and rewrites a database of the same name
    without asking. A name that exists here belongs to someone else: a name
    that matched the scenario would have satisfied its year already, and its
    year would not be in the build list.
    """
    wanted = {**filters, **({"sectors": sectors} if sectors else {})}
    colliding = [name for name in names.values() if name in bd.databases]
    if colliding:
        raise ValueError(
            f"Database(s) {colliding} already exists in this project but does "
            f"not match scenario {wanted!r}, and premise would overwrite them. "
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
    sectors = build.get("sectors")

    existing = find_existing_vintages(filters, sectors)
    missing = [year for year in years if year not in existing]

    if not missing:
        logger.info(
            f"All {len(years)} requested background vintage(s) already exist in "
            f"this project. Nothing to build."
        )
        return dict(existing[year] for year in years)

    names = {year: vintage_name(filters, year) for year in missing}
    _check_no_collisions(names, filters, sectors)

    key = _resolve_premise_key(premise_key)
    source_database, biosphere = _resolve_source_database(
        filters, build, ecoinvent_credentials
    )

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

    resolved = dict(existing[year] for year in years if year in existing)
    # premise writes 1 January of the scenario year.
    resolved.update({names[year]: datetime(year, 1, 1) for year in missing})
    return resolved
