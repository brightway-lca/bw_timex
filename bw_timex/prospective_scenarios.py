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
    #
    # premise's real IMAGE pathway catalogue (see
    # `premise/iam_variables_mapping/constants.yaml`, `SUPPORTED_PATHWAYS`) has
    # no "SSP1-RCP19", "SSP1-RCP26" or "SSP1-NDC": RCP-named and NDC pathways
    # only exist there for SSP2 (and SSP5 for NDC). Those three keys used to be
    # listed here but named pathways premise cannot build, so they were dead
    # entries; removed.
    ("image", "SSP1-PkBudg500"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    ("image", "SSP1-PkBudg650"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    ("image", "SSP1-PkBudg1000"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
    ("image", "SSP1-PkBudg1150"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
    ("image", "SSP1-NPi"): {"iam": "IMAGE", "ssp": "SSP1", "rcp": "4.5"},
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


import pandas as pd

_USABLE_FOR = {None, "premise", "prospective", "both"}


def _premise_catalogue() -> Tuple[set, bool]:
    """Every `(iam_model, pathway)` premise can build, and whether that is complete.

    premise validates `NewDatabase(model=..., pathway=...)` against two lists,
    `SUPPORTED_MODELS` and `SUPPORTED_PATHWAYS`, defined in
    `premise/iam_variables_mapping/constants.yaml`. That file - not the
    `data/iam_output_files` directory, which holds only the scenario files a
    given install happens to have already fetched - is premise's actual
    catalogue, so this reads it and takes the cross product of the two lists.

    Falls back to the pairings this module already knows about, and reports
    `complete=False`, whenever premise is not installed or that file is
    missing, unreadable or missing either list: `available_scenarios()` must
    never raise because of this.
    """
    try:
        import premise
    except ImportError:
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False

    from pathlib import Path

    path = Path(premise.__file__).parent / "iam_variables_mapping" / "constants.yaml"
    if not path.is_file():
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False

    try:
        import yaml

        data = yaml.safe_load(path.read_text())
        models = data["SUPPORTED_MODELS"]
        pathways = data["SUPPORTED_PATHWAYS"]
        catalogue = {
            (str(model).lower(), pathway) for model in models for pathway in pathways
        }
    except Exception as error:  # noqa: BLE001 - any parse/shape problem degrades, never raises
        logger.info(
            f"Could not read premise's scenario catalogue from {path} ({error!r}); "
            "falling back to the pairings bw_timex maps to prospective "
            "characterization factors."
        )
        return {key for key in PROSPECTIVE_SCENARIO_MAP}, False

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
            "premise is not installed, or its scenario catalogue could not be "
            "read, so the premise side of this table lists only the scenarios "
            'bw_timex maps to prospective characterization factors. Install it '
            'with: pip install "bw_timex[premise]"'
        )

    if usable_for == "premise":
        table = table[table.premise]
    elif usable_for == "prospective":
        table = table[table.prospective]
    elif usable_for == "both":
        table = table[table.premise & table.prospective]

    return table.reset_index(drop=True)
