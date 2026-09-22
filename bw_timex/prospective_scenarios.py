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
