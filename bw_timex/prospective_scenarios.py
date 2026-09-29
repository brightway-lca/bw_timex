"""Bridging premise background scenarios to prospective characterization scenarios.

The two packages describe a future differently. premise labels a database with
`iam_model` and `pathway` (`"image"`, `"SSP1-RCP26"`); the prospective
characterization factors of Watanabe et al. (2026) are indexed by `iam`, `ssp`
and `rcp` (`"IMAGE"`, `"SSP1"`, `"2.6"`). This module derives one from the
other, axis by axis, and holds the rule for deciding which characterization
scenario a run uses.

Earlier versions of this module held a single table mapping a whole premise
`(iam_model, pathway)` pair to a whole Watanabe `(iam, ssp, rcp)` triple. That
required asserting an RCP for pathways named after carbon budgets
(`PkBudg650`), policy assumptions (`NDC`, `NPi`, `Base`, `rollBack`) or
ScenarioMIP warming levels (`L`, `M`, `VLHO`, ...) - an undisclosed scientific
choice premise's own data does not support: premise records only
`{iam_model, pathway, ...}` on a database, no RCP field, and of its 33
pathways only three (`SSP2-RCP19`, `SSP2-RCP26`, `SSP2-RCP45`) literally name
one.

So each axis is now derived independently and only when exact:

- `iam` / `ssp` - Watanabe et al. pair each IAM with a single SSP (IMAGE-SSP1,
  MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4, REMIND-SSP5); premise does not. An
  `iam_model` bw_timex recognises (`image`, `message`, `remind`) only yields
  `iam`/`ssp` when the pathway's own SSP prefix agrees with the SSP that IAM
  is paired with. `remind-eu`, `tiam-ucl`, `gcam`, `witch`, and any IAM/SSP
  mismatch such as `remind` + `SSP2-*`, have no counterpart and derive
  nothing.
- `rcp` - derived only when the pathway suffix literally names one
  (`SSP2-RCP26` -> `"2.6"`) *and* Watanabe et al. provide that RCP for the
  matching SSP. `SSP2-RCP19` -> `"1.9"` is parsed but then rejected: Watanabe
  et al. do not have an RCP1.9 scenario, and the nearest one is not
  substituted. Budgets, policies and warming levels never yield an RCP.

Nothing is inferred beyond that. `available_scenarios()` lists both
catalogues side by side, and every error raised here points at it.
"""

import re
from typing import Dict, Optional, Tuple

from dynamic_characterization.prospective import VALID_SCENARIOS, get_scenario
from loguru import logger

#: Metrics that need a prospective characterization scenario.
PROSPECTIVE_METRICS = frozenset(
    {"pGWP", "pGTP", "prospective_radiative_forcing"}
)

#: premise `iam_model` (lowercase) -> the Watanabe `iam`/`ssp` it is paired
#: with. Only IAMs Watanabe et al. (2026) actually parameterise are here.
#: `remind-eu`, `tiam-ucl`, `gcam` and `witch` are deliberately absent: they
#: have no Watanabe counterpart, so there is nothing to derive from them.
PROSPECTIVE_IAM_MAP: Dict[str, Dict[str, str]] = {
    "image": {"iam": "IMAGE", "ssp": "SSP1"},
    "message": {"iam": "MESSAGE", "ssp": "SSP2"},
    "remind": {"iam": "REMIND", "ssp": "SSP5"},
}

_PAIRINGS = "IMAGE (SSP1), MESSAGE (SSP2), AIM (SSP3), GCAM4 (SSP4), REMIND (SSP5)"

#: The only three premise pathways whose name literally contains an RCP.
_KNOWN_RCP_NAMED_PATHWAYS = "SSP2-RCP19 / SSP2-RCP26 / SSP2-RCP45"

_SSP_PREFIX_RE = re.compile(r"^(SSP\d)", re.IGNORECASE)
_RCP_SUFFIX_RE = re.compile(r"^RCP(\d{2})$", re.IGNORECASE)
_BUDGET_SUFFIX_RE = re.compile(r"^PkBudg\d+$", re.IGNORECASE)
_POLICY_SUFFIXES = {"ndc", "npi", "base", "rollback"}
_WARMING_LEVEL_SUFFIXES = {"l", "m", "ml", "vl", "vllo", "vlho", "lo", "h"}

#: A handful of premise `iam_model` spellings that are recognisable but still
#: have no Watanabe counterpart, so the error can say something more specific
#: than "unrecognised".
_IAM_NOTE_OVERRIDES = {
    "remind-eu": "is a regional REMIND variant",
    "tiam-ucl": "is a TIMES-family model, not one Watanabe et al. calibrate",
    "gcam": "has no Watanabe counterpart",
    "witch": "has no Watanabe counterpart",
}


def _format_rcp(digits: str) -> str:
    """`"26"` -> `"2.6"`, `"19"` -> `"1.9"`, `"60"` -> `"6.0"`."""
    return f"{digits[0]}.{digits[1:]}"


def _valid_rcps_for_ssp(ssp: Optional[str]) -> list:
    """The RCPs Watanabe et al. provide for a given SSP, sorted."""
    return sorted({rcp for (_, s, rcp) in VALID_SCENARIOS if s == ssp})


def _watanabe_iam_for_ssp(ssp: Optional[str]) -> Optional[str]:
    """The one IAM Watanabe et al. pair with `ssp`, or None."""
    for iam, s, _ in VALID_SCENARIOS:
        if s == ssp:
            return iam
    return None


def _iam_unrecognized_reason(iam_model_raw: str, iam_model_norm: str) -> str:
    tail = _IAM_NOTE_OVERRIDES.get(iam_model_norm, "has no Watanabe counterpart")
    return (
        f"no match: '{iam_model_raw}' {tail}; Watanabe et al. (2026) parameterise "
        f"{_PAIRINGS}."
    )


def _rcp_failure_reason(suffix: str) -> str:
    """Why `suffix` (the pathway text after the SSP prefix) does not name an RCP."""
    suffix_lower = suffix.lower()
    rcp_match = _RCP_SUFFIX_RE.match(suffix)
    if rcp_match:
        # It does look like an RCP; the caller has already checked that the
        # resulting triple is not one Watanabe et al. provide.
        return (
            f"no match: {suffix!r} names RCP{_format_rcp(rcp_match.group(1))}, "
            f"which Watanabe et al. (2026) do not provide for this SSP."
        )
    if _BUDGET_SUFFIX_RE.match(suffix):
        kind = "a carbon budget"
    elif suffix_lower in _POLICY_SUFFIXES:
        kind = "a policy assumption"
    elif suffix_lower in _WARMING_LEVEL_SUFFIXES:
        kind = "a ScenarioMIP warming level"
    else:
        kind = None
    if kind is None:
        return f"no match: {suffix!r} does not name an RCP."
    return (
        f"no match: {suffix!r} is {kind}, not an RCP. Only premise's "
        f"{_KNOWN_RCP_NAMED_PATHWAYS} name one."
    )


class _Axis:
    """One derived axis: either a value with where it came from, or None with why not."""

    __slots__ = ("value", "detail")

    def __init__(self, value: Optional[str], detail: str):
        self.value = value
        self.detail = detail

    def line(self, axis_name: str) -> str:
        if self.value:
            return f"  {axis_name:<3}  -> {self.value}, {self.detail}"
        return f"  {axis_name:<3}  -> {self.detail}"


def _derive_axes(iam_model: Optional[str], pathway: Optional[str]) -> Dict[str, _Axis]:
    """Per-axis derivation of `(iam, ssp, rcp)` from a premise `(iam_model, pathway)`.

    Each axis derives independently and only when exact - see the module
    docstring. `ssp` is read straight off the pathway's prefix regardless of
    whether `iam_model` matches it; that mismatch is what makes `iam` fail to
    derive for e.g. `remind` + `SSP2-*`, without hiding that the SSP itself
    was perfectly readable.
    """
    pathway = pathway or ""
    ssp_match = _SSP_PREFIX_RE.match(pathway)
    ssp_value = ssp_match.group(1).upper() if ssp_match else None
    suffix = pathway[ssp_match.end():].lstrip("-") if ssp_match else pathway

    if ssp_value is None:
        ssp_axis = _Axis(None, f"no match: {pathway!r} does not start with an SSP number.")
    else:
        ssp_axis = _Axis(ssp_value, "from the pathway prefix.")

    # Both call sites (`_axes_for_scenario`, `available_scenarios`'s catalogue
    # loop) only ever pass a truthy `iam_model`, so there is no "no iam_model
    # at all" branch here to worry about.
    iam_model_norm = str(iam_model).lower()
    iam_entry = PROSPECTIVE_IAM_MAP.get(iam_model_norm)

    if iam_entry is None:
        iam_axis = _Axis(None, _iam_unrecognized_reason(iam_model, iam_model_norm))
    elif ssp_value != iam_entry["ssp"]:
        iam_axis = _Axis(
            None,
            f"no match: {iam_model_norm!r} pairs with {iam_entry['ssp']} in "
            f"Watanabe et al. (2026), not {ssp_value or 'this pathway'}.",
        )
    else:
        iam_axis = _Axis(iam_entry["iam"], "from iam_model.")

    if not suffix:
        rcp_axis = _Axis(None, "no match: pathway has no suffix after the SSP prefix.")
    else:
        rcp_match = _RCP_SUFFIX_RE.match(suffix)
        if rcp_match:
            rcp_value = _format_rcp(rcp_match.group(1))
            if ssp_value and rcp_value in _valid_rcps_for_ssp(ssp_value):
                rcp_axis = _Axis(rcp_value, f"from the pathway suffix {suffix!r}.")
            else:
                rcp_axis = _Axis(None, _rcp_failure_reason(suffix))
        else:
            rcp_axis = _Axis(None, _rcp_failure_reason(suffix))

    return {"iam": iam_axis, "ssp": ssp_axis, "rcp": rcp_axis}


def _axes_for_scenario(scenario: Optional[dict]) -> Dict[str, _Axis]:
    if not scenario or not scenario.get("iam_model") or not scenario.get("pathway"):
        no_scenario = _Axis(None, "no match: no background scenario to derive from.")
        return {"iam": no_scenario, "ssp": no_scenario, "rcp": no_scenario}
    return _derive_axes(scenario["iam_model"], scenario["pathway"])


def derive_prospective_scenario(scenario: Optional[dict]) -> Optional[Dict[str, str]]:
    """The full Watanabe scenario a premise background derives, or None.

    Parameters
    ----------
    scenario : dict or None
        A `TimexLCASettings.scenario`, i.e. premise metadata. Only `iam_model`
        and `pathway` are read; build keys like `years` are ignored.

    Returns
    -------
    dict or None
        `{"iam": ..., "ssp": ..., "rcp": ...}` only when all three axes derive
        exactly (see the module docstring); otherwise None, even when one or
        two axes derived fine. Use `available_scenarios()` or
        `resolve_characterization_scenario`'s error message to see which axis
        did not.
    """
    axes = _axes_for_scenario(scenario)
    if axes["iam"].value and axes["ssp"].value and axes["rcp"].value:
        return {
            "iam": axes["iam"].value,
            "ssp": axes["ssp"].value,
            "rcp": axes["rcp"].value,
        }
    return None


def _validate(scenario: Dict[str, str]) -> Dict[str, str]:
    """Reject a characterization scenario Watanabe et al. do not provide."""
    missing = {"iam", "ssp", "rcp"} - set(scenario)
    if missing:
        raise ValueError(
            f"A characterization scenario needs the keys 'iam', 'ssp' and 'rcp'; "
            f"missing {sorted(missing)} in {scenario!r}. "
            f"bw_timex.available_scenarios() lists the options."
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


def resolve_characterization_scenario(
    metric: str,
    characterization_scenario: Optional[dict],
    scenario: Optional[dict],
) -> Optional[Dict[str, str]]:
    """Decide which prospective scenario a run characterizes with.

    Precedence, highest first: an explicit `characterization_scenario`
    (merged with whatever the background derives, for any key it leaves out),
    the full derivation from the background scenario, a merge of whatever
    derives with the session default set with
    `dynamic_characterization.prospective.set_scenario`.

    A partially-supplied `characterization_scenario` is meaningful: any key it
    leaves out is filled in from the background's derivation when that axis
    derives. `{"rcp": "2.6"}` on a `message` / `SSP2-L` background resolves to
    MESSAGE-SSP2-RCP2.6, because `iam` and `ssp` derive exactly there even
    though `rcp` does not.

    The session default sits below the background on purpose: someone who set
    it once in a notebook and then compares two backgrounds should get the two
    matching factor sets, not one stale set carried across both. It only ever
    *fills in* axes the background did not derive - it never overrides an
    axis that did derive. When fewer than three axes derive, the session
    default fills the rest; if the session default disagrees with an axis
    that did derive, the derived value wins and a warning names both.

    Returns None for a non-prospective metric, which then never derives
    anything and never raises over a missing pairing.
    """
    if metric not in PROSPECTIVE_METRICS:
        return None

    axes = _axes_for_scenario(scenario)
    derived = {axis: value.value for axis, value in axes.items() if value.value}

    if characterization_scenario:
        supplied_keys = set(characterization_scenario) & {"iam", "ssp", "rcp"}
        merged = dict(characterization_scenario)
        for axis, value in derived.items():
            merged.setdefault(axis, value)
        resolved = _validate(merged)
        filled_in = sorted({"iam", "ssp", "rcp"} - supplied_keys)
        source = (
            f"given as characterization_scenario, {', '.join(filled_in)} "
            f"from the background)"
            if filled_in
            else "given as characterization_scenario)"
        )
        logger.info(
            f"{metric}: characterization scenario "
            f"{resolved['iam']}-{resolved['ssp']}-RCP{resolved['rcp']} ({source}"
        )
        return resolved

    if len(derived) == 3:
        resolved = _validate(derived)
        logger.info(
            f"{metric}: characterization scenario "
            f"{resolved['iam']}-{resolved['ssp']}-RCP{resolved['rcp']} "
            f"derived from background {scenario['iam_model']} / {scenario['pathway']}"
        )
        return resolved

    try:
        from_session = get_scenario()
    except RuntimeError:
        from_session = None

    # Derived axes always win: the session default only fills in axes that
    # did not derive. A session value that disagrees with a derived axis is
    # overridden, not merged, and that override is warned about - it means
    # a stale `set_scenario()` from earlier in the session would otherwise
    # silently contradict what this background actually is.
    merged = dict(from_session) if from_session else {}
    conflicts = [
        (axis, merged[axis], value)
        for axis, value in derived.items()
        if axis in merged and merged[axis] != value
    ]
    merged.update(derived)

    if len(merged) == 3:
        for axis, session_value, derived_value in conflicts:
            logger.warning(
                f"{metric}: characterization axis {axis!r} derived as "
                f"{derived_value!r} from the background scenario, overriding "
                f"the session default {session_value!r} set with "
                f"set_scenario()."
            )
        resolved = _validate(merged)
        derived_axes = sorted(derived)
        session_axes = sorted(set(merged) - set(derived))
        parts = []
        if derived_axes:
            if scenario and scenario.get("iam_model") and scenario.get("pathway"):
                background_note = (
                    f" (background {scenario['iam_model']} / "
                    f"{scenario['pathway']})"
                )
            else:
                background_note = ""
            parts.append(
                f"{', '.join(derived_axes)} from the background{background_note}"
            )
        if session_axes:
            parts.append(
                f"{', '.join(session_axes)} from the session default set "
                f"with set_scenario()"
            )
        logger.info(
            f"{metric}: characterization scenario "
            f"{resolved['iam']}-{resolved['ssp']}-RCP{resolved['rcp']} "
            f"({'; '.join(parts)})"
        )
        return resolved

    raise ValueError(_unresolved_message(metric, scenario, axes))


def _suggested_override(metric: str, axes: Dict[str, _Axis]) -> Tuple[str, str]:
    """A `lcia={...}` snippet tailored to what did and did not derive.

    Returns `(note, snippet)`. Never suggests an IAM/SSP unrelated to what the
    background actually derived: when `iam`/`ssp` derived exactly and only
    `rcp` is missing, the suggestion is the partial form the spec says
    "alone suffices" - naming the *same* IAM/SSP the axes block above already
    showed, not a fixed example. When `iam`/`ssp` did not derive, the full
    triple offered is a *suggestion to confirm*, guessed from the pathway's
    SSP prefix when there is one (so it at least matches the SSP the user
    typed), and is labelled as a guess rather than presented as a fact.
    """
    iam_value, ssp_value = axes["iam"].value, axes["ssp"].value

    if iam_value and ssp_value:
        valid_rcps = _valid_rcps_for_ssp(ssp_value)
        suggested_rcp = valid_rcps[0] if valid_rcps else "2.6"
        note = (
            f"{iam_value}-{ssp_value} already derives from your background; "
            f"only rcp is missing, so this alone suffices:"
        )
        body = f'{{"rcp": "{suggested_rcp}"}}'
    else:
        guessed_ssp = ssp_value or "SSP2"
        guessed_iam = _watanabe_iam_for_ssp(guessed_ssp) or "MESSAGE"
        valid_rcps = _valid_rcps_for_ssp(guessed_ssp)
        suggested_rcp = valid_rcps[0] if valid_rcps else "2.6"
        basis = (
            f"the pathway's own SSP prefix ({guessed_ssp})"
            if ssp_value
            else "no information at all - replace every key"
        )
        note = (
            f"iam/ssp did not derive, so this is only a guess based on "
            f"{basis} - confirm it is the future you mean before using it:"
        )
        body = (
            f'{{"iam": "{guessed_iam}", "ssp": "{guessed_ssp}", '
            f'"rcp": "{suggested_rcp}"}}'
        )

    snippet = (
        f"    lcia={{\"metric\": \"{metric}\",\n"
        f"          \"characterization_scenario\": {body}}}"
    )
    return note, snippet


def _unresolved_message(
    metric: str, scenario: Optional[dict], axes: Dict[str, _Axis]
) -> str:
    """What to tell someone whose prospective metric has no scenario."""
    note, snippet = _suggested_override(metric, axes)

    if scenario and scenario.get("iam_model") and scenario.get("pathway"):
        axes_block = "\n".join(axes[axis].line(axis) for axis in ("iam", "ssp", "rcp"))

        hint = ""
        ssp_value = axes["ssp"].value
        if ssp_value:
            watanabe_iam = _watanabe_iam_for_ssp(ssp_value)
            available = ", ".join(_valid_rcps_for_ssp(ssp_value))
            if watanabe_iam and available:
                hint = f"\n\n{watanabe_iam}-{ssp_value} is available for RCPs {available}."

        return (
            f"metric={metric!r} needs a prospective characterization scenario, and the "
            f"background scenario {scenario['iam_model']} / {scenario['pathway']} does "
            f"not fully determine one.\n\n"
            f"{axes_block}\n\n"
            f"{note}\n\n{snippet}"
            f"{hint}\n\n"
            f"bw_timex.available_scenarios() lists both catalogues side by side."
        )
    return (
        f"metric={metric!r} needs a prospective characterization scenario, and this "
        f"TimexLCA has no background scenario to derive one from (it was built with "
        f"database_dates, or with no scenario at all).\n\n"
        f"{note}\n\n{snippet}\n\n"
        f"bw_timex.available_scenarios(usable_for='prospective') lists the options."
    )


import pandas as pd

_USABLE_FOR = {None, "premise", "prospective", "both"}


def _premise_catalogue() -> Tuple[set, bool]:
    """Every `(iam_model, pathway)` premise *accepts*, and whether that is complete.

    premise validates `NewDatabase(model=..., pathway=...)` against two lists,
    `SUPPORTED_MODELS` and `SUPPORTED_PATHWAYS`, defined in
    `premise/iam_variables_mapping/constants.yaml`, checked independently of
    each other - so the cross product taken here is the set of pairs premise
    will *accept as an argument*, not the set of pairs it can actually build.
    Whether the underlying scenario data exists is a separate question,
    decided by what premise's downloadable data archive
    (https://doi.org/10.5281/zenodo.21790981) contains for that model, which
    this function does not check: as of this writing that archive has no
    `message_SSP2-RCP26`/`RCP45` files at all, and its only RCP-named
    scenario data is `tiam-ucl_SSP2-RCP19/26/45` - so a pair marked accepted
    here can still fail with a missing-data error inside `NewDatabase`. See
    `available_scenarios`'s docstring.

    Falls back to whatever `(iam_model, pathway)` pairs this project's
    databases already declare, and reports `complete=False`, whenever premise
    is not installed or that file is missing, unreadable or missing either
    list: `available_scenarios()` must never raise because of this.
    """
    try:
        import premise
    except ImportError:
        return _project_declared_pairs(), False

    from pathlib import Path

    path = Path(premise.__file__).parent / "iam_variables_mapping" / "constants.yaml"
    if not path.is_file():
        return _project_declared_pairs(), False

    try:
        import yaml

        data = yaml.safe_load(path.read_text())
        models = data["SUPPORTED_MODELS"]
        # "static" is not a scenario pathway - it is premise's non-prospective
        # mode (no IAM projection applied at all), so it names no IAM/SSP/RCP
        # and does not belong in a table of scenarios.
        pathways = [p for p in data["SUPPORTED_PATHWAYS"] if p != "static"]
        catalogue = {
            (str(model).lower(), pathway) for model in models for pathway in pathways
        }
    except Exception as error:  # noqa: BLE001 - any parse/shape problem degrades, never raises
        logger.info(
            f"Could not read premise's scenario catalogue from {path} ({error!r}); "
            "falling back to the (iam_model, pathway) pairs this project's "
            "databases already declare."
        )
        return _project_declared_pairs(), False

    if not catalogue:
        return _project_declared_pairs(), False
    return catalogue, True


def _project_declared_pairs() -> set:
    """`(iam_model, pathway)` pairs already declared on this project's databases."""
    import bw2data as bd

    pairs = set()
    for name in bd.databases:
        metadata = bd.databases[name]
        iam_model = metadata.get("iam_model")
        pathway = metadata.get("pathway")
        if iam_model and pathway:
            pairs.add((str(iam_model).lower(), pathway))
    return pairs


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

    A premise `(iam_model, pathway)` row's `iam`/`ssp` columns are filled in
    whenever those two axes derive exactly, *even when `rcp` does not* - such
    a row is genuinely partly usable (an explicit
    `characterization_scenario={"rcp": ...}` is all it needs), so it is not
    shown as if nothing derived. `prospective` is only True, and `rcp` only
    filled in, when the full triple derives and the row needs no
    `characterization_scenario` at all.

    **`premise=True` means premise *accepts* that `(iam_model, pathway)` pair**
    as an argument to `NewDatabase` - it validates the model and the pathway
    against two independent lists, not against each other, and not against
    what scenario data it can actually download. It does **not** mean
    premise's data archive ships a file for that pair: as of this writing,
    premise's Zenodo archive (record 21790981) has no `message_SSP2-RCP26` or
    `message_SSP2-RCP45` files, and the only RCP-named scenario data it ships
    at all is `tiam-ucl_SSP2-RCP19/26/45` - so even the two rows this table
    marks `premise=True, prospective=True` (`usable_for="both"`) cannot be
    built end to end today; `NewDatabase` will fail on missing data before a
    database exists to run a prospective metric against. Treat `premise=True`
    as "premise will not reject this pair up front", not as "this background
    is buildable right now" - check premise's own archive, or try the build,
    to know that.

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
        such scenario), `iam`, `ssp`, `rcp` (the Watanabe side, NA when that
        axis does not derive), the flags `premise` and `prospective`, and
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
        axes = _derive_axes(iam_model, pathway)
        iam = axes["iam"].value
        ssp = axes["ssp"].value if iam else None
        rcp = axes["rcp"].value if iam else None
        rows.append(
            {
                "iam_model": iam_model,
                "pathway": pathway,
                "iam": iam if iam else pd.NA,
                "ssp": ssp if ssp else pd.NA,
                "rcp": rcp if rcp else pd.NA,
                "premise": True,
                "prospective": bool(iam and ssp and rcp),
                "in_project": _years_in_project(iam_model, pathway),
            }
        )

    paired = {
        (row["iam"], row["ssp"], row["rcp"]) for row in rows if row["prospective"]
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
            "read, so the premise side of this table lists only the "
            '(iam_model, pathway) pairs this project\'s databases already '
            'declare. Install it with: pip install "bw_timex[premise]"'
        )

    if usable_for == "premise":
        table = table[table.premise]
    elif usable_for == "prospective":
        table = table[table.prospective]
    elif usable_for == "both":
        table = table[table.premise & table.prospective]

    return table.reset_index(drop=True)
