"""The bridge between premise background scenarios and Watanabe CF scenarios.

`PROSPECTIVE_SCENARIO_MAP` (a whole `(iam_model, pathway)` -> whole `(iam, ssp,
rcp)` table) has been replaced by `PROSPECTIVE_IAM_MAP` plus per-axis
derivation (`bw_timex/prospective_scenarios.py`, AMENDMENT section of
`docs/superpowers/specs/2026-09-22-prospective-scenario-integration-design.md`).
The old table asserted an RCP for pathways named after carbon budgets, policy
assumptions or ScenarioMIP warming levels - premise's own metadata does not
support that (no RCP field, and only 3 of premise's 33 pathways literally name
an RCP), so the old table entries and the tests asserting them are gone.
"""

import pytest
from bw2data.tests import bw2test

from bw_timex.prospective_scenarios import (
    PROSPECTIVE_IAM_MAP,
    available_scenarios,
    derive_prospective_scenario,
    resolve_characterization_scenario,
)

MESSAGE_RCP26 = {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"}


def test_iam_map_only_has_iams_watanabe_pairs():
    assert PROSPECTIVE_IAM_MAP == {
        "image": {"iam": "IMAGE", "ssp": "SSP1"},
        "message": {"iam": "MESSAGE", "ssp": "SSP2"},
        "remind": {"iam": "REMIND", "ssp": "SSP5"},
    }


def test_full_derivation_hits_an_rcp_named_pathway():
    assert derive_prospective_scenario(
        {"iam_model": "message", "pathway": "SSP2-RCP26"}
    ) == MESSAGE_RCP26


def test_full_derivation_is_case_insensitive_on_iam_model():
    assert derive_prospective_scenario(
        {"iam_model": "MESSAGE", "pathway": "SSP2-RCP26"}
    ) == MESSAGE_RCP26


def test_full_derivation_ignores_extra_scenario_keys():
    assert derive_prospective_scenario(
        {
            "iam_model": "message",
            "pathway": "SSP2-RCP26",
            "system_model": "cutoff",
            "ecoinvent_version": "3.10.1",
            "years": [2020, 2030],
        }
    ) == MESSAGE_RCP26


def test_full_derivation_of_no_scenario_is_none():
    assert derive_prospective_scenario(None) is None
    assert derive_prospective_scenario({}) is None


@pytest.mark.parametrize(
    "suffix", ["PkBudg500", "PkBudg650", "NPi", "Base", "rollBack", "L", "VLHO"]
)
def test_budget_policy_and_warming_level_pathways_never_yield_an_rcp(suffix):
    # image/SSP1-<suffix> derives iam and ssp (IMAGE, SSP1) but never an RCP:
    # none of these names one, and the old table's substitution (Base -> the
    # highest RCP, budgets -> "the closest" RCP) is exactly what this change
    # removes.
    assert derive_prospective_scenario(
        {"iam_model": "image", "pathway": f"SSP1-{suffix}"}
    ) is None


def test_rcp19_is_parsed_but_rejected_rather_than_rounded():
    # SSP2-RCP19 literally names an RCP, but Watanabe et al. do not provide
    # RCP1.9 for MESSAGE-SSP2 (only 2.6/4.5/6.0/8.5); the nearest RCP must not
    # be substituted.
    assert derive_prospective_scenario(
        {"iam_model": "message", "pathway": "SSP2-RCP19"}
    ) is None


def test_iam_ssp_mismatch_derives_nothing():
    # The very common premise setup remind + SSP2-* has no Watanabe pairing:
    # REMIND is paired with SSP5, not SSP2.
    assert derive_prospective_scenario(
        {"iam_model": "remind", "pathway": "SSP2-PkBudg500"}
    ) is None


@pytest.mark.parametrize("iam_model", ["remind-eu", "tiam-ucl", "gcam", "witch"])
def test_iam_models_with_no_watanabe_counterpart_derive_nothing(iam_model):
    assert derive_prospective_scenario(
        {"iam_model": iam_model, "pathway": "SSP2-RCP26"}
    ) is None


def test_derivation_returns_a_fresh_dict():
    first = derive_prospective_scenario({"iam_model": "message", "pathway": "SSP2-RCP26"})
    first["rcp"] = "8.5"
    assert derive_prospective_scenario(
        {"iam_model": "message", "pathway": "SSP2-RCP26"}
    ) == MESSAGE_RCP26


def test_non_prospective_metric_resolves_to_none():
    assert resolve_characterization_scenario(
        metric="GWP",
        characterization_scenario=None,
        scenario={"iam_model": "remind", "pathway": "SSP2-PkBudg500"},
    ) is None


def test_explicit_full_scenario_wins_over_derivation():
    explicit = {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario=explicit,
        scenario={"iam_model": "message", "pathway": "SSP2-RCP26"},
    ) == explicit


def test_derivation_is_used_when_nothing_explicit_is_given():
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario=None,
        scenario={"iam_model": "message", "pathway": "SSP2-RCP26"},
    ) == MESSAGE_RCP26


def test_partial_characterization_scenario_is_filled_in_from_derivation():
    # message/SSP2-L derives iam=MESSAGE, ssp=SSP2 but no rcp (L is a
    # ScenarioMIP warming level). Supplying only the missing axis is enough.
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario={"rcp": "2.6"},
        scenario={"iam_model": "message", "pathway": "SSP2-L"},
    ) == MESSAGE_RCP26


def test_partial_characterization_scenario_key_wins_over_derivation():
    # The background would derive rcp=2.6 (SSP2-RCP26), but an explicit rcp
    # overrides it.
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario={"rcp": "4.5"},
        scenario={"iam_model": "message", "pathway": "SSP2-RCP26"},
    ) == {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}


def test_session_default_is_used_below_full_derivation():
    from dynamic_characterization.prospective import reset_scenario, set_scenario

    set_scenario(iam="MESSAGE", ssp="SSP2", rcp="4.5")
    try:
        # full derivation wins over the session default
        assert resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "message", "pathway": "SSP2-RCP26"},
        ) == MESSAGE_RCP26
        # session default is used when there is nothing to derive from
        assert resolve_characterization_scenario(
            metric="pGWP", characterization_scenario=None, scenario=None
        ) == {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "4.5"}
    finally:
        reset_scenario()


def test_session_default_is_used_when_only_rcp_is_missing_and_unset_explicitly():
    # A background that derives iam/ssp but not rcp (message/SSP2-L) still
    # raises rather than silently pulling in the session default: the session
    # default sits below *full* derivation, but a partial derivation with no
    # explicit characterization_scenario does not resolve at all - it is not
    # "full derivation", so falls through to the session default, exactly like
    # a background with nothing derived at all.
    from dynamic_characterization.prospective import reset_scenario, set_scenario

    set_scenario(iam="MESSAGE", ssp="SSP2", rcp="8.5")
    try:
        assert resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "message", "pathway": "SSP2-L"},
        ) == {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "8.5"}
    finally:
        reset_scenario()


def test_unmappable_iam_model_raises_with_per_axis_diagnostics():
    from dynamic_characterization.prospective import reset_scenario

    reset_scenario()
    with pytest.raises(ValueError) as error:
        resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "remind-eu", "pathway": "SSP2-NDC"},
        )
    message = str(error.value)
    assert "remind-eu" in message and "SSP2-NDC" in message
    assert "characterization_scenario" in message
    assert "available_scenarios" in message
    # Per-axis diagnostics: iam fails (no Watanabe counterpart), ssp succeeds
    # (read straight off the pathway prefix, independent of iam_model), rcp
    # fails ('NDC' is a policy assumption).
    assert "iam  -> no match" in message
    assert "ssp  -> SSP2, from the pathway prefix." in message
    assert "rcp  -> no match: 'NDC' is a policy assumption, not an RCP." in message
    assert "SSP2-RCP19 / SSP2-RCP26 / SSP2-RCP45" in message


def test_only_rcp_missing_raises_with_iam_and_ssp_shown_as_derived():
    from dynamic_characterization.prospective import reset_scenario

    reset_scenario()
    with pytest.raises(ValueError) as error:
        resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "message", "pathway": "SSP2-L"},
        )
    message = str(error.value)
    assert "iam  -> MESSAGE, from iam_model." in message
    assert "ssp  -> SSP2, from the pathway prefix." in message
    assert "rcp  -> no match: 'L' is a ScenarioMIP warming level, not an RCP." in message
    assert "MESSAGE-SSP2 is available for RCPs 2.6, 4.5, 6.0, 8.5." in message
    # The suggested fix is the partial form the spec says "alone suffices"
    # here, not a full triple naming some unrelated IAM/SSP.
    assert '"characterization_scenario": {"rcp": "2.6"}' in message


@pytest.mark.parametrize(
    "iam_model, pathway, iam, ssp, rcps",
    [
        ("image", "SSP1-Base", "IMAGE", "SSP1", "2.6, 4.5, 8.5"),
        ("remind", "SSP5-Base", "REMIND", "SSP5", "2.6, 4.5, 6.0, 8.5"),
    ],
)
def test_suggested_override_names_the_backgrounds_own_iam_ssp_not_message_ssp2(
    iam_model, pathway, iam, ssp, rcps
):
    # Regression test: the suggested "State the scenario explicitly" snippet
    # used to be a hardcoded {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"}
    # regardless of what actually derived. For a background whose iam/ssp
    # derive exactly (only rcp missing, as here), pasting that hardcoded
    # snippet would have silently switched the run to an unrelated IAM/SSP.
    # The snippet must instead either name *this* background's own iam/ssp,
    # or (as it does today) use the partial {"rcp": ...} form and omit
    # iam/ssp entirely, since they already derive.
    from dynamic_characterization.prospective import reset_scenario

    reset_scenario()
    with pytest.raises(ValueError) as error:
        resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": iam_model, "pathway": pathway},
        )
    message = str(error.value)
    assert f"{iam}-{ssp} is available for RCPs {rcps}." in message
    assert f"{iam}-{ssp} already derives from your background" in message
    # The two most likely wrong, unrelated IAM/SSP names must never appear as
    # a suggestion when they aren't this background's own.
    for other_iam, other_ssp in {("MESSAGE", "SSP2"), ("REMIND", "SSP5"), ("IMAGE", "SSP1")} - {
        (iam, ssp)
    }:
        assert f'"iam": "{other_iam}"' not in message
        assert f'"ssp": "{other_ssp}"' not in message
    assert '"characterization_scenario": {"rcp":' in message


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


def _premise_installed() -> bool:
    try:
        import premise  # noqa: F401
    except ImportError:
        return False
    return True


def test_columns_and_shape():
    table = available_scenarios()
    assert list(table.columns) == EXPECTED_COLUMNS
    assert len(table) > 0


@pytest.mark.skipif(not _premise_installed(), reason="premise is not installed")
def test_a_fully_derived_pairing_is_ticked_on_both_sides():
    table = available_scenarios()
    matches = table[
        (table.iam_model == "message") & (table.pathway == "SSP2-RCP26")
    ]
    if matches.empty:
        pytest.skip(
            "the installed premise's catalogue has no message/SSP2-RCP26 "
            "pathway; nothing to check here"
        )
    row = matches.iloc[0]
    assert row.premise and row.prospective
    assert (row.iam, row.ssp, row.rcp) == ("MESSAGE", "SSP2", "2.6")


@pytest.mark.skipif(not _premise_installed(), reason="premise is not installed")
def test_a_partly_derived_pairing_gets_iam_ssp_but_not_rcp_or_prospective():
    # image/SSP1-PkBudg500 used to assert rcp=2.6 in the old table; now iam
    # and ssp derive (IMAGE, SSP1) but rcp does not (PkBudg500 is a carbon
    # budget, not an RCP), so the row is genuinely only partly usable: iam/ssp
    # are filled in (not NA) so a reader can see it needs only an explicit
    # rcp, but prospective stays False and rcp stays NA since pGWP cannot run
    # on this row with no further input.
    table = available_scenarios()
    matches = table[
        (table.iam_model == "image") & (table.pathway == "SSP1-PkBudg500")
    ]
    if matches.empty:
        pytest.skip(
            "the installed premise's catalogue has no image/SSP1-PkBudg500 "
            "pathway; nothing to check here"
        )
    row = matches.iloc[0]
    assert row.premise
    assert not row.prospective
    assert row.iam == "IMAGE"
    assert row.ssp == "SSP1"
    import pandas as pd

    assert pd.isna(row.rcp)


def test_a_totally_unmapped_pairing_gets_no_iam_or_ssp_either():
    table = available_scenarios()
    matches = table[
        (table.iam_model == "remind") & (table.pathway == "SSP2-PkBudg500")
    ]
    if matches.empty:
        pytest.skip(
            "the installed premise's catalogue has no remind/SSP2-PkBudg500 "
            "pathway; nothing to check here"
        )
    row = matches.iloc[0]
    assert row.premise
    assert not row.prospective
    import pandas as pd

    assert pd.isna(row.iam)
    assert pd.isna(row.ssp)
    assert pd.isna(row.rcp)


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
    # Every shared row is one where the full triple derives.
    for row in table.itertuples():
        assert derive_prospective_scenario(
            {"iam_model": row.iam_model, "pathway": row.pathway}
        ) == {"iam": row.iam, "ssp": row.ssp, "rcp": row.rcp}


def test_usable_for_rejects_an_unknown_value():
    with pytest.raises(ValueError):
        available_scenarios(usable_for="nonsense")


@bw2test
def test_in_project_is_empty_without_matching_databases():
    # The test project holds no premise-built vintages.
    table = available_scenarios(usable_for="both")
    assert (table.in_project == "").all()
