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
