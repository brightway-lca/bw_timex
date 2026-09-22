"""The bridge between premise background scenarios and Watanabe CF scenarios."""

import pytest
from bw2data.tests import bw2test

from bw_timex.prospective_scenarios import (
    PROSPECTIVE_SCENARIO_MAP,
    available_scenarios,
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
        {"iam_model": "image", "pathway": "SSP1-PkBudg500"}
    ) == IMAGE_LOW


def test_lookup_is_case_insensitive_on_iam_model():
    assert lookup_prospective_scenario(
        {"iam_model": "IMAGE", "pathway": "SSP1-PkBudg500"}
    ) == IMAGE_LOW


def test_lookup_ignores_extra_scenario_keys():
    assert lookup_prospective_scenario(
        {
            "iam_model": "image",
            "pathway": "SSP1-PkBudg500",
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
    first = lookup_prospective_scenario({"iam_model": "image", "pathway": "SSP1-PkBudg500"})
    first["rcp"] = "8.5"
    assert lookup_prospective_scenario(
        {"iam_model": "image", "pathway": "SSP1-PkBudg500"}
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
        scenario={"iam_model": "image", "pathway": "SSP1-PkBudg500"},
    ) == explicit


def test_table_is_used_when_nothing_explicit_is_given():
    assert resolve_characterization_scenario(
        metric="pGWP",
        characterization_scenario=None,
        scenario={"iam_model": "image", "pathway": "SSP1-PkBudg500"},
    ) == IMAGE_LOW


def test_session_default_is_used_below_the_table():
    from dynamic_characterization.prospective import reset_scenario, set_scenario

    set_scenario(iam="MESSAGE", ssp="SSP2", rcp="4.5")
    try:
        # table wins over the session default
        assert resolve_characterization_scenario(
            metric="pGWP",
            characterization_scenario=None,
            scenario={"iam_model": "image", "pathway": "SSP1-PkBudg500"},
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


@pytest.mark.skipif(not _premise_installed(), reason="premise is not installed")
def test_every_table_key_is_a_real_premise_pairing():
    """Would have caught the SSP1-RCP19/RCP26/NDC entries: those pathway
    names are not in premise's own catalogue, so PROSPECTIVE_SCENARIO_MAP
    must only ever name IAMs and pathways premise actually supports."""
    import yaml
    from pathlib import Path

    import premise

    path = Path(premise.__file__).parent / "iam_variables_mapping" / "constants.yaml"
    data = yaml.safe_load(path.read_text())
    supported_models = {str(model).lower() for model in data["SUPPORTED_MODELS"]}
    supported_pathways = set(data["SUPPORTED_PATHWAYS"])

    for iam_model, pathway in PROSPECTIVE_SCENARIO_MAP:
        assert iam_model in supported_models, (
            f"{iam_model!r} is not one of premise's SUPPORTED_MODELS"
        )
        assert pathway in supported_pathways, (
            f"{pathway!r} is not one of premise's SUPPORTED_PATHWAYS"
        )


def test_columns_and_shape():
    table = available_scenarios()
    assert list(table.columns) == EXPECTED_COLUMNS
    assert len(table) > 0


def test_a_shared_pairing_is_ticked_on_both_sides():
    table = available_scenarios()
    row = table[
        (table.iam_model == "image") & (table.pathway == "SSP1-PkBudg500")
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
    # simply absent from the premise catalogue. Whether the intersection is
    # non-empty depends on what premise's local install supports, so this
    # only asserts the subset invariant, not a row count.
    shared = {(row.iam_model, row.pathway) for row in table.itertuples()}
    assert shared <= set(PROSPECTIVE_SCENARIO_MAP)

    # Specific mappings, asserted directly through the table rather than
    # through whatever happens to intersect with the installed premise.
    assert PROSPECTIVE_SCENARIO_MAP[("image", "SSP1-PkBudg500")] == {
        "iam": "IMAGE",
        "ssp": "SSP1",
        "rcp": "2.6",
    }
    assert PROSPECTIVE_SCENARIO_MAP[("message", "SSP2-RCP26")] == {
        "iam": "MESSAGE",
        "ssp": "SSP2",
        "rcp": "2.6",
    }


def test_usable_for_rejects_an_unknown_value():
    with pytest.raises(ValueError):
        available_scenarios(usable_for="nonsense")


@bw2test
def test_in_project_is_empty_without_matching_databases():
    # The test project holds no premise-built vintages.
    table = available_scenarios(usable_for="both")
    assert (table.in_project == "").all()
