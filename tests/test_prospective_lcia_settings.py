"""The prospective scenario travels with the settings, not with module state."""

from datetime import datetime
from unittest.mock import patch

import bw2data as bd
import pandas as pd
import pytest

from bw_timex import TimexLCA, TimexLCASettings, set_database_metadata


@pytest.fixture
def electric_vehicle_lci(temporal_grouping_db_monthly):
    """A `TimexLCA` that has run `lci()`, ready for `dynamic_lcia()`.

    Follows the same fixture/database setup as
    `TestSettingsAndRun.setup` in `tests/test_timex_lca_run.py`: the
    `temporal_grouping_db_monthly` database fixture, `foreground:A` as the
    functional unit, and the `db_2022`/`db_2024`/`foreground` background
    mapping used throughout that test module.
    """
    fu = bd.get_node(database="foreground", code="A")
    database_dates = {
        "db_2022": datetime(2022, 1, 1),
        "db_2024": datetime(2024, 1, 1),
        "foreground": "dynamic",
    }
    tlca = TimexLCA(
        demand={fu.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(starting_datetime=datetime(2024, 1, 2))
    tlca.lci()
    return tlca


@pytest.fixture
def electric_vehicle_lci_with_background(temporal_grouping_db_monthly):
    """Like `electric_vehicle_lci`, but with a premise `scenario` attached.

    `TimexLCA` refuses `database_dates` and `scenario` together (`scenario`
    only ever *filters* databases resolved from their own metadata, so it is
    meaningless once `database_dates` already gives the whole mapping - see
    `TimexLCA._resolve_database_dates`). So the dates here are declared via
    `set_database_metadata` instead, exactly as `scenario` alone would
    require it, and `scenario={"iam_model": "image", "pathway":
    "SSP1-PkBudg500"}` both filters and selects the scenario: it derives
    `iam`/`ssp` exactly (image/SSP1-PkBudg500 -> IMAGE-SSP1), but not `rcp`
    (`PkBudg500` is a carbon budget, not an RCP) - the case a partial
    `characterization_scenario` is meant for.
    """
    set_database_metadata(
        "db_2022",
        representative_time=datetime(2022, 1, 1),
        iam_model="image",
        pathway="SSP1-PkBudg500",
    )
    set_database_metadata(
        "db_2024",
        representative_time=datetime(2024, 1, 1),
        iam_model="image",
        pathway="SSP1-PkBudg500",
    )
    set_database_metadata("foreground", representative_time="dynamic")

    fu = bd.get_node(database="foreground", code="A")
    tlca = TimexLCA(
        demand={fu.key: 1},
        method=("GWP", "example"),
        scenario={"iam_model": "image", "pathway": "SSP1-PkBudg500"},
    )
    tlca.build_timeline(starting_datetime=datetime(2024, 1, 2))
    tlca.lci()
    return tlca


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
            dynamic_lcia_enabled=True,
            characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
        )
        tlca.run(
            metric="pGWP",
            dynamic_lcia_enabled=True,
            characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
        )
    assert tlca.current_characterization_scenario["rcp"] == "8.5"


def test_partial_characterization_scenario_reaches_characterize_via_dynamic_lcia(
    electric_vehicle_lci_with_background,
):
    """Regression test: a partial `characterization_scenario` used to be
    rejected by `DynamicLCIAInputs.validate_characterization_scenario` before
    `dynamic_lcia` ever called `resolve_characterization_scenario`, so the
    merge-with-derived-axes path in `prospective_scenarios.py` was
    unreachable from every public entry point (`dynamic_lcia`, `run`,
    `compare` all construct `DynamicLCIAInputs` first). Only `rcp` is
    supplied here; `iam`/`ssp` must be derived from the background
    (`image`/`SSP1-PkBudg500` -> IMAGE-SSP1) and merged in.
    """
    tlca = electric_vehicle_lci_with_background

    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.dynamic_lcia(metric="pGWP", characterization_scenario={"rcp": "2.6"})

    expected = {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}
    assert characterize.call_args.kwargs["scenario"] == expected
    assert tlca.current_characterization_scenario == expected


def test_partial_characterization_scenario_reaches_characterize_via_run(
    electric_vehicle_lci_with_background,
):
    """Same regression as above, through the `run()` entry point."""
    tlca = electric_vehicle_lci_with_background

    with patch("bw_timex.timex_lca.characterize") as characterize:
        characterize.return_value = pd.DataFrame(
            columns=["date", "amount", "flow", "activity"]
        )
        tlca.run(
            metric="pGWP",
            dynamic_lcia_enabled=True,
            characterization_scenario={"rcp": "2.6"},
        )

    expected = {"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"}
    assert characterize.call_args.kwargs["scenario"] == expected
    assert tlca.current_characterization_scenario == expected
