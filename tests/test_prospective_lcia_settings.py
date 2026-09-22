"""The prospective scenario travels with the settings, not with module state."""

from datetime import datetime
from unittest.mock import patch

import bw2data as bd
import pandas as pd
import pytest

from bw_timex import TimexLCA, TimexLCASettings


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
