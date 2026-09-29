"""Comparisons record and vary the characterization scenario."""

from dataclasses import replace
from datetime import datetime

import bw2data as bd
import pandas as pd
import pytest
from dynamic_characterization.ipcc_ar6.radiative_forcing import (
    characterize_co2 as characterize_co2_ipcc,
)
from dynamic_characterization.prospective.radiative_forcing import (
    characterize_co2 as characterize_co2_prospective,
)

from bw_timex import TimexLCA, TimexLCASettings


@pytest.fixture
def electric_vehicle_settings(temporal_grouping_db_monthly):
    """A `TimexLCASettings` for `TimexLCA.compare()`, ready for dynamic LCIA.

    Follows the same fixture/database setup as `electric_vehicle_lci` in
    `tests/test_prospective_lcia_settings.py` (Task 6), which in turn follows
    `TestSettingsAndRun.setup` in `tests/test_timex_lca_run.py`: the
    `temporal_grouping_db_monthly` database fixture, `foreground:A` as the
    functional unit, and the `db_2022`/`db_2024`/`foreground` background
    mapping used throughout that test module. Unlike that fixture, this one
    stops short of running anything - `compare()` builds and runs the
    `TimexLCA` itself.

    `dynamic_lcia_enabled=True` so `run()`/`compare()` actually characterize
    dynamically instead of silently skipping it (this fixture's `bio`
    database is not `biosphere3`, so the automatic characterization-function
    mapping finds nothing for it - see `tests/test_timex_lca_run.py:200`).
    `characterization_functions` defaults to the non-prospective IPCC AR6 CO2
    function, which has no scenario dependency of its own, so it is safe for
    both static metrics and, since the prospective metrics resolve their own
    scenario independently of it, prospective ones too. Tests that need the
    scenario-sensitive Watanabe function pass their own
    `characterization_functions` instead.
    """
    fu = bd.get_node(database="foreground", code="A")
    database_dates = {
        "db_2022": datetime(2022, 1, 1),
        "db_2024": datetime(2024, 1, 1),
        "foreground": "dynamic",
    }
    co2_id = bd.get_node(database="bio", code="CO2").id
    return TimexLCASettings(
        demand={fu.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
        starting_datetime=datetime(2024, 1, 2),
        dynamic_lcia_enabled=True,
        characterization_functions={co2_id: characterize_co2_ipcc},
    )


def test_summary_records_the_characterization_scenario(electric_vehicle_settings):
    base = replace(
        electric_vehicle_settings,
        metric="pGWP",
        characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
    )
    comparison = TimexLCA.compare([replace(base, label="low")])
    row = comparison.summary.iloc[0]
    assert (row.cf_iam, row.cf_ssp, row.cf_rcp) == ("IMAGE", "SSP1", "2.6")


def test_static_metric_rows_have_no_cf_columns_filled(electric_vehicle_settings):
    comparison = TimexLCA.compare(
        [replace(electric_vehicle_settings, metric="GWP", label="static")]
    )
    assert pd.isna(comparison.summary.iloc[0].cf_iam)


def test_two_characterization_scenarios_share_one_object(electric_vehicle_settings):
    """`prospective_radiative_forcing` (not `pGWP`) genuinely varies with the
    scenario for this fixture's single CO2 flow: `pGWP` divides the flow's
    forcing by the reference CO2 forcing computed with the same scenario, and
    for a pure-CO2 flow that ratio does not depend on which scenario was used.
    `prospective_radiative_forcing` returns the forcing series directly, so
    it differs between RCP2.6 and RCP8.5 and proves the per-call scoping
    actually reaches `dynamic_characterization`.
    """
    co2_id = bd.get_node(database="bio", code="CO2").id
    base = replace(
        electric_vehicle_settings,
        metric="prospective_radiative_forcing",
        characterization_functions={co2_id: characterize_co2_prospective},
    )
    comparison = TimexLCA.compare(
        [
            replace(
                base,
                characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
                label="RCP2.6",
            ),
            replace(
                base,
                characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "8.5"},
                label="RCP8.5",
            ),
        ],
        keep_objects=True,
    )
    assert comparison.objects["RCP2.6"] is comparison.objects["RCP8.5"]
    assert list(comparison.summary.cf_rcp) == ["2.6", "8.5"]
    scores = comparison.summary.dynamic_score
    assert scores.iloc[0] != scores.iloc[1]


def test_skipped_dynamic_lcia_row_does_not_leak_the_previous_rows_scenario(
    electric_vehicle_settings,
):
    """A row that shares one `TimexLCA` with a previous prospective row, but
    itself skips dynamic LCIA, must not report the previous row's
    `cf_iam`/`cf_ssp`/`cf_rcp` next to `dynamic_score=NaN`.

    Regression test for `current_characterization_scenario` not being
    cleared by `TimexLCA._clear_stale_results` alongside `current_metric`
    and `current_time_horizon`: without the fix, the second row here would
    still report `IMAGE`/`SSP1`/`2.6` from the first row even though it
    never ran `dynamic_lcia` itself.
    """
    co2_id = bd.get_node(database="bio", code="CO2").id
    base = replace(
        electric_vehicle_settings,
        characterization_functions={co2_id: characterize_co2_prospective},
    )
    comparison = TimexLCA.compare(
        [
            replace(
                base,
                metric="pGWP",
                characterization_scenario={"iam": "IMAGE", "ssp": "SSP1", "rcp": "2.6"},
                label="prospective",
            ),
            replace(
                base,
                metric="GWP",
                dynamic_lcia_enabled=False,
                label="skipped",
            ),
        ],
        keep_objects=True,
    )
    assert comparison.objects["prospective"] is comparison.objects["skipped"]

    skipped_row = comparison.summary[comparison.summary.label == "skipped"].iloc[0]
    assert pd.isna(skipped_row.dynamic_score)
    assert pd.isna(skipped_row.cf_iam)
    assert pd.isna(skipped_row.cf_ssp)
    assert pd.isna(skipped_row.cf_rcp)


def test_settings_differing_only_in_create_missing_do_not_share_an_object(
    electric_vehicle_settings,
):
    key_a = TimexLCA._background_key(replace(electric_vehicle_settings, create_missing=False))
    key_b = TimexLCA._background_key(replace(electric_vehicle_settings, create_missing=True))
    assert key_a != key_b
