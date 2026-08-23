"""Tests for TimexLCASettings, TimexLCA.run(), and TimexLCA.compare()."""

from dataclasses import replace
from datetime import datetime

import bw2data as bd
import pandas as pd
import pytest

from bw_timex import TimexLCA, TimexLCASettings, set_database_metadata


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSettingsAndRun:

    @pytest.fixture(autouse=True)
    def setup(self, temporal_grouping_db_monthly):
        self.fu = bd.get_node(database="foreground", code="A")
        self.database_dates = {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }
        self.method = ("GWP", "example")
        self.start = datetime(2024, 1, 2)

    def base_settings(self, **kwargs):
        defaults = dict(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=self.start,
            dynamic_lcia_enabled=False,
        )
        defaults.update(kwargs)
        return TimexLCASettings(**defaults)

    # ─── settings carry the whole calculation ───

    def test_settings_carry_identity_and_knobs(self):
        settings = self.base_settings(temporal_grouping="month", label="run A")
        assert settings.demand == {self.fu.key: 1}
        assert settings.method == self.method
        assert settings.database_dates == self.database_dates
        assert settings.scenario is None
        assert settings.temporal_grouping == "month"
        assert settings.label == "run A"

    def test_from_settings_builds_a_working_object(self):
        settings = self.base_settings()
        tlca = TimexLCA.from_settings(settings)

        assert tlca.demand == settings.demand
        assert tlca.method == settings.method
        assert tlca.settings is settings
        assert isinstance(tlca.base_score, float)

    # ─── run() ───

    def test_run_without_arguments_uses_the_objects_settings(self):
        tlca = TimexLCA.from_settings(self.base_settings())
        result = tlca.run()

        assert result is tlca  # method chaining
        assert hasattr(tlca, "timeline")
        assert isinstance(tlca.static_score, float)
        assert tlca.static_score != 0

    def test_run_kwargs_override_without_mutating_settings(self):
        settings = self.base_settings()
        tlca = TimexLCA.from_settings(settings)

        tlca.run(starting_datetime=datetime(2023, 1, 2))

        assert settings.starting_datetime == self.start
        assert tlca.settings.starting_datetime == self.start

    def test_run_accepts_a_replacement_settings_object(self):
        settings = self.base_settings()
        tlca = TimexLCA.from_settings(settings)

        tlca.run(replace(settings, temporal_grouping="month"))

        assert tlca.temporal_grouping == "month"

    def test_run_rejects_unknown_kwarg(self):
        tlca = TimexLCA.from_settings(self.base_settings())
        with pytest.raises(TypeError, match="not_a_setting"):
            tlca.run(not_a_setting=1)

    def test_run_rejects_changing_the_background(self):
        """database_dates fixes the matrix column space, so it cannot change per run."""
        tlca = TimexLCA.from_settings(self.base_settings())
        with pytest.raises(ValueError, match="database_dates"):
            tlca.run(database_dates={"db_2024": datetime(2024, 1, 1), "foreground": "dynamic"})

    def test_run_with_changed_demand_rebuilds_base_lca(self):
        tlca = TimexLCA.from_settings(self.base_settings())
        tlca.run()
        score_single = tlca.static_score
        base_lca_id = id(tlca.base_lca)

        tlca.run(demand={self.fu.key: 2})

        assert id(tlca.base_lca) != base_lca_id  # rebuilt, not reused
        assert tlca.static_score == pytest.approx(2 * score_single)

    def test_run_does_not_leave_stale_results_from_a_previous_run(self):
        tlca = TimexLCA.from_settings(self.base_settings())
        tlca.run(expand_technosphere=True)
        assert hasattr(tlca, "datapackage")

        tlca.run(expand_technosphere=False)

        assert tlca.expanded_technosphere is False
        assert not hasattr(tlca, "datapackage")

    # ─── repeated runs ───

    def test_repeated_runs_reuse_base_lca_when_demand_is_unchanged(self):
        tlca = TimexLCA.from_settings(self.base_settings())
        base_lca_id = id(tlca.base_lca)

        for _ in range(3):
            tlca.run()
            assert id(tlca.base_lca) == base_lca_id

    def test_repeated_runs_are_reproducible(self):
        tlca = TimexLCA.from_settings(self.base_settings())

        tlca.run()
        first = tlca.static_score
        tlca.run(starting_datetime=datetime(2023, 1, 2))
        assert tlca.static_score != first
        tlca.run()

        assert tlca.static_score == first


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestCompare:

    @pytest.fixture(autouse=True)
    def setup(self, temporal_grouping_db_monthly):
        self.fu = bd.get_node(database="foreground", code="A")
        self.method = ("GWP", "example")
        self.start = datetime(2024, 1, 2)
        self.both_dbs = {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }
        self.one_db = {
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def base_settings(self, **kwargs):
        defaults = dict(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.both_dbs,
            starting_datetime=self.start,
            dynamic_lcia_enabled=False,
        )
        defaults.update(kwargs)
        return TimexLCASettings(**defaults)

    def test_compare_returns_one_row_per_settings(self):
        result = TimexLCA.compare(
            [
                self.base_settings(label="both vintages"),
                self.base_settings(label="2024 only", database_dates=self.one_db),
            ]
        )

        assert isinstance(result.summary, pd.DataFrame)
        assert len(result.summary) == 2
        assert list(result.summary["label"]) == ["both vintages", "2024 only"]

    def test_compare_summary_carries_scores_and_knobs(self):
        result = TimexLCA.compare([self.base_settings(label="a", time_horizon=20)])
        row = result.summary.iloc[0]

        for column in (
            "label",
            "base_score",
            "static_score",
            "starting_datetime",
            "temporal_grouping",
            "cutoff",
            "expand_technosphere",
            "time_horizon",
            "timeline_rows",
            "runtime_s",
        ):
            assert column in result.summary.columns

        assert isinstance(row["static_score"], float)
        assert row["static_score"] != 0
        assert row["time_horizon"] == 20
        assert row["timeline_rows"] > 0

    def test_compare_reuses_one_object_per_background(self):
        result = TimexLCA.compare(
            [
                self.base_settings(label="a"),
                self.base_settings(label="b", temporal_grouping="month"),
                self.base_settings(label="c", database_dates=self.one_db),
            ],
            keep_objects=True,
        )

        # a and b share database_dates -> same object; c differs -> its own
        assert result.objects["a"] is result.objects["b"]
        assert result.objects["c"] is not result.objects["a"]

    def test_compare_without_keep_objects_holds_no_objects(self):
        result = TimexLCA.compare([self.base_settings(label="a")])
        assert result.objects is None

    def test_compare_labels_default_to_positions(self):
        result = TimexLCA.compare([self.base_settings(), self.base_settings()])
        assert list(result.summary["label"]) == ["run 0", "run 1"]

    def test_compare_records_errors_instead_of_aborting(self):
        result = TimexLCA.compare(
            [
                self.base_settings(label="ok"),
                self.base_settings(label="broken", temporal_grouping="fortnight"),
            ],
            on_error="record",
        )

        assert len(result.summary) == 2
        ok, broken = result.summary.iloc[0], result.summary.iloc[1]
        assert pd.isna(ok["error"])
        assert pd.isna(broken["static_score"])
        assert "fortnight" in broken["error"]

    def test_compare_raises_on_error_by_default(self):
        with pytest.raises(Exception):
            TimexLCA.compare([self.base_settings(temporal_grouping="fortnight")])


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestCompareScenarios:
    """Scenario comparison: the metadata filter picks the background databases."""

    @pytest.fixture(autouse=True)
    def setup(self, temporal_grouping_db_monthly):
        self.fu = bd.get_node(database="foreground", code="A")
        self.method = ("GWP", "example")
        set_database_metadata(
            "db_2022", representative_time="2022-01-01", pathway="SSP2-Base"
        )
        set_database_metadata(
            "db_2024", representative_time="2024-01-01", pathway="SSP2-Base"
        )
        set_database_metadata("foreground", representative_time="dynamic")

    def test_compare_across_scenarios_records_the_scenario(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            scenario={"pathway": "SSP2-Base"},
            starting_datetime=datetime(2024, 1, 2),
            dynamic_lcia_enabled=False,
            label="SSP2-Base",
        )

        result = TimexLCA.compare([settings])
        row = result.summary.iloc[0]

        assert row["label"] == "SSP2-Base"
        assert row["scenario_pathway"] == "SSP2-Base"
        assert isinstance(row["static_score"], float)
        assert row["static_score"] != 0

    def test_scenario_is_a_fixed_field(self):
        tlca = TimexLCA.from_settings(
            TimexLCASettings(
                demand={self.fu.key: 1},
                method=self.method,
                scenario={"pathway": "SSP2-Base"},
                dynamic_lcia_enabled=False,
            )
        )
        with pytest.raises(ValueError, match="scenario"):
            tlca.run(scenario={"pathway": "SSP2-PkBudg500"})
