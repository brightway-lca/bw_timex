"""Tests for TimexLCA.run() and TimexLCASettings."""

from datetime import datetime

import bw2data as bd
import pytest
from bw2data.tests import bw2test

from bw_timex import TimexLCA, TimexLCASettings


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCASettings:

    @pytest.fixture(autouse=True)
    def setup(self, temporal_grouping_db_monthly):
        self.fu = bd.get_node(database="foreground", code="A")
        self.database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        self.method = ("GWP", "example")

    def test_settings_creation_with_defaults(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
        )
        assert settings.demand == {self.fu.key: 1}
        assert settings.method == self.method
        assert settings.temporal_grouping == "year"
        assert settings.static_lcia_enabled is True
        assert settings.dynamic_lcia_enabled is True

    def test_settings_with_custom_values(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            temporal_grouping="month",
            cutoff=1e-6,
            time_horizon=20,
            dynamic_lcia_enabled=False,
        )
        assert settings.temporal_grouping == "month"
        assert settings.cutoff == 1e-6
        assert settings.time_horizon == 20
        assert settings.dynamic_lcia_enabled is False

    def test_run_static_only(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            dynamic_lcia_enabled=False,
        )
        tlca = TimexLCA(
            demand=settings.demand,
            method=settings.method,
            database_dates=settings.database_dates,
        )
        result = tlca.run(settings)

        assert result is tlca  # Check method chaining
        assert hasattr(tlca, "timeline")
        assert hasattr(tlca, "lca")
        assert hasattr(tlca, "static_score")
        assert isinstance(tlca.static_score, float)
        assert tlca.static_score != 0
        assert not hasattr(tlca, "dynamic_score")

    def test_run_full_pipeline(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            static_lcia_enabled=True,
            dynamic_lcia_enabled=False,
        )
        tlca = TimexLCA(
            demand=settings.demand,
            method=settings.method,
            database_dates=settings.database_dates,
        )
        tlca.run(settings)

        assert hasattr(tlca, "timeline")
        assert hasattr(tlca, "lca")
        assert hasattr(tlca, "static_score")
        assert isinstance(tlca.static_score, float)
        assert tlca.static_score != 0

    def test_run_without_timeline_expansion(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            expand_technosphere=False,
            dynamic_lcia_enabled=False,
        )
        tlca = TimexLCA(
            demand=settings.demand,
            method=settings.method,
            database_dates=settings.database_dates,
        )
        tlca.run(settings)

        assert tlca.expanded_technosphere is False
        assert hasattr(tlca, "dynamic_inventory_df")

    def test_run_with_expanded_technosphere(self):
        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            expand_technosphere=True,
            dynamic_lcia_enabled=False,
        )
        tlca = TimexLCA(
            demand=settings.demand,
            method=settings.method,
            database_dates=settings.database_dates,
        )
        tlca.run(settings)

        assert tlca.expanded_technosphere is True
        assert hasattr(tlca, "datapackage")

    def test_run_multiple_times_with_different_settings(self):
        tlca = TimexLCA(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
        )

        # First run
        settings1 = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            temporal_grouping="year",
            dynamic_lcia_enabled=False,
        )
        tlca.run(settings1)
        score1 = tlca.static_score
        assert isinstance(score1, float)

        # Second run with different starting_datetime
        settings2 = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2023-01-02", "%Y-%m-%d"),
            temporal_grouping="year",
            dynamic_lcia_enabled=False,
        )
        tlca.run(settings2)
        score2 = tlca.static_score
        assert isinstance(score2, float)
        # Different starting datetime should produce different results
        assert score1 != score2

        # Third run back to first settings
        settings3 = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            temporal_grouping="year",
            dynamic_lcia_enabled=False,
        )
        tlca.run(settings3)
        score3 = tlca.static_score
        # Should match first run (same settings)
        assert score3 == score1

    def test_run_multiple_times_reuses_base_lca(self):
        tlca = TimexLCA(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
        )
        base_lca_id = id(tlca.base_lca)

        settings = TimexLCASettings(
            demand={self.fu.key: 1},
            method=self.method,
            database_dates=self.database_dates,
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            dynamic_lcia_enabled=False,
        )

        # Run multiple times
        for _ in range(3):
            tlca.run(settings)
            # base_lca should be the same object (not recreated)
            assert id(tlca.base_lca) == base_lca_id
