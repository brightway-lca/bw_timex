"""Tests for background-to-background temporal distributions."""
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import TimexLCA


@pytest.mark.usefixtures("background_td_db")
class TestBackgroundTDParameter:

    @pytest.fixture(autouse=True)
    def setup_method(self, background_td_db):
        self.product = bd.get_node(database="foreground", code="product")
        self.database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",
        }

    def test_default_depth_zero_no_background_traversal(self):
        """With default depth=0, background-to-background edges should be skipped."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
        )
        producers = set(timeline["producer_name"].values)
        assert "electricity production" not in producers

    def test_depth_one_traverses_background(self):
        """With depth=1, should traverse one level into background and find electricity."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=1,
        )
        producers = set(timeline["producer_name"].values)
        assert "electricity production" in producers

    def test_depth_one_creates_temporal_markets_for_electricity(self):
        """Electricity from bg-to-bg TD should get temporal market shares."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=1,
        )
        electricity_rows = timeline[timeline["producer_name"] == "electricity production"]
        assert not electricity_rows.empty
        for _, row in electricity_rows.iterrows():
            assert row["temporal_market_shares"] is not None

    def test_depth_cutoff_respected(self):
        """With depth=0, electricity should not appear."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=0,
        )
        producers = set(timeline["producer_name"].values)
        assert "electricity production" not in producers
        assert "steel production" in producers

    def test_full_lci_with_background_traversal(self):
        """Full LCI calculation should work with background_traversal_depth=1."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=1,
        )
        tlca.lci()
        tlca.static_lcia()
        assert tlca.static_score != 0
        assert isinstance(tlca.static_score, float)

    def test_score_differs_with_background_traversal(self):
        """Score with depth=1 should differ from depth=0 (different time-mapping of electricity)."""
        tlca_0 = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        tlca_0.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=0,
        )
        tlca_0.lci()
        tlca_0.static_lcia()

        tlca_1 = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        tlca_1.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=1,
        )
        tlca_1.lci()
        tlca_1.static_lcia()

        assert tlca_0.static_score != tlca_1.static_score

    def test_dynamic_lcia_with_background_traversal(self):
        """Dynamic LCIA should work with background_traversal_depth=1."""
        tlca = TimexLCA(
            demand={self.product.key: 1},
            method=("GWP", "example"),
            database_dates=self.database_dates,
        )
        tlca.build_timeline(
            starting_datetime="2024-01-01",
            graph_traversal="bfs",
            background_traversal_depth=1,
        )
        tlca.lci()
        result = tlca.dynamic_lcia(metric="GWP")
        assert not result.empty
        assert tlca.dynamic_score != 0
