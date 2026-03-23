"""Tests for TimexLCA methods: properties, labelling, error paths, and edge cases."""

from datetime import datetime

import bw2data as bd
import numpy as np
import pandas as pd
import pytest
from dynamic_characterization.classes import CharacterizedRow

from bw_timex import TimexLCA

# ─── Tests for score properties and error paths ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAProperties:

    @pytest.fixture(autouse=True)
    def setup_tlca(self, temporal_grouping_db_monthly):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        self.tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )

    def test_base_score(self):
        score = self.tlca.base_score
        assert isinstance(score, float)
        assert score != 0

    def test_static_score_before_lci_raises(self):
        with pytest.raises(AttributeError, match="LCI not yet calculated"):
            _ = self.tlca.static_score

    def test_dynamic_score_before_dynamic_lcia_raises(self):
        with pytest.raises(AttributeError, match="Characterized inventory not yet"):
            _ = self.tlca.dynamic_score

    def test_static_lcia_before_lci_raises(self):
        with pytest.raises(AttributeError, match="LCI not yet calculated"):
            self.tlca.static_lcia()

    def test_lci_before_timeline_raises(self):
        with pytest.raises(AttributeError, match="Timeline not yet built"):
            self.tlca.lci()

    def test_lci_no_expand_no_dynamic_raises(self):
        self.tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        with pytest.raises(ValueError, match="not possible to skip"):
            self.tlca.lci(expand_technosphere=False, build_dynamic_biosphere=False)


# ─── Tests for labelling / dataframe methods ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCALabellingMethods:

    @pytest.fixture(autouse=True)
    def setup_tlca(self, temporal_grouping_db_monthly):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        self.tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        self.tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        self.tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)
        self.tlca.static_lcia()

    def test_create_labelled_technosphere_dataframe(self):
        df = self.tlca.create_labelled_technosphere_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] > 0
        assert df.shape[0] == df.shape[1]  # square matrix

    def test_create_labelled_biosphere_dataframe(self):
        df = self.tlca.create_labelled_biosphere_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] > 0

    def test_create_labelled_dynamic_biosphere_dataframe(self):
        df = self.tlca.create_labelled_dynamic_biosphere_dataframe()
        assert isinstance(df, pd.DataFrame)
        # Only non-zero rows are kept
        assert all((df != 0).any(axis=1))

    def test_get_activity_name_from_time_mapped_id(self):
        time_mapped_id = self.tlca.timeline["time_mapped_producer"].iloc[0]
        name = self.tlca.get_activity_name_from_time_mapped_id(time_mapped_id)
        assert isinstance(name, str)
        assert len(name) > 0

    def test_create_labelled_dynamic_inventory_dataframe(self):
        df = self.tlca.create_labelled_dynamic_inventory_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert "flow" in df.columns
        assert "activity" in df.columns
        # Labels should be strings, not integer ids
        assert all(isinstance(v, str) for v in df["flow"].unique())
        assert all(isinstance(v, str) for v in df["activity"].unique())

    def test_static_lcia_without_expand_raises(self):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca2 = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca2.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        tlca2.lci(expand_technosphere=False, build_dynamic_biosphere=True)
        with pytest.raises(ValueError, match="expanded matrix"):
            tlca2.static_lcia()


# ─── Edge cases ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAEdgeCases:

    def test_custom_edge_filter_function(self):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        custom_filter = lambda x: x in tlca.node_collections.get("background", set())
        timeline = tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            edge_filter_function=custom_filter,
        )
        assert isinstance(timeline, pd.DataFrame)
        assert len(timeline) > 0

    def test_no_database_dates_foreground_only(self):
        fu = bd.get_node(database="foreground", code="A")
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates={"foreground": "dynamic"},
        )
        assert tlca.database_dates == {"foreground": "dynamic"}

    def test_check_format_invalid_timestamp(self):
        fu = bd.get_node(database="foreground", code="A")
        with pytest.raises(ValueError, match="neither 'dynamic' nor a datetime"):
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                database_dates={"foreground": 2024},
            )

    def test_check_format_missing_database(self):
        fu = bd.get_node(database="foreground", code="A")
        with pytest.raises(ValueError, match="not available"):
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                database_dates={
                    "foreground": "dynamic",
                    "nonexistent_db": datetime(2024, 1, 1),
                },
            )

    def test_calculate_dynamic_inventory_before_lci_raises(self):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        with pytest.raises(AttributeError, match="does not exist"):
            tlca.calculate_dynamic_inventory()

    def test_bfs_graph_traversal(self):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            graph_traversal="bfs",
        )
        assert isinstance(timeline, pd.DataFrame)
        assert len(timeline) > 0

    def test_nearest_interpolation(self):
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            interpolation_type="nearest",
        )
        assert isinstance(timeline, pd.DataFrame)
        # With nearest, each temporal_market_shares should map to a single db
        for shares in tlca.timeline["temporal_market_shares"].dropna():
            assert len(shares) == 1
            assert list(shares.values()) == [1]

    def test_lci_expand_true_no_dynamic_biosphere(self):
        """Test lci with expand_technosphere=True but build_dynamic_biosphere=False (line 389)."""
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        tlca.lci(expand_technosphere=True, build_dynamic_biosphere=False)
        tlca.static_lcia()
        assert tlca.static_score != 0
        # dynamic_inventory should NOT exist
        assert not hasattr(tlca, "dynamic_inventory")

    def test_dynamic_lcia(self):
        """Test the full dynamic_lcia path (lines 582-652)."""
        fu = bd.get_node(database="foreground", code="A")
        co2 = bd.get_node(database="bio", code="CO2")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)

        def simple_rf(row, time_horizon):
            return CharacterizedRow(
                date=row.date, amount=row.amount, flow=row.flow, activity=row.activity
            )

        char_funcs = {co2.id: simple_rf}
        result = tlca.dynamic_lcia(
            metric="radiative_forcing",
            time_horizon=100,
            characterization_functions=char_funcs,
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "amount" in result.columns
        # dynamic_score should now work
        assert isinstance(tlca.dynamic_score, float)

    def test_dynamic_lcia_gwp_metric(self):
        """Test dynamic_lcia with GWP metric."""
        fu = bd.get_node(database="foreground", code="A")
        co2 = bd.get_node(database="bio", code="CO2")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)

        def array_rf(row, time_horizon):
            # GWP path calls .amount.sum(), so amount must be array-like
            return CharacterizedRow(
                date=row.date,
                amount=np.array([row.amount]),
                flow=row.flow,
                activity=row.activity,
            )

        char_funcs = {co2.id: array_rf}
        result = tlca.dynamic_lcia(
            metric="GWP",
            time_horizon=100,
            characterization_functions=char_funcs,
            characterization_function_co2=array_rf,
        )
        assert isinstance(result, pd.DataFrame)
        assert tlca.dynamic_score != 0

    def test_dynamic_lcia_before_dynamic_inventory_raises(self):
        """Test dynamic_lcia raises if dynamic inventory not calculated (line 582)."""
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        tlca.lci(expand_technosphere=True, build_dynamic_biosphere=False)
        with pytest.raises(AttributeError, match="Dynamic lci not yet calculated"):
            tlca.dynamic_lcia()

    def test_create_labelled_dynamic_inventory_before_lci_raises(self):
        """Test error path for create_labelled_dynamic_inventory_dataframe (line 1498)."""
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        with pytest.raises(AttributeError, match="Dynamic inventory not yet"):
            tlca.create_labelled_dynamic_inventory_dataframe()

    def test_add_interdatabase_mapping_before_timeline_raises(self):
        """Test error path for add_interdatabase_activity_mapping (line 1235)."""
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        with pytest.raises(AttributeError, match="Timeline not yet built"):
            tlca.add_interdatabase_activity_mapping_from_timeline()

    def test_single_static_db(self):
        """Test with only one static database (simpler temporal market shares)."""
        fu = bd.get_node(database="foreground", code="A")
        database_dates = {
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        }
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )
        timeline = tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
        )
        assert isinstance(timeline, pd.DataFrame)
        # With a single static db, all temporal_market_shares should map to it
        for shares in tlca.timeline["temporal_market_shares"].dropna():
            assert len(shares) == 1
            assert list(shares.values()) == [1]
