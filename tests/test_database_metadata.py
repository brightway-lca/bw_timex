"""Tests for reading and writing what a Brightway database represents."""

from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import set_database_metadata

# ─── Tests for set_database_metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSetDatabaseMetadata:

    def test_datetime_is_stored_as_iso_string(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_iso_string_is_stored_as_given(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01"

    def test_dynamic_is_allowed(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert bd.databases["foreground"]["representative_time"] == "dynamic"

    def test_scenario_fields_are_stored(self):
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            iam_model="remind",
            pathway="SSP2-PkBudg500",
        )
        assert bd.databases["db_2022"]["iam_model"] == "remind"
        assert bd.databases["db_2022"]["pathway"] == "SSP2-PkBudg500"

    def test_database_object_is_accepted(self):
        set_database_metadata(
            bd.Database("db_2022"), representative_time=datetime(2022, 1, 1)
        )
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_existing_metadata_is_kept(self):
        before = bd.databases["db_2022"]["backend"]
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["backend"] == before

    def test_survives_flush_and_reload(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        bd.databases.__init__()  # re-read from disk
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_unregistered_database_raises(self):
        with pytest.raises(ValueError, match="not registered"):
            set_database_metadata("no_such_db", representative_time=datetime(2022, 1, 1))

    def test_unparseable_representative_time_raises(self):
        with pytest.raises(ValueError, match="representative_time"):
            set_database_metadata("db_2022", representative_time="whenever")

    def test_non_serializable_value_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            set_database_metadata("db_2022", pathway=object())

    def test_no_metadata_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            set_database_metadata("db_2022")
