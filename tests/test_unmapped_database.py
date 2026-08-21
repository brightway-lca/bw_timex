"""A database that is reached by the traversal but is missing from the mapping.

Only the databases holding the functional unit are treated as `"dynamic"`
automatically. A second foreground database - an intermediate one, which does
not hold the functional unit - is therefore missing from the mapping unless the
user marks it, and its nodes cannot be placed in time.
"""

from datetime import datetime

import pytest

from bw_timex import TimexLCA, set_database_metadata
from bw_timex.errors import UnmappedDatabaseError

DATABASE_DATES = {
    "foreground": "dynamic",
    "background_2020": datetime(2020, 1, 1),
    "background_2030": datetime(2030, 1, 1),
}


def _set_background_metadata():
    set_database_metadata("background_2020", representative_time=datetime(2020, 1, 1))
    set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))


@pytest.mark.usefixtures("split_foreground_db")
class TestUnmappedDatabase:

    def test_explicit_database_dates_raise_unmapped_database_error(self):
        tlca = TimexLCA(
            demand={("foreground", "fu"): 1},
            method=("GWP", "example"),
            database_dates=DATABASE_DATES,
        )
        with pytest.raises(UnmappedDatabaseError) as excinfo:
            tlca.build_timeline()
        assert "intermediate_foreground" in str(excinfo.value)

    def test_error_names_an_affected_process(self):
        tlca = TimexLCA(
            demand={("foreground", "fu"): 1},
            method=("GWP", "example"),
            database_dates=DATABASE_DATES,
        )
        with pytest.raises(UnmappedDatabaseError, match="assembly"):
            tlca.build_timeline()

    def test_error_points_at_the_fix(self):
        tlca = TimexLCA(
            demand={("foreground", "fu"): 1},
            method=("GWP", "example"),
            database_dates=DATABASE_DATES,
        )
        with pytest.raises(UnmappedDatabaseError, match="set_database_metadata"):
            tlca.build_timeline()

    def test_metadata_resolution_raises_the_same_error(self):
        _set_background_metadata()
        tlca = TimexLCA(demand={("foreground", "fu"): 1}, method=("GWP", "example"))
        with pytest.raises(UnmappedDatabaseError, match="intermediate_foreground"):
            tlca.build_timeline()

    def test_marking_the_database_dynamic_fixes_it(self):
        _set_background_metadata()
        set_database_metadata("intermediate_foreground", representative_time="dynamic")
        tlca = TimexLCA(demand={("foreground", "fu"): 1}, method=("GWP", "example"))
        tlca.build_timeline()
        assert "assembly" in set(tlca.timeline["producer_name"])

    def test_listing_the_database_in_database_dates_fixes_it(self):
        tlca = TimexLCA(
            demand={("foreground", "fu"): 1},
            method=("GWP", "example"),
            database_dates={**DATABASE_DATES, "intermediate_foreground": "dynamic"},
        )
        tlca.build_timeline()
        assert "assembly" in set(tlca.timeline["producer_name"])

    def test_unmapped_database_error_is_a_value_error(self):
        assert issubclass(UnmappedDatabaseError, ValueError)

    def test_unmapped_database_error_is_exposed_at_top_level(self):
        import bw_timex

        assert bw_timex.UnmappedDatabaseError is UnmappedDatabaseError
