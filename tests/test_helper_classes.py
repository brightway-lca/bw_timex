"""Tests for helper classes in bw_timex.helper_classes."""

import warnings

import pytest

from bw_timex.helper_classes import InterDatabaseMapping, SetList, TimeMappingDict

# --- SetList ---


class TestSetList:
    def test_add_and_len(self):
        sl = SetList()
        sl.add({1, 2})
        sl.add({3, 4})
        assert len(sl) == 2

    def test_add_duplicate_set(self):
        sl = SetList()
        sl.add({1, 2})
        sl.add({1, 2})
        assert len(sl) == 1

    def test_add_overlapping_set_skipped(self):
        sl = SetList()
        sl.add({1, 2})
        sl.add({2, 3})  # overlaps with first set (item 2)
        assert len(sl) == 1  # second set not added

    def test_getitem_found(self):
        sl = SetList()
        sl.add({1, 2})
        sl.add({3, 4})
        assert sl[3] == {3, 4}

    def test_getitem_not_found(self):
        sl = SetList()
        sl.add({1, 2})
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sl[99]
            assert result is None
            assert len(w) == 1
            assert "not found" in str(w[0].message)

    def test_repr(self):
        sl = SetList()
        sl.add({1, 2})
        assert "SetList" in repr(sl)


# --- TimeMappingDict ---


class TestTimeMappingDict:
    def test_add_auto_id(self):
        tmd = TimeMappingDict()
        id1 = tmd.add(("flow1", "2023"))
        id2 = tmd.add(("flow2", "2024"))
        assert id1 == 2  # start_id default
        assert id2 == 3

    def test_add_duplicate_returns_existing(self):
        tmd = TimeMappingDict()
        id1 = tmd.add(("flow1", "2023"))
        id2 = tmd.add(("flow1", "2023"))
        assert id1 == id2

    def test_add_custom_id(self):
        tmd = TimeMappingDict()
        id1 = tmd.add(("flow1", "2023"), unique_id=42)
        assert id1 == 42

    def test_add_duplicate_custom_id_raises(self):
        tmd = TimeMappingDict()
        tmd.add(("flow1", "2023"), unique_id=42)
        with pytest.raises(ValueError, match="already assigned"):
            tmd.add(("flow2", "2024"), unique_id=42)

    def test_custom_start_id(self):
        tmd = TimeMappingDict(start_id=100)
        id1 = tmd.add(("flow1", "2023"))
        assert id1 == 100

    def test_reversed(self):
        tmd = TimeMappingDict()
        tmd.add(("flow1", "2023"))
        tmd.add(("flow2", "2024"))
        rev = tmd.reversed
        assert rev[2] == ("flow1", "2023")
        assert rev[3] == ("flow2", "2024")

    def test_reversed_caching(self):
        tmd = TimeMappingDict()
        tmd.add(("flow1", "2023"))
        rev1 = tmd.reversed
        rev2 = tmd.reversed
        assert rev1 is rev2  # same object when not modified

    def test_reversed_invalidated_on_add(self):
        tmd = TimeMappingDict()
        tmd.add(("flow1", "2023"))
        rev1 = tmd.reversed
        tmd.add(("flow2", "2024"))
        rev2 = tmd.reversed
        assert rev1 is not rev2  # rebuilt after modification
        assert 3 in rev2


# --- InterDatabaseMapping ---


class TestInterDatabaseMapping:
    def test_find_match(self):
        idm = InterDatabaseMapping()
        idm[1] = {"db_a": 1, "db_b": 10}
        assert idm.find_match(1, "db_b") == 10

    def test_make_reciprocal(self):
        idm = InterDatabaseMapping()
        idm[1] = {"db_a": 1, "db_b": 10}
        idm.make_reciprocal()
        # Now looking up id 10 should also work
        assert idm[10] == {"db_a": 1, "db_b": 10}

    def test_auto_reciprocal_on_getitem(self):
        idm = InterDatabaseMapping()
        idm[1] = {"db_a": 1, "db_b": 10}
        # Accessing via __getitem__ should trigger make_reciprocal
        result = idm[10]
        assert result == {"db_a": 1, "db_b": 10}
        assert idm._reciprocal is True

    def test_reciprocal_only_applied_once(self):
        idm = InterDatabaseMapping()
        idm[1] = {"db_a": 1, "db_b": 10}
        idm.make_reciprocal()
        idm.make_reciprocal()  # should be no-op
        assert idm._reciprocal is True
