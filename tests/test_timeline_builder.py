"""Tests for timeline_builder methods that can be tested with minimal setup."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from bw2data.configuration import labels

from bw_timex.timeline_builder import TimelineBuilder


def _make_stub(**kwargs):
    """Create a minimal stub with just the attributes needed for the method under test."""
    return SimpleNamespace(**kwargs)


# --- find_closest_date ---


class TestFindClosestDate:
    def setup_method(self):
        self.tb = _make_stub()
        # bind the unbound method
        self.find = TimelineBuilder.find_closest_date.__get__(self.tb)

    def test_exact_match(self):
        target = datetime(2023, 1, 1)
        dates = [datetime(2022, 1, 1), datetime(2023, 1, 1), datetime(2024, 1, 1)]
        result = self.find(target, dates)
        assert result == {datetime(2023, 1, 1): 1}

    def test_closer_to_earlier(self):
        target = datetime(2022, 3, 1)
        dates = [datetime(2022, 1, 1), datetime(2024, 1, 1)]
        result = self.find(target, dates)
        assert result == {datetime(2022, 1, 1): 1}

    def test_closer_to_later(self):
        target = datetime(2023, 10, 1)
        dates = [datetime(2022, 1, 1), datetime(2024, 1, 1)]
        result = self.find(target, dates)
        assert result == {datetime(2024, 1, 1): 1}

    def test_empty_dates(self):
        result = self.find(datetime(2023, 1, 1), [])
        assert result is None


# --- get_weights_for_interpolation_between_nearest_years ---


class TestGetWeightsForInterpolation:
    def setup_method(self):
        self.tb = _make_stub(interpolation_type="linear")
        self.get_weights = (
            TimelineBuilder.get_weights_for_interpolation_between_nearest_years.__get__(
                self.tb
            )
        )

    def test_exact_match(self):
        dates = [datetime(2020, 1, 1), datetime(2024, 1, 1)]
        result = self.get_weights(datetime(2020, 1, 1), dates)
        assert result == {datetime(2020, 1, 1): 1}

    def test_linear_midpoint(self):
        dates = [datetime(2020, 1, 1), datetime(2024, 1, 1)]
        result = self.get_weights(datetime(2022, 1, 1), dates)
        # ~midpoint, weights should be roughly 0.5 each
        assert datetime(2020, 1, 1) in result
        assert datetime(2024, 1, 1) in result
        assert result[datetime(2020, 1, 1)] + result[
            datetime(2024, 1, 1)
        ] == pytest.approx(1.0)
        assert result[datetime(2020, 1, 1)] == pytest.approx(0.5, abs=0.01)

    def test_below_all_dates(self):
        dates = [datetime(2022, 1, 1), datetime(2024, 1, 1)]
        result = self.get_weights(datetime(2020, 1, 1), dates)
        assert result == {datetime(2022, 1, 1): 1}

    def test_above_all_dates(self):
        dates = [datetime(2020, 1, 1), datetime(2022, 1, 1)]
        result = self.get_weights(datetime(2025, 1, 1), dates)
        assert result == {datetime(2022, 1, 1): 1}

    def test_unsupported_interpolation_type(self):
        self.tb.interpolation_type = "cubic"
        dates = [datetime(2020, 1, 1), datetime(2024, 1, 1)]
        with pytest.raises(ValueError, match="interpolation is not available"):
            self.get_weights(datetime(2022, 1, 1), dates)


# --- adjust_sign_of_amount_based_on_edge_type ---


class TestAdjustSignOfAmount:
    def setup_method(self):
        self.tb = _make_stub()
        self.adjust = TimelineBuilder.adjust_sign_of_amount_based_on_edge_type.__get__(
            self.tb
        )

    def test_technosphere(self):
        assert self.adjust("technosphere") == 1

    def test_substitution(self):
        assert self.adjust("substitution") == -1

    def test_unrecognized_type(self):
        with pytest.raises(TypeError, match="Unrecognized type"):
            self.adjust("unknown_type")
