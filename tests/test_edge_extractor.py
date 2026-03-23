"""Tests for edge_extractor functions that don't require a full BW database."""

from numbers import Number

import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex.edge_extractor import _join_datetime_and_timedelta_distributions


class TestJoinDatetimeAndTimedeltaDistributions:
    def test_number_producer_returns_consumer(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01"], dtype="datetime64[s]"),
            np.array([1.0]),
        )
        result = _join_datetime_and_timedelta_distributions(2.5, td_consumer)
        assert result is td_consumer

    def test_both_temporal_distributions(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01", "2024-06-01"], dtype="datetime64[s]"),
            np.array([1.0, 1.0]),
        )
        td_producer = TemporalDistribution(
            np.array([0, 365 * 24 * 3600], dtype="timedelta64[s]"),
            np.array([0.5, 0.5]),
        )
        result = _join_datetime_and_timedelta_distributions(td_producer, td_consumer)
        assert isinstance(result, TemporalDistribution)
        assert len(result.date) == 4  # 2 consumer x 2 producer
        assert result.date.dtype == np.dtype("datetime64[s]")

    def test_wrong_consumer_dtype_raises(self):
        td_consumer = TemporalDistribution(
            np.array([0, 100], dtype="timedelta64[s]"),
            np.array([1.0, 1.0]),
        )
        td_producer = TemporalDistribution(
            np.array([0, 100], dtype="timedelta64[s]"),
            np.array([0.5, 0.5]),
        )
        with pytest.raises(ValueError, match="datetime64"):
            _join_datetime_and_timedelta_distributions(td_producer, td_consumer)

    def test_wrong_producer_dtype_raises(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01"], dtype="datetime64[s]"),
            np.array([1.0]),
        )
        td_producer = TemporalDistribution(
            np.array(["2024-06-01"], dtype="datetime64[s]"),
            np.array([0.5]),
        )
        with pytest.raises(ValueError, match="timedelta64"):
            _join_datetime_and_timedelta_distributions(td_producer, td_consumer)

    def test_invalid_types_raises(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01"], dtype="datetime64[s]"),
            np.array([1.0]),
        )
        with pytest.raises(ValueError, match="Can't join"):
            _join_datetime_and_timedelta_distributions("not_a_td", td_consumer)
