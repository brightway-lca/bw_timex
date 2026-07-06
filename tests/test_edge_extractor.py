"""Tests for edge_extractor functions that don't require a full BW database."""

import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex.edge_extractor import (
    _join_datetime_and_timedelta_distributions,
    VariantBackgroundMixin,
)


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
        np.testing.assert_array_equal(
            result.date,
            np.array(
                [
                    "2024-01-01",
                    "2024-12-31",
                    "2024-06-01",
                    "2025-06-01",
                ],
                dtype="datetime64[s]",
            ),
        )
        np.testing.assert_array_equal(result.amount, np.array([0.5, 0.5, 0.5, 0.5]))

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

    def test_absolute_producer_td_is_broadcast_to_consumer_dates(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01", "2024-06-01"], dtype="datetime64[s]"),
            np.array([1.0, 1.0]),
        )
        td_producer = TemporalDistribution(
            np.array(["2025-01-01", "2025-06-01"], dtype="datetime64[s]"),
            np.array([0.25, 0.75]),
        )
        result = _join_datetime_and_timedelta_distributions(td_producer, td_consumer)
        assert isinstance(result, TemporalDistribution)
        assert len(result.date) == 4  # 2 consumer x 2 producer
        assert result.date.dtype == np.dtype("datetime64[s]")
        np.testing.assert_array_equal(
            result.date,
            np.array(
                [
                    "2025-01-01",
                    "2025-06-01",
                    "2025-01-01",
                    "2025-06-01",
                ],
                dtype="datetime64[s]",
            ),
        )
        np.testing.assert_array_equal(
            result.amount, np.array([0.25, 0.75, 0.25, 0.75])
        )

    def test_invalid_types_raises(self):
        td_consumer = TemporalDistribution(
            np.array(["2024-01-01"], dtype="datetime64[s]"),
            np.array([1.0]),
        )
        with pytest.raises(ValueError, match="Can't join"):
            _join_datetime_and_timedelta_distributions("not_a_td", td_consumer)


def test_fold_production_td_outer_product():
    base = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    prod = TemporalDistribution(
        date=np.array([0, 3], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.5]),
    )
    out = VariantBackgroundMixin._fold_production_td(base, prod)
    # dates: 0+0, 0+3, 10+0, 10+3  (i-major)
    assert list(out.date.astype("timedelta64[Y]").astype(int)) == [0, 3, 10, 13]
    # amounts: 0.6*0.5, 0.6*0.5, 0.4*0.5, 0.4*0.5
    np.testing.assert_allclose(out.amount, [0.3, 0.3, 0.2, 0.2])
    # total weight preserved (prod is normalized)
    assert out.amount.sum() == pytest.approx(base.amount.sum())
