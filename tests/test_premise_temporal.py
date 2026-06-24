import numpy as np
import pytest
from bw_temporalis import TemporalDistribution


def test_discrete_code_1_single_pulse_at_loc():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td({"temporal_distribution": 1, "temporal_loc": -5.0})
    assert isinstance(td, TemporalDistribution)
    assert td.date.astype("timedelta64[Y]").astype(int).tolist() == [-5]
    assert np.allclose(td.amount.sum(), 1.0)


def test_empirical_code_6_offsets_weights_normalised():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 6, "temporal_offsets": [0, 10], "temporal_weights": [1.0, 3.0]}
    )
    assert td.date.astype("timedelta64[Y]").astype(int).tolist() == [0, 10]
    np.testing.assert_allclose(td.amount, [0.25, 0.75])


def test_uniform_code_4_from_min_max():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td({"temporal_distribution": 4, "temporal_min": 0.0, "temporal_max": 5.0})
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == 0 and yrs.max() == 5
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_normal_code_3_bounds_from_min_max():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 3, "temporal_loc": -20.0, "temporal_scale": 3.0,
         "temporal_min": -40.0, "temporal_max": -1.0}
    )
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == -40 and yrs.max() == -1
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_triangular_code_5():
    from bw_timex.premise_temporal import premise_params_to_td
    td = premise_params_to_td(
        {"temporal_distribution": 5, "temporal_loc": 5.0, "temporal_min": 0.0, "temporal_max": 10.0}
    )
    assert td.date.astype("timedelta64[Y]").astype(int).max() == 10
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_unsupported_code_raises():
    from bw_timex.premise_temporal import premise_params_to_td
    with pytest.raises(ValueError):
        premise_params_to_td({"temporal_distribution": 99, "temporal_loc": 1.0})


def test_annotation_report_merge():
    from bw_timex.premise_temporal import AnnotationReport
    a = AnnotationReport(annotated=1, skipped_existing=2, faults=[{"x": 1}])
    b = AnnotationReport(annotated=3, skipped_existing=0, faults=[{"y": 2}])
    a.merge(b)
    assert a.annotated == 4 and a.skipped_existing == 2 and len(a.faults) == 2
