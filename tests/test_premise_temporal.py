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
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == 0
    assert yrs.max() == 10
    np.testing.assert_allclose(td.amount.sum(), 1.0)


def test_normal_peak_near_loc():
    """Issue 1 guard: normalized scale must concentrate mass near loc, not be flat."""
    from bw_timex.premise_temporal import premise_params_to_td

    # Narrow scale=2 over wide range [-50, 50], loc=0 → mass should concentrate near 0
    td_narrow = premise_params_to_td(
        {"temporal_distribution": 3, "temporal_loc": 0.0, "temporal_scale": 2.0,
         "temporal_min": -50.0, "temporal_max": 50.0}
    )
    yrs_narrow = td_narrow.date.astype("timedelta64[Y]").astype(int)

    # Peak must be within 2 years of loc=0
    peak_year = yrs_narrow[np.argmax(td_narrow.amount)]
    assert abs(peak_year - 0) <= 2, f"Peak at {peak_year}, expected near 0"

    # Narrow scale should capture most mass within ±5 years
    mass_within_5_narrow = td_narrow.amount[np.abs(yrs_narrow) <= 5].sum()
    assert mass_within_5_narrow > 0.90, (
        f"Narrow scale=2 mass within ±5 yrs: {mass_within_5_narrow:.4f} (expected >0.90)"
    )

    # Wide scale=40 over same range should spread mass much more
    td_wide = premise_params_to_td(
        {"temporal_distribution": 3, "temporal_loc": 0.0, "temporal_scale": 40.0,
         "temporal_min": -50.0, "temporal_max": 50.0}
    )
    yrs_wide = td_wide.date.astype("timedelta64[Y]").astype(int)
    mass_within_5_wide = td_wide.amount[np.abs(yrs_wide) <= 5].sum()

    # Narrow should capture significantly more mass near centre than wide
    assert mass_within_5_narrow > mass_within_5_wide * 3, (
        f"Narrow ({mass_within_5_narrow:.4f}) should be >3x wide ({mass_within_5_wide:.4f})"
    )


def test_degenerate_min_equals_max_codes_345():
    """Issue 2 guard: start==end must return single pulse, not raise ValueError."""
    from bw_timex.premise_temporal import premise_params_to_td

    for code in (3, 4, 5):
        td = premise_params_to_td(
            {"temporal_distribution": code, "temporal_loc": 7.0,
             "temporal_min": 7.0, "temporal_max": 7.0,
             "temporal_scale": 1.0}  # scale provided for code 3 normal
        )
        yrs = td.date.astype("timedelta64[Y]").astype(int)
        assert len(yrs) == 1, f"code {code}: expected 1 date, got {len(yrs)}"
        assert yrs[0] == 7, f"code {code}: expected year 7, got {yrs[0]}"
        np.testing.assert_allclose(td.amount.sum(), 1.0, err_msg=f"code {code}: amount must sum to 1")


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


# ---------------------------------------------------------------------------
# Task 2: annotate_database tests
# ---------------------------------------------------------------------------
from bw2data.tests import bw2test


def _write_synthetic_dbs():
    import bw2data as bd
    bd.Database("bio").write({
        ("bio", "co2"): {"name": "Carbon dioxide, in air", "type": "emission", "categories": ("air",)},
    })
    bd.Database("ei").write({
        # biomass-growth dataset: has the CO2-in-air biosphere exchange
        ("ei", "forest"): {
            "name": "forestry", "reference product": "wood", "location": "GLO", "unit": "kg",
            "exchanges": [
                {"input": ("ei", "forest"), "amount": 1.0, "type": "production"},
                {"input": ("bio", "co2"), "amount": -2.0, "type": "biosphere"},
            ],
        },
        # supplier used as stock_asset, maintenance, and end_of_life by consumers below
        ("ei", "machine"): {
            "name": "machine", "reference product": "machine", "location": "GLO", "unit": "unit",
            "exchanges": [{"input": ("ei", "machine"), "amount": 1.0, "type": "production"}],
        },
        # consumer with a 50-year lifetime that buys the machine (tagged maintenance/eol per specs)
        ("ei", "plant"): {
            "name": "plant", "reference product": "power", "location": "GLO", "unit": "kWh",
            "exchanges": [
                {"input": ("ei", "plant"), "amount": 1.0, "type": "production"},
                {"input": ("ei", "machine"), "amount": 0.1, "type": "technosphere"},
            ],
        },
    })


@bw2test
def test_biomass_growth_lands_on_co2_exchange():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={("forestry", "wood"): {
            "temporal_distribution": 3, "temporal_loc": -20.0, "temporal_scale": 3.0,
            "temporal_min": -40.0, "temporal_max": -1.0}},
        stock_asset_params={}, maintenance_suppliers=set(),
        end_of_life_suppliers=set(), dataset_lifetimes={},
    )
    report = annotate_database("ei", specs)
    forest = bd.get_node(database="ei", code="forest")
    bio_exc = [e for e in forest.exchanges() if e["type"] == "biosphere"][0]
    assert bio_exc.get("temporal_distribution") is not None
    assert report.annotated == 1


@bw2test
def test_maintenance_uniform_over_lifetime():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={}, stock_asset_params={},
        maintenance_suppliers={("machine", "machine")}, end_of_life_suppliers=set(),
        dataset_lifetimes={("plant", "power"): 50.0},
    )
    report = annotate_database("ei", specs)
    plant = bd.get_node(database="ei", code="plant")
    tech_exc = [e for e in plant.exchanges() if e["type"] == "technosphere"][0]
    td = tech_exc.get("temporal_distribution")
    assert td is not None
    yrs = td.date.astype("timedelta64[Y]").astype(int)
    assert yrs.min() == 0 and yrs.max() == 50
    assert report.annotated == 1


@bw2test
def test_ambiguous_supplier_is_faulted_not_applied():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={}, stock_asset_params={},
        maintenance_suppliers={("machine", "machine")},
        end_of_life_suppliers={("machine", "machine")},
        dataset_lifetimes={("plant", "power"): 50.0},
    )
    report = annotate_database("ei", specs)
    plant = bd.get_node(database="ei", code="plant")
    tech_exc = [e for e in plant.exchanges() if e["type"] == "technosphere"][0]
    assert tech_exc.get("temporal_distribution") is None
    assert report.annotated == 0 and len(report.faults) == 1


@bw2test
def test_idempotent_skip_then_overwrite():
    import bw2data as bd
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    _write_synthetic_dbs()
    specs = TemporalSpecs(
        biomass_growth_params={("forestry", "wood"): {
            "temporal_distribution": 1, "temporal_loc": -5.0}},
        stock_asset_params={}, maintenance_suppliers=set(),
        end_of_life_suppliers=set(), dataset_lifetimes={},
    )
    annotate_database("ei", specs)
    again = annotate_database("ei", specs)
    assert again.annotated == 0 and again.skipped_existing >= 1
    forced = annotate_database("ei", specs, overwrite=True)
    assert forced.annotated == 1


@bw2test
def test_unknown_database_raises():
    from bw_timex.premise_temporal import annotate_database, TemporalSpecs
    specs = TemporalSpecs({}, {}, set(), set(), {})
    with pytest.raises(ValueError):
        annotate_database("does-not-exist", specs)
