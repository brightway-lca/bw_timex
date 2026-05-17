import os
from datetime import datetime

import bw2data as bd
import pytest
from dynamic_characterization.classes import CharacterizedRow

from bw_timex import TimexLCA


pytestmark = pytest.mark.skipif(
    os.getenv("BW_TIMEX_ENABLE_PERF_TESTS", "0") != "1",
    reason="Performance regression tests are disabled by default.",
)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def _run_pipeline_and_get_metrics(
    demand_key, database_dates, starting_datetime: datetime
) -> dict:
    tlca = TimexLCA(
        demand={demand_key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
        performance_mode="speed",
    )
    tlca.build_timeline(
        starting_datetime=starting_datetime,
        temporal_grouping="year",
        graph_traversal="priority",
    )
    tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)

    co2 = bd.get_node(database="bio", code="CO2")

    def simple_rf(row, time_horizon):
        return CharacterizedRow(
            date=row.date, amount=row.amount, flow=row.flow, activity=row.activity
        )

    tlca.dynamic_lcia(
        metric="radiative_forcing",
        characterization_functions={co2.id: simple_rf},
    )
    return tlca.get_performance_report()


def _assert_stage_metrics_present(metrics: dict) -> None:
    required_stages = {
        "build_timeline",
        "lci",
        "calculate_dynamic_inventory",
        "dynamic_lcia",
    }
    assert required_stages.issubset(set(metrics.keys()))
    for stage in required_stages:
        assert metrics[stage]["elapsed_seconds"] >= 0
        assert metrics[stage]["peak_memory_mb"] >= 0


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
def test_performance_baseline_small(temporal_grouping_db_monthly):
    fu = bd.get_node(database="foreground", code="A")
    database_dates = {
        "db_2022": datetime.strptime("2022", "%Y"),
        "db_2024": datetime.strptime("2024", "%Y"),
        "foreground": "dynamic",
    }
    metrics = _run_pipeline_and_get_metrics(
        demand_key=fu.key,
        database_dates=database_dates,
        starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
    )
    _assert_stage_metrics_present(metrics)
    total_seconds = sum(stage["elapsed_seconds"] for stage in metrics.values())
    peak_memory_mb = max(stage["peak_memory_mb"] for stage in metrics.values())
    assert total_seconds <= _env_float("BW_TIMEX_PERF_SMALL_TOTAL_SEC", 120.0)
    assert peak_memory_mb <= _env_float("BW_TIMEX_PERF_SMALL_PEAK_MB", 1024.0)


@pytest.mark.usefixtures("nonunitary_db")
def test_performance_baseline_medium(nonunitary_db):
    fu = bd.get_node(database="foreground", code="A")
    database_dates = {
        "db_2020": datetime.strptime("2020", "%Y"),
        "foreground": "dynamic",
    }
    metrics = _run_pipeline_and_get_metrics(
        demand_key=fu.key,
        database_dates=database_dates,
        starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
    )
    _assert_stage_metrics_present(metrics)
    total_seconds = sum(stage["elapsed_seconds"] for stage in metrics.values())
    peak_memory_mb = max(stage["peak_memory_mb"] for stage in metrics.values())
    assert total_seconds <= _env_float("BW_TIMEX_PERF_MEDIUM_TOTAL_SEC", 180.0)
    assert peak_memory_mb <= _env_float("BW_TIMEX_PERF_MEDIUM_PEAK_MB", 1536.0)


@pytest.mark.usefixtures("vehicle_db")
def test_performance_baseline_large(vehicle_db):
    fu = bd.get_node(database="foreground", code="EV")
    database_dates = {
        "db_2020": datetime.strptime("2020", "%Y"),
        "db_2030": datetime.strptime("2030", "%Y"),
        "db_2040": datetime.strptime("2040", "%Y"),
        "foreground": "dynamic",
    }
    metrics = _run_pipeline_and_get_metrics(
        demand_key=fu.key,
        database_dates=database_dates,
        starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d"),
    )
    _assert_stage_metrics_present(metrics)
    total_seconds = sum(stage["elapsed_seconds"] for stage in metrics.values())
    peak_memory_mb = max(stage["peak_memory_mb"] for stage in metrics.values())
    assert total_seconds <= _env_float("BW_TIMEX_PERF_LARGE_TOTAL_SEC", 240.0)
    assert peak_memory_mb <= _env_float("BW_TIMEX_PERF_LARGE_PEAK_MB", 2048.0)
