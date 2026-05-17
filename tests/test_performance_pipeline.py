import os
from datetime import datetime
from time import perf_counter

import bw2data as bd
import numpy as np
import pytest
from dynamic_characterization.classes import CharacterizedRow
from scipy import sparse

from bw_timex import TimexLCA
from bw_timex.dynamic_biosphere_builder import DynamicBiosphereBuilder


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


class _FakeMapping:
    def __init__(self, reversed_mapping):
        self.reversed = reversed_mapping


class _FakeLCA:
    def __init__(self):
        self.redo_calls = 0
        self.inventory = sparse.csr_matrix(([1.0], ([0], [0])), shape=(1, 1))

    def redo_lci(self, _demand):
        self.redo_calls += 1


def _legacy_per_activity_cache_benchmark(activities, unit_inventory):
    cache = {}
    for act in activities:
        if act not in cache:
            cache[act] = unit_inventory
    return len(cache)


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


def test_background_unit_lci_cache_benchmark():
    activity_count = int(_env_float("BW_TIMEX_PERF_BG_ACTIVITY_COUNT", 5000))
    activities = list(range(activity_count))

    fake_lca = _FakeLCA()
    builder = DynamicBiosphereBuilder.__new__(DynamicBiosphereBuilder)
    builder.lca_obj = fake_lca
    builder.activity_time_mapping = _FakeMapping(
        {
            act: (("db_2020", "shared_background_process"), np.datetime64(act, "Y"))
            for act in activities
        }
    )
    builder._background_unit_lci_cache = {}

    legacy_start = perf_counter()
    legacy_unique = _legacy_per_activity_cache_benchmark(activities, fake_lca.inventory)
    _ = perf_counter() - legacy_start

    new_start = perf_counter()
    for act in activities:
        builder.get_background_unit_lci(act)
    _ = perf_counter() - new_start

    assert legacy_unique == activity_count
    assert fake_lca.redo_calls == 1
    assert len(builder._background_unit_lci_cache) == 1
    assert legacy_unique / fake_lca.redo_calls >= activity_count
