from __future__ import annotations

import hashlib
import json
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any


@dataclass
class StageMetric:
    """Performance measurement for a single pipeline stage."""

    elapsed_seconds: float
    peak_memory_mb: float


@dataclass
class ExecutionContext:
    """
    Reusable execution context for TimexLCA runs.

    The context stores:
    - deterministic cache key
    - stage metrics and stage outputs
    - generic in-memory caches used by compute kernels
    """

    demand: dict
    method: tuple
    database_dates: dict
    stage_metrics: dict[str, StageMetric] = field(default_factory=dict)
    stage_outputs: dict[str, Any] = field(default_factory=dict)
    caches: dict[str, dict] = field(default_factory=dict)

    @property
    def cache_key(self) -> str:
        payload = {
            "demand": sorted([(str(k), float(v)) for k, v in self.demand.items()]),
            "method": list(self.method),
            "database_dates": sorted(
                [(str(k), str(v)) for k, v in self.database_dates.items()]
            ),
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        )
        return digest.hexdigest()

    def get_cache(self, namespace: str) -> dict:
        if namespace not in self.caches:
            self.caches[namespace] = {}
        return self.caches[namespace]

    def reset_metrics(self) -> None:
        self.stage_metrics.clear()

    @contextmanager
    def track_stage(self, stage_name: str):
        was_tracing = tracemalloc.is_tracing()
        if not was_tracing:
            tracemalloc.start()
        start = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - start
            current, peak = tracemalloc.get_traced_memory()
            if not was_tracing:
                tracemalloc.stop()
            self.stage_metrics[stage_name] = StageMetric(
                elapsed_seconds=elapsed,
                peak_memory_mb=peak / (1024 * 1024),
            )

    def serialize_metrics(self) -> dict[str, dict[str, float]]:
        return {
            stage: {
                "elapsed_seconds": metric.elapsed_seconds,
                "peak_memory_mb": metric.peak_memory_mb,
            }
            for stage, metric in self.stage_metrics.items()
        }
