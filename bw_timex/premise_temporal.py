"""Annotate existing premise databases with bw_timex temporal distributions.

premise (the trails work, released in premise >= 2.5.0) curates background
temporal data in ``temporal_distributions.csv`` and places it on exchanges via
fixed rules. This module reuses premise's CSV loader and mirrors those
placement rules to write ``bw_temporalis.TemporalDistribution`` objects onto the
exchanges of already-existing, year-specific premise bw2 databases. It does not
build, unfold, or materialize databases.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from bw_temporalis import TemporalDistribution, easy_timedelta_distribution

_RESOLUTION = "Y"  # premise temporal values are in years


@dataclass
class TemporalSpecs:
    """premise's categorized temporal buckets (keys are ``(name, reference product)``)."""

    biomass_growth_params: dict
    stock_asset_params: dict
    maintenance_suppliers: set
    end_of_life_suppliers: set
    dataset_lifetimes: dict


@dataclass
class AnnotationReport:
    """Summary of an annotation pass."""

    annotated: int = 0
    skipped_existing: int = 0
    faults: list = field(default_factory=list)

    def merge(self, other: "AnnotationReport") -> None:
        self.annotated += other.annotated
        self.skipped_existing += other.skipped_existing
        self.faults.extend(other.faults)


def _single_pulse(year: float) -> TemporalDistribution:
    return TemporalDistribution(
        date=np.array([int(round(year))], dtype="timedelta64[Y]"),
        amount=np.array([1.0], dtype=float),
    )


def _bounds(params: dict, loc, scale) -> tuple[int, int]:
    mn = params.get("temporal_min")
    mx = params.get("temporal_max")
    if mn is not None and mx is not None:
        start, end = int(np.floor(mn)), int(np.ceil(mx))
    elif loc is not None and scale:
        start, end = int(np.floor(loc - 3 * scale)), int(np.ceil(loc + 3 * scale))
    else:
        raise ValueError(
            "Cannot determine distribution bounds: need temporal_min/temporal_max "
            "or temporal_loc + temporal_scale."
        )
    if start > end:
        start, end = end, start
    return start, end


def premise_params_to_td(params: dict, *, max_steps: int = 200) -> TemporalDistribution:
    """Convert one premise temporal-parameter dict into a ``TemporalDistribution``.

    ``params`` uses premise keys: ``temporal_distribution`` (int code),
    ``temporal_loc``, ``temporal_scale``, ``temporal_min``, ``temporal_max``,
    ``temporal_offsets``, ``temporal_weights``. Time unit is years.
    """
    code = params.get("temporal_distribution")
    loc = params.get("temporal_loc")
    scale = params.get("temporal_scale")

    if code == 1:  # discrete: all mass at loc
        if loc is None:
            raise ValueError("discrete (code 1) temporal distribution requires temporal_loc")
        return _single_pulse(loc)

    if code == 6:  # discrete empirical: explicit offsets/weights
        offsets = params.get("temporal_offsets")
        weights = params.get("temporal_weights")
        if not offsets or not weights or len(offsets) != len(weights):
            raise ValueError("empirical (code 6) requires matching temporal_offsets/temporal_weights")
        amount = np.asarray(weights, dtype=float)
        total = amount.sum()
        if total == 0:
            raise ValueError("empirical (code 6) weights sum to zero")
        amount = amount / total
        return TemporalDistribution(
            date=np.array([int(round(o)) for o in offsets], dtype="timedelta64[Y]"),
            amount=amount,
        )

    start, end = _bounds(params, loc, scale)
    steps = max(2, min(max_steps, end - start + 1))

    if code == 3:  # normal
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="normal", param=scale)
    if code == 4:  # uniform
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="uniform")
    if code == 5:  # triangular, mode = loc
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="triangular", param=loc)

    raise ValueError(f"Unsupported premise temporal_distribution code: {code!r}")
