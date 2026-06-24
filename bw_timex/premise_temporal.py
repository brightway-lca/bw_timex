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

    # Issue 2 fix: degenerate bounds (start == end) → single pulse, avoiding
    # easy_timedelta_distribution's "Start value is later than end" error.
    if start == end:
        return _single_pulse(start)

    if code == 3:  # normal
        # Issue 1 fix: easy_timedelta_distribution evaluates norm.pdf on a
        # normalized [-0.5, 0.5] axis (width 1.0), so the std `param` must be
        # in that normalized coordinate space, not in years.  Convert:
        #   param_normalized = scale_years / (end - start)
        # If scale is None, a default (or error) is deferred to the library.
        normalized_scale = (scale / (end - start)) if scale is not None else scale
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="normal", param=normalized_scale)
    if code == 4:  # uniform
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="uniform")
    if code == 5:  # triangular, mode = loc
        return easy_timedelta_distribution(start, end, _RESOLUTION, steps=steps, kind="triangular", param=loc)

    raise ValueError(f"Unsupported premise temporal_distribution code: {code!r}")


def _clean(value) -> str:
    return (value or "").strip()


def _supplier_key(exchange) -> tuple[str, str]:
    supplier = exchange.input
    return _clean(supplier.get("name")), _clean(
        supplier.get("reference product") or supplier.get("product")
    )


def annotate_database(db_name, specs: TemporalSpecs, *, overwrite: bool = False) -> AnnotationReport:
    """Write temporal distributions onto an existing premise bw2 database.

    Mirrors premise's ``add_temporal_distributions`` placement rules using the
    buckets in ``specs``. Returns an :class:`AnnotationReport`; never raises out
    of a single bad exchange (records a fault and continues).
    """
    import bw2data as bd

    if db_name not in bd.databases:
        raise ValueError(f"Database {db_name!r} not found in the current project.")

    report = AnnotationReport()

    def _fault(ds, exc, reason):
        report.faults.append({
            "database": db_name,
            "dataset": f"{_clean(ds.get('name'))} | {_clean(ds.get('reference product'))}",
            "exchange": _clean(exc.get("name")),
            "reason": reason,
        })

    def _convert_and_apply(ds, exc, params):
        try:
            td = premise_params_to_td(params)
        except Exception as err:  # malformed CSV row -> fault, never abort the pass
            _fault(ds, exc, f"Temporal distribution conversion failed: {err}")
            return
        exc["temporal_distribution"] = td
        exc.save()
        report.annotated += 1

    for ds in bd.Database(db_name):
        ds_key = (_clean(ds.get("name")), _clean(ds.get("reference product")))
        bg = specs.biomass_growth_params.get(ds_key)
        ds_lifetime = specs.dataset_lifetimes.get(ds_key)

        for exc in ds.exchanges():
            if not overwrite and exc.get("temporal_distribution") is not None:
                report.skipped_existing += 1
                continue

            etype = exc.get("type")

            if etype == "biosphere":
                if (
                    bg is not None
                    and _clean(exc.input.get("name")) == "Carbon dioxide, in air"
                    and bg.get("temporal_distribution") is not None
                ):
                    _convert_and_apply(ds, exc, bg)
                continue

            if etype != "technosphere":
                continue

            sup_name, sup_ref = _supplier_key(exc)
            if not sup_ref:
                _fault(ds, exc, "Missing supplier product on technosphere exchange.")
                continue
            key = (sup_name, sup_ref)

            params = specs.stock_asset_params.get(key)
            is_maintenance = key in specs.maintenance_suppliers
            is_end_of_life = key in specs.end_of_life_suppliers
            matched = int(params is not None) + int(is_maintenance) + int(is_end_of_life)

            if matched == 0:
                continue
            if matched > 1:
                _fault(ds, exc, f"Ambiguous temporal tags for supplier {key}.")
                continue

            if params is not None:
                _convert_and_apply(ds, exc, params)
                continue

            if ds_lifetime is None:
                _fault(ds, exc, "Missing dataset lifetime for maintenance/end_of_life.")
                continue

            if is_maintenance:
                _convert_and_apply(ds, exc,
                    {"temporal_distribution": 4, "temporal_min": 0.0, "temporal_max": ds_lifetime})
            else:  # end_of_life
                _convert_and_apply(ds, exc,
                    {"temporal_distribution": 6, "temporal_offsets": [ds_lifetime], "temporal_weights": [1.0]})

    return report


# ---------------------------------------------------------------------------
# Task 3: load_temporal_specs — reuse premise's CSV loader (optional dep)
# ---------------------------------------------------------------------------

def _import_premise_trails():
    """Import premise's trails module, or raise a clear, actionable error."""
    try:
        from premise import trails as premise_trails
    except ImportError as exc:
        raise ImportError(
            "premise temporal annotation requires premise (>=2.5.0). "
            "Install premise separately (e.g. `pip install premise>=2.5.0`). "
            "Note: premise pins scipy<1.14, which is not available for Python "
            "3.13, so it cannot be a co-resolved bw_timex extra; install it in "
            "an environment that satisfies that constraint."
        ) from exc
    if not hasattr(premise_trails, "TrailsDataPackage") or not hasattr(
        premise_trails, "FILEPATH_TEMPORAL_PARAMETERS"
    ):
        raise RuntimeError(
            "The installed premise lacks temporal-distribution support "
            "(TrailsDataPackage / temporal_distributions.csv). Upgrade to premise>=2.5.0."
        )
    return premise_trails


class _DummySelf:
    """Stand-in for the unused ``self`` of premise's CSV loader method."""


def load_temporal_specs(path=None) -> TemporalSpecs:
    """Load premise's curated temporal specs into a :class:`TemporalSpecs`.

    Reuses premise's own ``_load_temporal_specs_from_csv`` (which ignores
    ``self``) so parsing/categorization stays in premise. ``path`` defaults to
    premise's bundled ``temporal_distributions.csv``.
    """
    premise_trails = _import_premise_trails()
    csv_path = path if path is not None else premise_trails.FILEPATH_TEMPORAL_PARAMETERS
    loader = premise_trails.TrailsDataPackage._load_temporal_specs_from_csv
    stock_assets, end_of_life, biomass_growth, maintenance, dataset_lifetimes = loader(
        _DummySelf(), csv_path
    )
    return TemporalSpecs(
        biomass_growth_params=biomass_growth,
        stock_asset_params=stock_assets,
        maintenance_suppliers=maintenance,
        end_of_life_suppliers=end_of_life,
        dataset_lifetimes=dataset_lifetimes,
    )


# ---------------------------------------------------------------------------
# Task 4: public entry point
# ---------------------------------------------------------------------------

def add_premise_temporal_distributions(databases, *, overwrite: bool = False) -> AnnotationReport:
    """Annotate existing premise databases with temporal distributions.

    ``databases`` is an iterable of database names (or a mapping whose keys are
    database names; values are ignored). Loads premise's temporal specs once and
    annotates each database. Returns an aggregated :class:`AnnotationReport`.
    """
    names = list(databases.keys()) if isinstance(databases, dict) else list(databases)
    specs = load_temporal_specs()
    report = AnnotationReport()
    for name in names:
        report.merge(annotate_database(name, specs, overwrite=overwrite))
    return report
