from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from functools import partial
from time import perf_counter
from typing import Callable, Optional

import bw2data as bd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sb
from bw2calc import LCA
from bw2data import (
    Database,
    Method,
    Normalization,
    Weighting,
    databases,
    methods,
    normalizations,
    projects,
    weightings,
)
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.backends.schema import get_id
from bw2data.errors import Brightway2Project
from dynamic_characterization import characterize
from loguru import logger
from peewee import fn
from scipy import sparse

from ._lci_cache import BACKGROUND_UNIT_LCI_CACHE, LCI_SOLVE_CACHE, NODES_CACHE
FACTORIZE_SOLVES_THRESHOLD = 8

from .database_metadata import resolve_database_dates_from_metadata, split_scenario
from ._lci_cache import (
    BACKGROUND_AGGREGATE_CACHE,
    BACKGROUND_SUPPLY_CACHE,
    LCI_SOLVE_CACHE,
    NODES_CACHE,
)
from .background_solver import BackgroundSolver
from .block_structure import BlockStructure
from .database_metadata import resolve_database_dates_from_metadata
from .dynamic_biosphere_builder import DynamicBiosphereBuilder
from .helper_classes import InterDatabaseMapping, LazyActivity, TimeMappingDict
from .matrix_modifier import MatrixModifier
from .timeline_builder import TimelineBuilder
from .utils import (
    convert_date_string_to_datetime,
    extract_date_as_integer,
    round_datetime,
    round_datetime_series_to_year,
)
from .validation import (
    BuildTimelineInputs,
    DynamicLCIAInputs,
    LCIInputs,
    PlotDynamicInventoryInputs,
    TimexLCAInputs,
)


@dataclass
class TimexLCASettings:
    """Everything needed to run one time-explicit LCA calculation.

    One `TimexLCASettings` fully describes a calculation, so it doubles as the
    record of what was run - keep it, log it, or put a list of them into
    [`TimexLCA.compare`][bw_timex.timex_lca.TimexLCA.compare].

    The fields fall into two groups. `database_dates`, `scenario` and
    `use_global_lci_cache` (`FIXED_FIELDS`) pick the background databases, which
    fix the column space of the time-explicit matrices and the caches keyed on
    them: they are set when the `TimexLCA` is built and cannot be changed per
    run. Everything else - the demand, the method, and all the timeline, LCI and
    LCIA knobs - may vary from run to run on the same object.
    """

    #: Fields that pick the background, and so cannot vary between runs of one
    #: `TimexLCA`. Changing one means building a new object (which
    #: `TimexLCA.compare` does for you).
    FIXED_FIELDS = ("database_dates", "scenario", "use_global_lci_cache")

    # Core parameters
    demand: dict
    method: tuple
    database_dates: Optional[dict] = None
    scenario: Optional[dict] = None
    use_global_lci_cache: bool = True
    #: Name for this calculation, used to label its row in a comparison.
    label: Optional[str] = None

    # Timeline parameters
    starting_datetime: datetime | str = "now"
    temporal_grouping: str = "year"
    interpolation_type: str = "linear"
    edge_filter_function: Optional[Callable] = None
    cutoff: float = 1e-9
    max_calc: int = 2000
    graph_traversal: str = "priority"
    traverse_background: bool = False
    timeline_args: tuple = field(default_factory=tuple)
    timeline_kwargs: dict = field(default_factory=dict)

    # LCI parameters
    build_dynamic_biosphere: bool = True
    expand_technosphere: bool = True
    keep_activity_dimension: bool = True

    # LCIA parameters
    static_lcia_enabled: bool = True
    dynamic_lcia_enabled: bool = True
    metric: str = "radiative_forcing"
    time_horizon: int = 100
    fixed_time_horizon: bool = False
    time_horizon_start: Optional[datetime] = None
    characterization_functions: Optional[dict] = None
    characterization_function_co2: Optional[dict] = None
    use_disaggregated_lci: bool = False


@dataclass
class ComparisonResult:
    """What [`TimexLCA.compare`][bw_timex.timex_lca.TimexLCA.compare] found.

    Attributes
    ----------
    summary : pandas.DataFrame
        One row per calculation: its label, its scores, the settings it was run
        with, how long it took, and the error it raised, if any. Scenario
        metadata is spread over `scenario_*` columns, so scenarios can be
        grouped and plotted directly.
    settings : list[TimexLCASettings]
        The settings of each row, in the same order - the full record of what
        produced the comparison.
    objects : dict[str, TimexLCA] or None
        The `TimexLCA` objects by label, if `compare(keep_objects=True)`. Use
        these to dig into a single result - its timeline, dynamic inventory, or
        contributions. Calculations that share a background share one object,
        so the same object can appear under several labels. `None` by default,
        since holding every object of a large comparison is expensive.
    """

    summary: pd.DataFrame
    settings: list
    objects: Optional[dict] = None

    def __len__(self) -> int:
        return len(self.summary)

    def _repr_html_(self) -> str:
        return self.summary._repr_html_()


class TimexLCA:
    """
    Class to perform time-explicit LCA calculations.

    A TimexLCA contains the LCI of processes occurring at explicit points in time. It tracks the timing of processes,
    relinks their technosphere and biosphere exchanges to match the technology landscape at that point in time,
    and also keeps track of the timing of the resulting emissions. As such, it combines prospective and dynamic LCA
    approaches.

    TimexLCA first calculates a static LCA, which informs a priority-first graph traversal. From the
    graph traversal, temporal relationships between exchanges and processes are derived. Based on
    the timing of the processes, bw_timex matches the processes at the intersection between
    foreground and background to the best available background databases. This temporal relinking is
    achieved by using datapackages to add new time-specific processes. The new processes and their
    exchanges to other technosphere processes or biosphere flows extent the technosphere and
    biosphere matrices.

    Temporal information of both processes and biosphere flows is retained, allowing for dynamic
    LCIA.

    TimexLCA calculates:
     1) a static "base" LCA score (`TimexLCA.base_score`, same as `bw2calc.lca.score`),
     2) a static time-explicit LCA score (`TimexLCA.static_score`), which links LCIs to the
        respective background databases, but without dynamic characterization of the time-explicit inventory
     3) a dynamic time-explicit LCA score (`TimexLCA.dynamic_score`), with dynamic inventory and
        dynamic characterization. These are provided for radiative forcing and GWP but can also be
        user-defined.


    Examples
    --------
    ```python
    from bw_timex import TimexLCA, set_database_metadata

    demand = {("my_foreground_database", "my_process"): 1}
    method = ("some_method_family", "some_category", "some_method")

    # Databases exported by premise >= 2.4.9.2 already know the point in
    # time they represent. For your own databases, say so once:
    set_database_metadata("my_background_database_one", representative_time=datetime(2020, 1, 1))
    set_database_metadata("my_background_database_two", representative_time=datetime(2030, 1, 1))

    tlca = TimexLCA(demand, method)

    # ... or map the databases explicitly, which then replaces the metadata:
    tlca = TimexLCA(
        demand,
        method,
        database_dates={
            "my_background_database_one": datetime(2020, 1, 1),
            "my_background_database_two": datetime(2030, 1, 1),
            # Several databases may share the same date, e.g. to keep your own
            # modified copies of background processes in their own database:
            "my_modified_background_2020": datetime(2020, 1, 1),
            "my_foreground_database": "dynamic",
        },
    )

    tlca.build_timeline()  # has many optional arguments
    tlca.lci()
    tlca.static_lcia()
    print(tlca.static_score)
    # also available: "GWP", "pGWP", "pGTP", "prospective_radiative_forcing"
    tlca.dynamic_lcia(metric="radiative_forcing")
    print(tlca.dynamic_score)
    ```
    """

    def __init__(
        self,
        demand: dict,
        method: tuple,
        database_dates: dict = None,
        scenario: dict = None,
        create_missing: bool = False,
        premise_key: str = None,
        ecoinvent_credentials: tuple = None,
        use_global_lci_cache: bool = True,
    ) -> None:
        """
        Instantiating a `TimexLCA` object calculates a static LCA, initializes time mappings
        for activities and biosphere flows, and stores useful subsets of ids in the
        node_collections.

        Parameters
        ----------
        demand : dict[object: float]
                The demand for which the LCA will be calculated. The keys can be Brightway `Node`
                instances, `(database, code)` tuples, or integer ids.
        method : tuple
                Tuple defining the LCIA method, such as `('foo', 'bar')` or default methods, such as
                `("EF v3.1", "climate change", "global warming potential (GWP100)")`
        database_dates : dict, optional
                Fallback for mapping the databases yourself instead of letting
                `bw_timex` read their metadata - useful for databases written by
                premise < 2.4.9.2, which carry no metadata, or when you want to
                override what the metadata says. Dictionary mapping database names
                to the point in time they represent, as a `datetime`, or to
                `"dynamic"` for databases whose processes are distributed over
                time (typically the foreground).
                Several databases may share the same date, e.g. to keep your own
                modified copies of background processes in their own database
                instead of writing them into the shared background database for
                that vintage. If not given, the mapping is read from the
                databases' own `representative_time` metadata (which premise
                >= 2.4.9.2 writes when exporting, and which you can set yourself
                with `bw_timex.set_database_metadata`). Passing this argument replaces
                the metadata entirely: only the databases listed here are used.
        scenario : dict, optional
                Metadata a background database must match to be used, e.g.
                `{"iam_model": "remind", "pathway": "SSP2-PkBudg500"}`. Reads the
                scenario metadata written by premise >= 2.4.9.2 (or by you, with
                `bw_timex.set_database_metadata`), so it does nothing for
                databases that carry none. Only needed when the project
                holds several scenarios - `TimexLCA`
                raises and lists them otherwise. Databases that don't declare the
                filtered key (your foreground, a hand-built vintage) are always
                kept. Cannot be combined with `database_dates`.
        create_missing : bool, optional
                If True, background databases the `scenario` names but that this
                project does not hold yet are built with `premise`, and ecoinvent
                is imported first if it is missing. The `scenario` then has to
                describe the build completely: a `years` list plus all four of
                `iam_model`, `pathway`, `system_model` and `ecoinvent_version`
                (it may also narrow `sectors` or name a `source_database`).
                Needs the optional dependency:
                `pip install "bw_timex[premise]"`. Building takes tens of minutes
                and roughly 2-4 GB per year. Default is False, which raises
                instead of building. Cannot be combined with `database_dates`.
        premise_key : str, optional
                premise decryption key, used only when building. Falls back to the
                environment variable `PREMISE_KEY`.
        ecoinvent_credentials : tuple, optional
                `(username, password)`, used only when ecoinvent itself has to be
                imported. Falls back to the environment variables
                `ECOINVENT_USERNAME` and `ECOINVENT_PASSWORD`.
        use_global_lci_cache : bool, optional
                If True (default), background unit LCI matrices are cached at
                module level and reused across `TimexLCA` objects within the
                same Python session. The cache is keyed by background process
                identity plus the database's `modified` token, so edits to a
                background database invalidate stale entries automatically. Set
                to False to isolate this object's caching (e.g. when mutating
                background databases via raw SQL that bypasses bw2data). The
                module-level cache can be cleared with
                `bw_timex.clear_background_lci_cache()`.
        """

        logger.info("Initializing TimexLCA object...")

        self.demand = demand
        self.method = method
        self.scenario = scenario

        TimexLCAInputs(
            demand=demand,
            method=method,
            database_dates=database_dates,
            scenario=scenario,
            create_missing=create_missing,
            premise_key=premise_key,
            ecoinvent_credentials=ecoinvent_credentials,
        )

        if create_missing:
            from .scenario_builder import ensure_scenario_databases

            ensure_scenario_databases(
                scenario,
                premise_key=premise_key,
                ecoinvent_credentials=ecoinvent_credentials,
            )

        self.database_dates = self._resolve_database_dates(
            demand=demand, database_dates=database_dates, scenario=scenario
        )

        # Settings this object was built from, if any, and the raw values of the
        # fields that pick the background. Kept as passed (not as resolved), so
        # `run` can tell whether a settings object asks for the same background.
        self.settings = None
        self._fixed_fields = {
            "database_dates": database_dates,
            "scenario": scenario,
            "use_global_lci_cache": use_global_lci_cache,
        }

        # Validate again against the *resolved* mapping: the earlier call
        # above validated the raw `database_dates` argument (needed so a bad
        # `create_missing` combination is rejected before any build starts),
        # which is `None` on the metadata/scenario-resolution path - that
        # call alone would never catch a demand database whose own
        # `representative_time` metadata resolves it to a fixed date rather
        # than "dynamic". `create_missing` is left at its default here since
        # `self.database_dates` is never `None`.
        TimexLCAInputs(
            demand=demand,
            method=method,
            database_dates=self.database_dates,
            scenario=self.scenario,
        )

        # Filled in by `prepare_base_lca_inputs`: the databases the base LCA
        # covers, which is a subset of `database_dates` plus their dependents.
        self._base_lca_database_names = set()

        logger.info("Calculating base LCA...")
        # Calculate static LCA results using a custom prepare_lca_inputs function that includes all
        # background databases in the LCA. We need all the IDs for the time mapping dict.
        fu, data_objs, remapping = self.prepare_base_lca_inputs(
            demand=self.demand, method=self.method
        )
        self.base_lca = LCA(fu, data_objs=data_objs, remapping_dicts=remapping)
        self.base_lca.lci()
        self.base_lca.lcia()

        # Create static_only dict that excludes dynamic processes that will be exploded later.
        # This way we only have the "background databases" that we can later link to from the dates
        # of the timeline.
        self.database_dates_static = {
            k: v for k, v in self.database_dates.items() if isinstance(v, datetime)
        }

        logger.info("Collecting node infos...")
        # Create some collections of nodes that will be useful down the line, e.g. all nodes from
        # the background databases that link to foreground nodes.
        self.create_node_collections()

        self.interdatabase_activity_mapping = InterDatabaseMapping()

        # Getting all nodes from the databases for faster lookup later. Node
        # proxies are cached per database at module level (keyed by the
        # database's `modified` token) so repeated TimexLCA objects in the same
        # session reuse them instead of re-querying. Opt out via
        # `use_global_lci_cache=False`.
        logger.info(
            f"Loading node metadata from {len(self.database_dates)} database(s)..."
        )
        self._nodes_cache = NODES_CACHE if use_global_lci_cache else {}
        project = bd.projects.current
        self.nodes = {}
        # Build a cache mapping activity code to name for efficient lookups.
        # This avoids repeated database queries in plotting and labeling functions.
        self._activity_code_to_name_cache = {}
        for db in self.database_dates.keys():
            modified = bd.databases[db].get("modified") if db in bd.databases else None
            key = ("nodes", project, db, modified)
            db_nodes = self._nodes_cache.get(key)
            if db_nodes is None:
                # Only the scalar columns are read here; the pickled `data`
                # blob of a node is loaded lazily, if it is needed at all.
                columns = [getattr(AD, name) for name in LazyActivity.COLUMN_NAMES]
                rows = AD.select(*columns).where(AD.database == db).tuples()
                db_nodes = {row[0]: LazyActivity(row) for row in rows}
                self._nodes_cache[key] = db_nodes
            self.nodes.update(db_nodes)
            for node in db_nodes.values():
                self._activity_code_to_name_cache[node["code"]] = node["name"]

        self._last_timeline_build_key = None
        self._cached_timeline = None
        self._default_edge_filter_function = None
        self._dynamic_lcia_inventory_cache = {}
        # Handed to every `BackgroundSolver` this object builds, so background
        # unit LCIs are shared across `lci()` calls - and, unless opted out,
        # across `TimexLCA` objects in the session.
        self._background_supply_cache = (
            BACKGROUND_SUPPLY_CACHE if use_global_lci_cache else {}
        )
        self._background_aggregate_cache = (
            BACKGROUND_AGGREGATE_CACHE if use_global_lci_cache else {}
        )
        self._background_solver = None
        # Materialized on demand from the builder's recipes; see
        # `temporal_market_lcis`.
        self._temporal_market_lcis = None
        # Whether the last lci() call restored its supply_array / inventory
        # from `LCI_SOLVE_CACHE` instead of running a fresh `spsolve`.
        self._lci_used_cached_solve = False

        logger.info("TimexLCA initialized.")

    @classmethod
    def from_settings(cls, settings: TimexLCASettings) -> "TimexLCA":
        """Build a `TimexLCA` from a [`TimexLCASettings`][bw_timex.timex_lca.TimexLCASettings].

        The settings' background fields (`TimexLCASettings.FIXED_FIELDS`) are
        used to construct the object; the rest become the default arguments of
        [`run`][bw_timex.timex_lca.TimexLCA.run].

        Examples
        --------
        ```python
        settings = TimexLCASettings(demand=demand, method=method, database_dates=dates)
        tlca = TimexLCA.from_settings(settings).run()
        print(tlca.static_score)
        ```
        """
        tlca = cls(
            demand=settings.demand,
            method=settings.method,
            database_dates=settings.database_dates,
            scenario=settings.scenario,
            use_global_lci_cache=settings.use_global_lci_cache,
        )
        tlca.settings = settings
        return tlca

    def _settings_for_run(
        self, settings: TimexLCASettings | None, overrides: dict
    ) -> TimexLCASettings:
        """Resolve the settings one `run` call should use.

        Starts from `settings` (or the object's own), applies `overrides`
        without mutating either, and refuses overrides that would change the
        background, since that would invalidate the matrices and caches this
        object is built around.
        """
        base = settings if settings is not None else self.settings
        if base is None:
            base = TimexLCASettings(
                demand=self.demand, method=self.method, **self._fixed_fields
            )

        known = set(TimexLCASettings.__dataclass_fields__)
        unknown = set(overrides) - known
        if unknown:
            raise TypeError(
                f"run() got unexpected setting(s) {sorted(unknown)}. "
                f"Valid settings are: {sorted(known)}."
            )

        resolved = replace(base, **overrides) if overrides else base

        for field_name in TimexLCASettings.FIXED_FIELDS:
            if getattr(resolved, field_name) != self._fixed_fields[field_name]:
                raise ValueError(
                    f"`{field_name}` selects the background databases, which fix the "
                    "columns of the time-explicit matrices, so it cannot change between "
                    f"runs of one TimexLCA (this one was built with "
                    f"{field_name}={self._fixed_fields[field_name]!r}). "
                    "Build another TimexLCA for it, or pass both settings to "
                    "TimexLCA.compare(), which does that for you."
                )

        return resolved

    def _rebuild_base_lca(self, demand: dict, method: tuple) -> None:
        """Recompute the base LCA for a new demand or method.

        Everything keyed on the background - the node proxies, the node
        collections, and both module-level caches - stays valid, since those
        depend on `database_dates` rather than on the demand. The cached
        timeline does not: the graph traversal starts from the demand and is
        prioritised by the method's scores, so it is dropped here.
        """
        logger.info("Demand or method changed; recalculating base LCA...")
        self.demand = demand
        self.method = method
        fu, data_objs, remapping = self.prepare_base_lca_inputs(
            demand=demand, method=method
        )
        self.base_lca = LCA(fu, data_objs=data_objs, remapping_dicts=remapping)
        self.base_lca.lci()
        self.base_lca.lcia()
        self._last_timeline_build_key = None
        self._cached_timeline = None

    def _clear_stale_results(self) -> None:
        """Drop results of the previous run that this one may not overwrite.

        Only derived *results* are cleared, never the caches: `build_timeline`,
        `lci` and `dynamic_lcia` are all keyed on their own inputs, so they
        recompute when they must and reuse when they can. Without this, a run
        with dynamic LCIA disabled would still answer `dynamic_score` from the
        previous run.
        """
        for attribute in (
            "characterized_inventory",
            "current_metric",
            "current_time_horizon",
            "dynamic_inventory",
            "dynamic_inventory_df",
            "dynamic_inventory_disaggregated",
            "dynamic_inventory_disaggregated_df",
            "datapackage",
        ):
            if hasattr(self, attribute):
                delattr(self, attribute)
        self._static_score_from_timeline = None

    def run(
        self, settings: TimexLCASettings | None = None, **overrides
    ) -> "TimexLCA":
        """Run the whole calculation: timeline, LCI, and LCIA.

        Runs `build_timeline()`, `lci()`, `static_lcia()` and, unless disabled,
        `dynamic_lcia()`.

        Can be called repeatedly on one object to vary the demand, the method,
        or any knob. The background caches and, where the timeline parameters
        are unchanged, the timeline itself are reused between calls; only a
        changed demand or method forces the base LCA to be recalculated.
        Changing the background databases is refused - see
        [`compare`][bw_timex.timex_lca.TimexLCA.compare] for that.

        Parameters
        ----------
        settings : TimexLCASettings, optional
            Settings for this run. Defaults to the ones the object was built
            with by [`from_settings`][bw_timex.timex_lca.TimexLCA.from_settings].
        **overrides
            Individual settings to override for this run only, e.g.
            `run(time_horizon=20)`. Neither `settings` nor the object's own
            settings are modified.

        Returns
        -------
        TimexLCA
            The object itself, so calls can be chained.

        Raises
        ------
        TypeError
            If an override is not a field of `TimexLCASettings`.
        ValueError
            If an override would change the background databases.

        Examples
        --------
        ```python
        tlca = TimexLCA.from_settings(settings)
        tlca.run()                            # the settings as given
        tlca.run(time_horizon=20)             # one knob, settings untouched
        tlca.run(demand={other_process: 1})   # new demand, background reused
        ```
        """
        settings = self._settings_for_run(settings, overrides)
        logger.info("Starting TimexLCA.run() pipeline...")

        if settings.demand != self.demand or settings.method != self.method:
            self._rebuild_base_lca(settings.demand, settings.method)
        self._clear_stale_results()

        # Build timeline
        logger.info("Step 1/4: Building timeline...")
        self.build_timeline(
            starting_datetime=settings.starting_datetime,
            temporal_grouping=settings.temporal_grouping,
            interpolation_type=settings.interpolation_type,
            edge_filter_function=settings.edge_filter_function,
            cutoff=settings.cutoff,
            max_calc=settings.max_calc,
            graph_traversal=settings.graph_traversal,
            traverse_background=settings.traverse_background,
            *settings.timeline_args,
            **settings.timeline_kwargs,
        )

        # Calculate LCI
        logger.info("Step 2/4: Calculating LCI...")
        self.lci(
            build_dynamic_biosphere=settings.build_dynamic_biosphere,
            expand_technosphere=settings.expand_technosphere,
            keep_activity_dimension=settings.keep_activity_dimension,
        )

        # Calculate static LCIA
        if settings.static_lcia_enabled:
            logger.info("Step 3/4: Calculating static LCIA...")
            self.static_lcia()
        else:
            logger.info("Step 3/4: Skipping static LCIA (disabled).")

        # Calculate dynamic LCIA
        if settings.dynamic_lcia_enabled:
            logger.info("Step 4/4: Calculating dynamic LCIA...")
            self.dynamic_lcia(
                metric=settings.metric,
                time_horizon=settings.time_horizon,
                fixed_time_horizon=settings.fixed_time_horizon,
                time_horizon_start=settings.time_horizon_start,
                characterization_functions=settings.characterization_functions,
                characterization_function_co2=settings.characterization_function_co2,
                use_disaggregated_lci=settings.use_disaggregated_lci,
            )
        else:
            logger.info("Step 4/4: Skipping dynamic LCIA (disabled).")

        logger.info("TimexLCA.run() completed successfully.")
        return self

    @staticmethod
    def _background_key(settings: TimexLCASettings) -> tuple:
        """Hashable identity of the background a settings object asks for.

        Two calculations can share one `TimexLCA` exactly when these match.
        """
        return (
            tuple(sorted((k, str(v)) for k, v in (settings.database_dates or {}).items())),
            tuple(sorted((k, str(v)) for k, v in (settings.scenario or {}).items())),
            settings.use_global_lci_cache,
        )

    def _result_row(self, settings: TimexLCASettings) -> dict:
        """Collect everything worth comparing about the run that just finished."""

        def score(name):
            try:
                return float(getattr(self, name))
            except (AttributeError, TypeError, ValueError):
                return float("nan")

        row = {
            "base_score": score("base_score"),
            "static_score": (
                score("static_score") if settings.static_lcia_enabled else float("nan")
            ),
            "dynamic_score": (
                score("dynamic_score") if settings.dynamic_lcia_enabled else float("nan")
            ),
        }
        for key, value in (settings.scenario or {}).items():
            row[f"scenario_{key}"] = value
        row.update(
            {
                "demand": settings.demand,
                "method": settings.method,
                "database_dates": self.database_dates,
                "n_databases": len(self.database_dates),
                "starting_datetime": settings.starting_datetime,
                "temporal_grouping": settings.temporal_grouping,
                "interpolation_type": settings.interpolation_type,
                "cutoff": settings.cutoff,
                "max_calc": settings.max_calc,
                "graph_traversal": settings.graph_traversal,
                "traverse_background": settings.traverse_background,
                "expand_technosphere": settings.expand_technosphere,
                "build_dynamic_biosphere": settings.build_dynamic_biosphere,
                "keep_activity_dimension": settings.keep_activity_dimension,
                "metric": settings.metric if settings.dynamic_lcia_enabled else None,
                "time_horizon": settings.time_horizon,
                "fixed_time_horizon": settings.fixed_time_horizon,
                "timeline_rows": len(self.timeline) if hasattr(self, "timeline") else 0,
            }
        )
        return row

    @classmethod
    def compare(
        cls,
        settings: list,
        keep_objects: bool = False,
        on_error: str = "raise",
    ) -> ComparisonResult:
        """Run several calculations and collect them into one table.

        This is the way to compare scenarios. Scenarios differ in their
        background databases, and the background fixes the columns of the
        time-explicit matrices and the caches keyed on them, so each one needs
        its own `TimexLCA` - there is nothing shareable between them to begin
        with. `compare` builds one object per distinct background and runs every
        calculation that asks for that background on it, so a scenario × demand
        grid only pays for a new object when the background actually changes.

        Parameters
        ----------
        settings : list[TimexLCASettings]
            The calculations to run, in the order they should appear.
        keep_objects : bool, optional
            If True, keep each `TimexLCA` in `ComparisonResult.objects` so the
            timelines and inventories behind the scores stay available. Default
            is False, since a large comparison holds a lot of memory this way.
        on_error : str, optional
            `"raise"` (default) propagates the first failure. `"record"` puts
            the message in the row's `error` column, leaves its scores as NaN,
            and carries on - useful for long unattended sweeps.

        Returns
        -------
        ComparisonResult
            Its `summary` is a `DataFrame` with one row per calculation.

        Examples
        --------
        ```python
        base = TimexLCASettings(demand=demand, method=method)
        comparison = TimexLCA.compare(
            [
                replace(base, scenario={"pathway": "SSP2-Base"}, label="Base"),
                replace(base, scenario={"pathway": "SSP2-PkBudg500"}, label="PkBudg500"),
            ]
        )
        comparison.summary.plot.bar(x="label", y="static_score")
        ```
        """
        if on_error not in ("raise", "record"):
            raise ValueError(
                f"`on_error` must be 'raise' or 'record', not {on_error!r}."
            )

        objects_by_background = {}
        objects_by_label = {}
        rows = []

        for position, one in enumerate(settings):
            label = one.label if one.label is not None else f"run {position}"
            if label in objects_by_label:
                label = f"{label} ({position})"

            key = cls._background_key(one)
            tlca = objects_by_background.get(key)
            if tlca is None:
                tlca = cls.from_settings(one)
                objects_by_background[key] = tlca
            objects_by_label[label] = tlca

            logger.info(f"Comparison {position + 1}/{len(settings)}: {label}")
            started = perf_counter()
            try:
                tlca.run(one)
                row = tlca._result_row(one)
                row["error"] = None
            except Exception as error:  # noqa: BLE001 - reported in the table
                if on_error == "raise":
                    raise
                logger.warning(f"Comparison run {label!r} failed: {error}")
                row = {"error": f"{type(error).__name__}: {error}"}
            row["label"] = label
            row["runtime_s"] = perf_counter() - started
            rows.append(row)

        summary = pd.DataFrame(rows)
        leading = [
            column
            for column in ("label", "base_score", "static_score", "dynamic_score")
            if column in summary.columns
        ]
        summary = summary[leading + [c for c in summary.columns if c not in leading]]

        return ComparisonResult(
            summary=summary,
            settings=list(settings),
            objects=objects_by_label if keep_objects else None,
        )

    @staticmethod
    def _resolve_database_dates(
        demand: dict, database_dates: dict | None, scenario: dict | None
    ) -> dict:
        """Map databases to the points in time they represent.

        Either from the explicit `database_dates` argument, which is then the
        whole mapping, or from the databases' own `representative_time`
        metadata. Databases holding the demand default to `"dynamic"`.

        Raises
        ------
        ValueError
            If both `database_dates` and `scenario` are given (`scenario`
            only selects among databases resolved from metadata, so it makes
            no sense once `database_dates` already gives the whole mapping),
            or if `scenario` is given but no surviving database positively
            declares one of its keys - almost always a typo in one of its
            keys or values, since a filter that legitimately excludes
            everything would leave nothing for `TimexLCA` to compute with.
            A database that doesn't declare a filtered key at all is kept by
            the filter (see `resolve_database_dates_from_metadata`), so
            checking whether the *resolved mapping* is empty is not enough:
            it stays non-empty whenever such a database happens to be
            present, even though the filter matched none of the databases it
            was meant to select among.
        """
        if database_dates is not None:
            if scenario:
                raise ValueError(
                    "`scenario` selects background databases by their metadata and "
                    "only applies when `database_dates` is not given. Pass one or "
                    "the other."
                )
            return dict(database_dates)

        resolved = resolve_database_dates_from_metadata(scenario)

        # Only the filter keys are matched against metadata; the build keys
        # (`years`, `sectors`, ...) describe what to build and are never
        # declared by any database, so reporting them as unmatched metadata
        # would send the user looking for a key that cannot exist.
        filters, _ = split_scenario(scenario)

        filter_matched = filters and any(
            key in bd.databases[name] for name in resolved for key in filters
        )

        if filters and not filter_matched:
            declared = {}
            for name in bd.databases:
                metadata = bd.databases[name]
                for key in filters:
                    if key in metadata:
                        declared.setdefault(key, set()).add(str(metadata[key]))
            details = "; ".join(
                f"'{key}': "
                f"{sorted(declared[key]) if key in declared else 'not declared by any database'}"
                for key in filters
            )
            raise ValueError(
                f"scenario={filters!r} matched no database in this project. "
                f"Values actually declared for its key(s) by this project's "
                f"databases: {details}. Check for a typo in the filter, or pass "
                f"`create_missing=True` (with a `years` list in the scenario) to "
                f"build the databases with premise."
            )
        elif not resolved:
            logger.info(
                "No database_dates provided, and no database in this project carries "
                "`representative_time` metadata. Treating the databases containing the "
                "functional unit as dynamic. No remapping of inventories to time "
                "explicit databases will be done."
            )

        for key in demand:
            database = bd.get_node(id=get_id(key))["database"]
            resolved.setdefault(database, "dynamic")

        return resolved

    ########################################
    # Main functions to be called by users #
    ########################################

    def build_timeline(
        self,
        starting_datetime: datetime | str = "now",
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
        edge_filter_function: Callable = None,
        cutoff: float = 1e-9,
        max_calc: int = 2000,
        graph_traversal: str = "priority",
        traverse_background: bool = False,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Creates a `TimelineBuilder` instance that does the graph traversal (similar to
        bw_temporalis) and extracts all edges with their temporal information. Creates the
        `TimexLCA.timeline` of technosphere exchanges.

        Parameters
        ----------
        starting_datetime: datetime | str, optional
            Point in time when the demand occurs. This is the initial starting point of the
            graph traversal and the timeline. Something like `"now"` or `"2023-01-01"`.
            Default is `"now"`.
        temporal_grouping : str, optional
            Time resolution for grouping exchanges over time in the timeline. Default is 'year',
            other options are 'month', 'day', 'hour'.
        interpolation_type : str, optional
            Type of interpolation when sourcing the new producers in the time-mapped background
            databases. Default is 'linear', which means linear interpolation between the closest 2
            databases, other options are 'nearest' (or 'closest'), which selects only the closest database.
        edge_filter_function : Callable, optional
            Function to skip edges in the graph traversal. Default is to skip all edges within
            background databases.
        cutoff: float, optional
            The cutoff value for the graph traversal. Default is 1e-9.
        max_calc: float, optional
            The maximum number of calculations to be performed by the graph traversal. Default is
            2000.
        graph_traversal : str, optional
            The graph traversal algorithm to use. Default is 'priority' (priority-first,
            using bw_temporalis TemporalisLCA). Alternative is 'bfs' (Breadth-First-Search,
            independent of TemporalisLCA, avoids per-subgraph LCA overhead).
        traverse_background : bool, optional
            If True, the graph traversal descends into background databases instead of
            stopping at the first-level background frontier. Temporal distributions defined
            on exchanges inside background databases are then honored: time-spread flows are
            sourced from the temporally-appropriate background-db variant(s). Bounded by
            ``cutoff`` and ``max_calc``. Default is False (background treated as static,
            as before).

            With ``graph_traversal='priority'``, non-referenced background variants are NOT
            placed on the priority heap. Instead, each variant's subtree is walked in full
            via proxy reads when the parent edge is reached. The referenced-system heap
            exploration order is unchanged and explored amounts are exact (identical to
            ``graph_traversal='bfs'`` for those subtrees). A one-time warning is emitted
            when this combination is used.
        *args : iterable
            Positional arguments for the graph traversal, for `bw_temporalis.TemporalisLCA` passed
            to the `EdgeExtractor` class, which inherits from `TemporalisLCA`. See `bw_temporalis`
            documentation for more information.
        **kwargs : dict
            Additional keyword arguments for the graph traversal, for `bw_temporalis.TemporalisLCA`
            passed to the EdgeExtractor class, which inherits from TemporalisLCA. See bw_temporalis
            documentation for more information.

        Returns
        -------
        pandas.DataFrame:
            A DataFrame containing the timeline of technosphere exchanges

        See Also
        --------
        - [`TimelineBuilder`][bw_timex.timeline_builder.TimelineBuilder]: Class that builds the
          timeline.

        """
        validated = BuildTimelineInputs(
            starting_datetime=starting_datetime,
            temporal_grouping=temporal_grouping,
            interpolation_type=interpolation_type,
            edge_filter_function=edge_filter_function,
            cutoff=cutoff,
            max_calc=max_calc,
            graph_traversal=graph_traversal,
            traverse_background=traverse_background,
        )
        interpolation_type = validated.interpolation_type
        graph_traversal = validated.graph_traversal

        if traverse_background and graph_traversal == "priority":
            logger.warning(
                "traverse_background=True with graph_traversal='priority': "
                "non-referenced background variants are not placed on the priority "
                "heap; each variant subtree is walked in full via proxy reads when its "
                "parent edge is reached. The referenced-system heap exploration order is "
                "unchanged and explored amounts are exact (identical to graph_traversal='bfs' "
                "for these subtrees)."
            )

        timeline_cache_key = (
            str(validated.starting_datetime),
            temporal_grouping,
            interpolation_type,
            cutoff,
            max_calc,
            graph_traversal,
            traverse_background,
            "default" if edge_filter_function is None else id(edge_filter_function),
        )
        if timeline_cache_key == self._last_timeline_build_key:
            self.timeline = self._cached_timeline
            return self.timeline[
                [
                    "date_producer",
                    "producer_name",
                    "date_consumer",
                    "consumer_name",
                    "amount",
                    "temporal_market_shares",
                ]
            ]

        if edge_filter_function is None and not traverse_background:
            logger.info(
                "No edge filter function provided. Skipping all edges in background databases."
            )
            if self._default_edge_filter_function is None:
                skippable = set()
                for db in self.database_dates_static.keys():
                    skippable.update(node.id for node in bd.Database(db))
                self._default_edge_filter_function = skippable.__contains__
            self.edge_filter_function = self._default_edge_filter_function
        elif edge_filter_function is not None:
            self.edge_filter_function = edge_filter_function
        else:
            # traverse_background=True with no user filter: BFS descends freely
            self.edge_filter_function = lambda x: False

        self.starting_datetime = starting_datetime
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type
        self.cutoff = cutoff
        self.max_calc = max_calc

        logger.info("Creating activity time mapping...")
        # Create a time mapping dict that maps each activity to a activity_time_mapping_id in the
        # format (('database', 'code'), datetime_as_integer): time_mapping_id)
        self.activity_time_mapping = TimeMappingDict(
            start_id=bd.backends.ActivityDataset.select(fn.MAX(AD.id)).scalar() + 1
        )  # making sure we get unique ids by counting up from the highest current activity id

        # pre-populate the activity time mapping dict with the static activities.
        # Doing this here because we need the temporal grouping for consistent time resolution.
        self.add_static_activities_to_activity_time_mapping()

        # When descending into the background, the BFS extractor must be able to
        # resolve any background activity to its sibling in every other static
        # variant database, so it can read the respective (non-referenced)
        # variant's exchanges. Build the full mapping up front.
        if traverse_background:
            self.add_full_interdatabase_activity_mapping()

        # Create timeline builder that does the graph traversal (similar to bw_temporalis) and
        # extracts all edges with their temporal information. Can later be used to build a timeline
        # with the TimelineBuilder.build_timeline() method.
        self.timeline_builder = TimelineBuilder(
            self.base_lca,
            self.starting_datetime,
            self.edge_filter_function,
            self.database_dates,
            self.database_dates_static,
            self.activity_time_mapping,
            self.node_collections,
            self.nodes,
            self.temporal_grouping,
            self.interpolation_type,
            self.cutoff,
            self.max_calc,
            graph_traversal=graph_traversal,
            traverse_background=traverse_background,
            interdatabase_activity_mapping=self.interdatabase_activity_mapping,
            *args,
            **kwargs,
        )

        self.timeline = self.timeline_builder.build_timeline()
        if not traverse_background:
            # When traverse_background=True the full interdatabase mapping was
            # already built by add_full_interdatabase_activity_mapping() above,
            # covering every background activity. Running this again would reset
            # entries via update({producer: {} ...}) before repopulating them,
            # producing the same final result but wasting work and creating a
            # fragile transient inconsistency. Skip it.
            self.add_interdatabase_activity_mapping_from_timeline()
        self._drop_unused_vintages_from_activity_time_mapping()
        self._last_timeline_build_key = timeline_cache_key
        self._cached_timeline = self.timeline
        self._dynamic_lcia_inventory_cache.clear()

        return self.timeline[
            [
                "date_producer",
                "producer_name",
                "date_consumer",
                "consumer_name",
                "amount",
                "temporal_market_shares",
            ]
        ]

    def lci(
        self,
        build_dynamic_biosphere: Optional[bool] = True,
        expand_technosphere: Optional[bool] = True,
        keep_activity_dimension: Optional[bool] = True,
        group_background_by_time: Optional[bool] = None,
    ) -> None:
        """
        Calculates the time-explicit LCI.

        There are two ways to generate time-explicit LCIs:
        If `expand_technosphere' is True, the biosphere and technosphere matrices are expanded by inserting
        time-specific processes via the `MatrixModifier` class by calling `TimexLCA.build_datapackage().
        Otherwise ('expand_technosphere' is False), it generates a dynamic inventory directly from the
        timeline without technosphere matrix calculations.

        Next to the choice above concerning how to retrieve the time-explicit inventory, users
        can also decide if they want to retain all temporal information at the biosphere level
        (build_dynamic_biosphere = True).
        Set `build_dynamic_biosphere` to False if you only want to get a new overall score of
        the time-explicit inventory and don't care about the timing of the emissions.
        This saves time and memory.

        Parameters
        ----------
        build_dynamic_biosphere: bool
            if True, build the dynamic biosphere matrix and calculate the dynamic LCI.
            Default is True.
        expand_technosphere: bool
            if True, creates an expanded time-explicit technosphere and biosphere matrix and
            calculates the LCI from it.
            if False, creates no new technosphere, but calculates the dynamic inventory directly
            from the timeline. Building from the timeline currently only works if
            `build_dynamic_biosphere` is also True.
        keep_activity_dimension: bool
            if True (default), the dynamic inventory keeps one column per emitting
            activity, which is what a contribution analysis needs.
            if False, emissions are accumulated per (biosphere flow, time) only, in a
            single column. Scores - static and dynamic - are identical, and so is the
            timing of the emissions, but they can no longer be attributed to the
            activities that caused them. Use this for large time-explicit systems,
            where the per-activity columns dominate memory.
        group_background_by_time: bool, optional
            How the background unit LCIs are solved. `None` (default) picks
            whichever of the two strategies needs fewer solves for this call;
            `True` forces per-time-step solving and `False` forces per-process
            solving.

            Per-time-step solving sums the background demands of every temporal
            market landing at the same point in time and solves those sums,
            costing one solve per `(time, block)` pair rather than one per
            distinct background process. It only applies with
            `expand_technosphere=False` and `keep_activity_dimension=False`,
            where the rows it sums share a column anyway - asking for it
            elsewhere logs a warning and is ignored.

            Worth pinning to `False` when re-running `lci()` several times in
            one session: grouped right-hand sides are sums specific to a run,
            so they are never cached, while per-process unit LCIs are - which
            makes every run after the first free.

        Returns
        -------
        None
            calls LCI calculations from bw2calc and calculates the dynamic inventory, if
            `build_dynamic_biosphere` is True.

        See Also
        --------
        - [`build_datapackage`][bw_timex.timex_lca.TimexLCA.build_datapackage]: Method to create
          the datapackages that contain the modifications to the technosphere and biosphere matrix
          using the [`MatrixModifier`][bw_timex.matrix_modifier.MatrixModifier] class.
        - [`calculate_dynamic_inventory`][bw_timex.timex_lca.TimexLCA.calculate_dynamic_inventory]:
          Method to calculate the dynamic inventory if `build_dynamic_biosphere` is True.
        """

        LCIInputs(
            build_dynamic_biosphere=build_dynamic_biosphere,
            expand_technosphere=expand_technosphere,
            keep_activity_dimension=keep_activity_dimension,
            group_background_by_time=group_background_by_time,
        )

        if hasattr(self, "dynamic_inventory"):
            del self.dynamic_inventory

        # Whether the initial fu solve was restored from `LCI_SOLVE_CACHE`
        # instead of running `lci_calculation`.
        self._lci_used_cached_solve = False
        # Both belong to the `self.lca` built below, so neither survives a
        # second `lci()` call.
        self._background_solver = None
        self._temporal_market_lcis = None

        if not hasattr(self, "timeline"):
            raise AttributeError(
                "Timeline not yet built. Call TimexLCA.build_timeline() first."
            )

        # mapping of the demand id to demand time
        self.demand_timing = self.create_demand_timing()

        self.fu, self.data_objs, self.remapping = self.prepare_bw_timex_inputs(
            demand=self.demand,
            method=self.method,
        )

        if expand_technosphere:
            logger.info("Expanding matrices...")
            self.datapackage = self.build_datapackage()
            data_obs = self.data_objs + self.datapackage
            self.expanded_technosphere = True  # set flag for later static lcia usage
        else:  # setup for timeline approach
            logger.info(
                "Disaggregated lci is not yet implemented with this option.\n" \
                "Please use expand_technosphere=True if you want to perform a contribution analysis on the background processes."
            )
            self.collect_temporalized_processes_from_timeline()
            data_obs = self.data_objs
            self.expanded_technosphere = False  # set flag for later lcia usage

        self.lca = LCA(
            self.fu,
            data_objs=data_obs,
            remapping_dicts=self.remapping,
        )

        logger.info("Calculating dynamic inventory...")
        if not build_dynamic_biosphere:
            self.lca.lci()
        else:  # building dynamic biosphere
            if expand_technosphere:
                # Build matrices and dicts without solving; whether the fu
                # solve is needed at all is decided from the solve cache below.
                self.lca.load_lci_data()
                self.lca.build_demand_array()
                # Placeholder so the shadow builder's __init__ can read
                # supply_array; replaced with the real (cached or freshly
                # solved) array below before any consumer uses it.
                self.lca.supply_array = np.zeros(
                    self.lca.technosphere_matrix.shape[0]
                )
                self._background_solver = self._build_background_solver()
                shadow = DynamicBiosphereBuilder(
                    self.lca,
                    self.activity_time_mapping,
                    TimeMappingDict(start_id=0),
                    self.demand_timing,
                    self.node_collections,
                    self.temporal_grouping,
                    self.database_dates,
                    self.database_dates_static,
                    self.timeline,
                    self.interdatabase_activity_mapping,
                    expand_technosphere=True,
                    background_solver=self._background_solver,
                    nodes=self.nodes,
                )
                self._prepare_background_solves(shadow)
                self._warn_if_grouping_unavailable(
                    group_background_by_time,
                    expand_technosphere=True,
                    keep_activity_dimension=keep_activity_dimension,
                )

                solve_key = self._solve_cache_key(expand_technosphere=True)
                if solve_key in LCI_SOLVE_CACHE:
                    # Skip the ~1.4 s fu solve; the cached supply_array and
                    # inventory are exactly what it would produce.
                    supply, inventory = LCI_SOLVE_CACHE[solve_key]
                    self.lca.supply_array = supply
                    self.lca.inventory = inventory
                    self._lci_used_cached_solve = True
                else:
                    # Background unit LCIs are solved by `BackgroundSolver`,
                    # never through `self.lca`, so the main matrix is solved
                    # exactly once and there is nothing to factorize for.
                    self.lca.lci_calculation()
                    LCI_SOLVE_CACHE[solve_key] = (
                        self.lca.supply_array.copy(),
                        self.lca.inventory.copy(),
                    )

                self.calculate_dynamic_inventory(
                    expand_technosphere=True,
                    keep_activity_dimension=keep_activity_dimension,
                )
            else:
                # Same planning as above, minus the functional-unit solve: the
                # supply comes from the timeline, so only the background
                # unit LCIs matter here.
                self.lca.load_lci_data()
                self._background_solver = self._build_background_solver()
                shadow = DynamicBiosphereBuilder(
                    self.lca,
                    self.activity_time_mapping,
                    TimeMappingDict(start_id=0),
                    self.demand_timing,
                    self.node_collections,
                    self.temporal_grouping,
                    self.database_dates,
                    self.database_dates_static,
                    self.timeline,
                    self.interdatabase_activity_mapping,
                    expand_technosphere=False,
                    background_solver=self._background_solver,
                    nodes=self.nodes,
                )
                group_by_time = self._plan_background_solves(
                    shadow,
                    expand_technosphere=False,
                    keep_activity_dimension=keep_activity_dimension,
                    group_background_by_time=group_background_by_time,
                )

                self.calculate_dynamic_inventory(
                    expand_technosphere=False,
                    keep_activity_dimension=keep_activity_dimension,
                    group_background_by_time=group_by_time,
                )

    def _technosphere_database_labels(self) -> tuple[np.ndarray, np.ndarray]:
        """Source database of every technosphere column and row of `self.lca`.

        Returns
        -------
        tuple of numpy.ndarray
            `(column_labels, row_labels)`, one label per technosphere column
            and row. `BlockStructure.detect` groups by these: background
            vintages never consume from each other or from the foreground, so
            one label per database is exactly the block structure.

        Notes
        -----
        Resolved from the two mappings `TimexLCA` already holds - the activity
        time mapping for time-mapped ids, `self.nodes` for everything else -
        so labelling 262k premise columns costs dictionary lookups, not one
        database query per column. If any node resists both, every column and
        row gets the same label instead, which makes `detect` fall back to a
        single block: a structure we cannot fully explain is one we must not
        split. That fallback logs a warning - it is a performance cliff, not a
        wrong answer, and otherwise leaves no trace.
        """
        time_mapping = self.activity_time_mapping.reversed
        nodes = self.nodes
        # Distinct ids, not positions: the same node is usually both a
        # process column and a product row, and reporting it twice would
        # overstate how broken the labelling is.
        unresolved_ids = set()

        def label_of(node_id):
            mapped = time_mapping.get(node_id)
            if mapped is not None:
                process_key = mapped[0]
                if isinstance(process_key, tuple):
                    return process_key[0]
            node = nodes.get(node_id)
            if node is not None:
                return node["database"]
            # Reachable: `get_background_lci_cache_key` keeps an
            # `("activity_id", act)` branch for ids whose process key is not a
            # `(db, code)` tuple, and such an id lands here.
            unresolved_ids.add(node_id)
            return ""

        n_rows, n_columns = self.lca.technosphere_matrix.shape
        column_labels = np.empty(n_columns, dtype=object)
        n_labelled_columns = 0
        for index, node_id in self.lca.dicts.activity.reversed.items():
            column_labels[index] = label_of(node_id)
            n_labelled_columns += 1
        row_labels = np.empty(n_rows, dtype=object)
        n_labelled_rows = 0
        for index, node_id in self.lca.dicts.product.reversed.items():
            row_labels[index] = label_of(node_id)
            n_labelled_rows += 1

        n_unmapped = (n_columns - n_labelled_columns) + (n_rows - n_labelled_rows)
        if unresolved_ids or n_unmapped:
            # All-or-nothing by design - one unexplained node makes the whole
            # split untrustworthy - but the cost is the difference between
            # per-vintage and whole-matrix solving, so say so rather than
            # leave a premise-sized run silently slow.
            example = (
                f" (e.g. node id {min(unresolved_ids)})" if unresolved_ids else ""
            )
            logger.warning(
                f"Could not determine the source database of "
                f"{len(unresolved_ids)} technosphere node(s){example}, and "
                f"{n_unmapped} matrix position(s) map to no node. Solving the "
                "technosphere as a single block instead of per database, "
                "which is markedly slower on large time-explicit systems."
            )
            return np.zeros(n_columns, dtype=np.int8), np.zeros(n_rows, dtype=np.int8)
        return column_labels.astype(str), row_labels.astype(str)

    def _build_background_solver(self) -> BackgroundSolver:
        """A `BackgroundSolver` over the current `self.lca`'s matrices.

        Its supply and aggregate caches are this object's, so background unit
        LCIs are reused across `lci()` calls - and, unless the object opted out
        with `use_global_lci_cache=False`, across `TimexLCA` objects in the
        session.
        """
        column_labels, row_labels = self._technosphere_database_labels()
        solver = BackgroundSolver(
            technosphere_matrix=self.lca.technosphere_matrix,
            biosphere_matrix=self.lca.biosphere_matrix,
            activity_dict=self.lca.dicts.activity,
            product_dict=self.lca.dicts.product,
            biosphere_dict=self.lca.dicts.biosphere,
            structure=BlockStructure.detect(
                self.lca.technosphere_matrix, column_labels, row_labels
            ),
        )
        solver.shared_cache = self._background_supply_cache
        solver.shared_aggregate_cache = self._background_aggregate_cache
        return solver

    def _prepare_background_solves(self, builder: DynamicBiosphereBuilder) -> None:
        """Let the solver pre-factorize for the solves `builder` implies.

        `builder` here is a shadow builder: it walks the timeline to find out
        which background activities the real build will ask for, without
        solving any of them.
        """
        demands = builder.collect_background_demands()
        self._background_solver.prepare(
            [activity_id for demand in demands.values() for activity_id in demand]
        )

    def _prepare_grouped_blocks(self, grouped: dict) -> None:
        """Factorize the blocks the grouped solves will revisit.

        Every time step solves the same few background blocks again, so
        without this each grouped solve is a fresh `spsolve` on a full block -
        far more expensive than the per-process solves grouping replaces.
        """
        solver = self._background_solver
        solver.prepare_blocks(
            block_index
            for demand in grouped.values()
            for block_index in {
                solver.block_index_for(activity_id) for activity_id in demand
            }
        )

    @staticmethod
    def _warn_if_grouping_unavailable(
        requested, expand_technosphere: bool, keep_activity_dimension: bool
    ) -> None:
        """Say so when an explicit `group_background_by_time=True` cannot apply.

        Summing the market rows of a time step is only lossless when those rows
        share a column anyway, so the request is dropped rather than honoured -
        and dropping it silently would leave the caller thinking they got it.
        """
        if not requested:
            return
        reasons = []
        if expand_technosphere:
            reasons.append("expand_technosphere=True")
        if keep_activity_dimension:
            reasons.append("keep_activity_dimension=True")
        logger.warning(
            f"group_background_by_time=True ignored: it needs "
            f"expand_technosphere=False and keep_activity_dimension=False, but "
            f"{' and '.join(reasons)} was given. Solving per background "
            f"process instead."
        )

    def _plan_background_solves(
        self,
        builder: DynamicBiosphereBuilder,
        expand_technosphere: bool,
        keep_activity_dimension: bool,
        group_background_by_time: Optional[bool] = None,
    ) -> bool:
        """Choose how the background gets solved, and prepare for that choice.

        Two strategies produce the same numbers:

        - *per background process* - one unit LCI per distinct background
          activity, linearly combined per temporal market. Costs one solve per
          uncached activity, and the results are cached across `TimexLCA`
          objects in a session, so a warm run costs nothing at all.
        - *per time step* - sum the background demands of every market row
          landing at the same time, and solve those sums. Costs one solve per
          `(time, block)` pair, caches nothing, but is independent of how many
          background processes the foreground reaches.

        Which is cheaper is a property of the model: a small foreground over
        many time steps favours the first, a wide foreground over few time
        steps the second. Both counts are known here, so when
        `group_background_by_time` is `None` the smaller one is taken; a
        `True`/`False` from the caller overrides that.

        Returns
        -------
        bool
            Whether the dynamic biosphere build should group by time step.
        """
        # Grouping sums rows that share a time step, which is only lossless
        # when they share a column anyway - i.e. with no activity dimension -
        # and is only wired for the timeline build, where a row is a column.
        can_group = not expand_technosphere and not keep_activity_dimension
        if not can_group:
            self._warn_if_grouping_unavailable(
                group_background_by_time,
                expand_technosphere=expand_technosphere,
                keep_activity_dimension=keep_activity_dimension,
            )
            demands = builder.collect_background_demands()
            self._background_solver.prepare(
                [
                    activity_id
                    for demand in demands.values()
                    for activity_id in demand
                ]
            )
            return False

        # One walk, both groupings: walking twice to choose between them can
        # cost more than the choice saves.
        demands, grouped = builder.collect_background_demand_plan()
        activity_ids = [
            activity_id for demand in demands.values() for activity_id in demand
        ]

        # An explicit request wins; `None` means decide by cost.
        if group_background_by_time is not None:
            if group_background_by_time:
                self._prepare_grouped_blocks(grouped)
            else:
                self._background_solver.prepare(activity_ids)
            return bool(group_background_by_time)

        solver = self._background_solver
        pending = {
            solver.cache_key(activity_id) for activity_id in activity_ids
        } - set(solver.shared_cache) - set(solver._instance_supply_cache)

        grouped_solves = len(
            {
                (time, solver.block_index_for(activity_id))
                for time, demand in grouped.items()
                for activity_id in demand
            }
        )

        if grouped_solves < len(pending):
            logger.info(
                f"Solving the background per time step ({grouped_solves} solves) "
                f"instead of per process ({len(pending)})."
            )
            self._prepare_grouped_blocks(grouped)
            return True

        self._background_solver.prepare(activity_ids)
        return False

    @property
    def temporal_market_lcis(self) -> dict:
        """Background LCI matrix per temporal market, keyed by time-mapped id.

        Materialized on first access from the recipes recorded during the
        matrix build: `sum(coefficient * unit supply)` over the market's
        background activities, scaled by the market's own supply, spread back
        over the biosphere as `B @ diag(x)`. Keeping the recipes instead of
        the matrices is what lets a premise-sized run finish `lci()`; anything
        that actually wants the matrices - `disaggregate_background_lci()`,
        contribution analyses - still gets them here.

        Empty unless the inventory was built with `expand_technosphere=True`,
        which is the only mode that can disaggregate a background.
        """
        if self._temporal_market_lcis is None:
            self._temporal_market_lcis = self._materialize_temporal_market_lcis()
        return self._temporal_market_lcis

    def _materialize_temporal_market_lcis(self) -> dict:
        if not hasattr(self, "dynamic_biosphere_builder") or (
            self._background_solver is None
        ):
            raise AttributeError(
                "Dynamic biosphere not yet built. "
                "Call TimexLCA.lci(build_dynamic_biosphere=True) first."
            )
        builder = self.dynamic_biosphere_builder
        solver = self._background_solver
        biosphere_matrix = solver.biosphere_matrix
        n_columns = solver.technosphere_matrix.shape[1]

        market_lcis = {}
        for market_id, recipe in builder.temporal_market_recipes.items():
            scale = builder.temporal_market_scales[market_id]
            # A market's background activities are the same process in
            # different vintages, so they sit in *different* blocks, whose
            # supply columns cover disjoint parts of the technosphere.
            # Accumulate per block and stitch the triplets together.
            supply_per_block = {}
            for activity_id, coefficient in recipe.items():
                supply = solver.unit_supply(activity_id)
                scaled = supply.values * (coefficient * scale)
                if supply.block_index in supply_per_block:
                    supply_per_block[supply.block_index] += scaled
                else:
                    supply_per_block[supply.block_index] = scaled

            rows, columns, data = [], [], []
            for block_index, values in supply_per_block.items():
                block_columns = solver.structure.blocks[block_index].columns
                # A background LCI supplies a fraction of even its own block;
                # slicing the columns it actually reaches keeps the product -
                # and the resulting matrix - to that fraction. Only exact
                # zeros are dropped, never small values.
                nonzero = np.flatnonzero(values)
                supplied = block_columns[nonzero]
                block_lci = (
                    biosphere_matrix[:, supplied].multiply(values[nonzero]).tocoo()
                )
                rows.append(block_lci.row)
                columns.append(supplied[block_lci.col])
                data.append(block_lci.data)

            market_lci = sparse.csr_matrix(
                (
                    np.concatenate(data),
                    (np.concatenate(rows), np.concatenate(columns)),
                ),
                shape=(biosphere_matrix.shape[0], n_columns),
            )
            # `multiply` keeps every stored entry of the biosphere slice, so a
            # biosphere matrix carrying explicit zeros would leak them in.
            market_lci.eliminate_zeros()
            market_lcis[market_id] = market_lci
        return market_lcis

    def disaggregate_background_lci(self) -> None:
        """
        This method disaggregates the background LCI's of the temporal markets.
        The disaggregated background LCI's allow a contribution analysis on the
        orginal inventory level as compared to the aggregated temporal market emissions.

        Returns
        -------
        None
            Stores the disaggregated background inventory in the attribute
            `dynamic_inventory_disaggregated` as a matrix and in `dynamic_inventory_disaggregated_df`
            as a DataFrame.
        """

        if not hasattr(self, "dynamic_inventory"):
            raise AttributeError(
                "Dynamic lci not yet calculated. Call TimexLCA.lci(build_dynamic_biosphere=True) first."
            )
        if not self.expanded_technosphere:
            raise NotImplementedError(
                "Currently the disaggregation of background processes is only possible\n\
                    if the expanded matrix has been built. Please call TimexLCA.lci(expand_technosphere=True) first."
            )
        # create array_dict for fast lookup
        # (key becomes index, value becomes value of 1D array)
        bio_dict_array = np.zeros(
            max(self.lca.dicts.biosphere.reversed.keys()) + 1, dtype=int
        )
        for key, value in self.lca.dicts.biosphere.reversed.items():
            bio_dict_array[key] = value

        # create biosphere_time_mapping_int for fast lookup
        biosphere_time_mapping_int = {
            (key[0], key[1].astype("int64")): value
            for key, value in self.biosphere_time_mapping.items()
        }

        self.dynamic_inventory_disaggregated = self.dynamic_inventory.tocsc()
        # 1) set all temporal market emissions to zero
        for col in self.dynamic_biosphere_builder.temporal_market_cols:
            self.dynamic_inventory_disaggregated.data[
                self.dynamic_inventory_disaggregated.indptr[
                    col
                ] : self.dynamic_inventory_disaggregated.indptr[col + 1]
            ] = 0
        self.dynamic_inventory_disaggregated.eliminate_zeros()
        # 2) add all background inventory to the dynamic inventory for all temporal markets

        self.dynamic_inventory_disaggregated = (
            self.dynamic_inventory_disaggregated.tocoo()
        )

        dynamic_inv_row_ids = self.dynamic_inventory_disaggregated.row.tolist()
        dynamic_inv_col_ids = self.dynamic_inventory_disaggregated.col.tolist()
        dynamic_inv_data = self.dynamic_inventory_disaggregated.data.tolist()

        for id_, lci in self.temporal_market_lcis.items():

            ((_, _), time) = self.activity_time_mapping.reversed[
                id_
            ]  # time of temporal market
            time_in_datetime = convert_date_string_to_datetime(
                self.temporal_grouping, str(time)
            )
            time_int = (
                np.datetime64(time_in_datetime).astype("datetime64[s]").astype("int64")
            )  # now time is a int64 in secs

            lci = lci.tocoo()

            # create list of tuples for fast lookup
            time_array = np.ones(len(lci.row), dtype="int64") * time_int
            list_of_tuples = list(zip(bio_dict_array[lci.row], time_array))

            new_rows = [biosphere_time_mapping_int[x] for x in list_of_tuples]
            dynamic_inv_row_ids.extend(new_rows)
            dynamic_inv_col_ids.extend(lci.col)
            dynamic_inv_data.extend(lci.data)

        # construct the new dynamic inventory including background inventory instead of aggregated temporal market emissions
        dynamic_inventory_disaggregated = sparse.coo_matrix(
            (dynamic_inv_data, (dynamic_inv_row_ids, dynamic_inv_col_ids)),
            shape=self.dynamic_inventory_disaggregated.shape,
        )
        self.dynamic_inventory_disaggregated = dynamic_inventory_disaggregated.tocsr()
        self.dynamic_inventory_disaggregated_df = (
            self.create_dynamic_inventory_dataframe(use_disaggregated_lci=True)
        )
        self._dynamic_lcia_inventory_cache.clear()

    def static_lcia(self) -> None:
        """
        Calculates static LCIA using time-explicit LCIs with the standard static characterization
        factors of the selected LCIA method using `bw2calc.lcia()`.

        Returns
        -------
        None
            Stores the static score in the attribute `static_score`.
        """
        if not hasattr(self, "lca"):
            raise AttributeError("LCI not yet calculated. Call TimexLCA.lci() first.")
        if self.expanded_technosphere:
            self.lca.lcia()
            self._static_score_from_timeline = None
            return

        # Without the expanded matrices there is no inventory on `self.lca` to
        # characterize, but the dynamic inventory holds the same flows (just
        # resolved in time), so characterize that with the static factors.
        if not hasattr(self, "dynamic_inventory_df"):
            raise AttributeError(
                "Dynamic inventory not yet calculated. Call "
                "TimexLCA.lci(expand_technosphere=False, build_dynamic_biosphere=True) first."
            )
        self.lca.load_lcia_data()
        diagonal = self.lca.characterization_matrix.diagonal()
        characterization_factors = {
            flow_id: diagonal[index]
            for flow_id, index in self.lca.dicts.biosphere.items()
        }
        self._static_score_from_timeline = float(
            (
                self.dynamic_inventory_df["amount"]
                * self.dynamic_inventory_df["flow"]
                .map(characterization_factors)
                .fillna(0.0)
            ).sum()
        )

    def dynamic_lcia(
        self,
        metric: str = "radiative_forcing",
        time_horizon: int = 100,
        fixed_time_horizon: bool = False,
        time_horizon_start: datetime = None,
        characterization_functions: dict = None,
        characterization_function_co2: dict = None,
        use_disaggregated_lci: bool = False,
    ) -> pd.DataFrame:
        """
        Calculates dynamic LCIA with the `DynamicCharacterization` class using the dynamic inventory
        and dynamic characterization functions. Dynamic characterization is handled by the separate
        package `dynamic_characterization` (https://dynamic-characterization.readthedocs.io).

        Dynamic characterization functions in the form of a dictionary {biosphere_flow_database_id:
        characterization_function} can be given by the user.
        If none are given, a set of default dynamic characterization functions based on IPCC AR6 are
        provided from `dynamic_characterization` package. These are mapped to the biosphere3 flows
        of the chosen static climate change impact category. If there is no characterization
        function for a biosphere flow, it will be ignored.

        Dynamic climate change metrics are supported for "GWP", "radiative_forcing",
        "pGWP", "pGTP", and "prospective_radiative_forcing".
        The time horizon for the impact assessment can be set with the `time_horizon` parameter,
        defaulting to 100 years. The `fixed_time_horizon` parameter determines whether the emission
        time horizon for all emissions is calculated from a specific starting point `time_horizon_start`
        (`fixed_time_horizon=True`) or from the time of the emission (`fixed_time_horizon=False`).
        The former is the implementation of the Levasseur approach
        (see https://doi.org/10.1021/es9030003), while the latter is how conventional LCA is done.

        Parameters
        ----------
        metric : str, optional
            the metric for which the dynamic LCIA should be calculated. Default is
            "radiative_forcing". Available: "GWP", "radiative_forcing", "pGWP",
            "pGTP", and "prospective_radiative_forcing"
        time_horizon: int, optional
            the time horizon for the impact assessment. Unit is years. Default is 100.
        fixed_time_horizon: bool, optional
            Whether the emission time horizon for all emissions is calculated from the functional
            unit (fixed_time_horizon=True) or from the time of the emission
            (fixed_time_horizon=False). Default is False.
        time_horizon_start: pd.Timestamp, optional
            The starting timestamp of the time horizon for the dynamic characterization. Only needed
            for fixed time horizons. Default is datetime.now().
        characterization_functions: dict, optional
            Dict of the form {biosphere_flow_database_id: characterization_function}. Default is
            None, which triggers the use of the provided dynamic characterization functions based on
            IPCC AR6 Chapter 7.
        characterization_function_co2: Callable, optional
            Characterization function for CO2 emissions. Necessary if GWP metric is chosen. Default
            is None, which triggers the use of the provided dynamic characterization function of CO2
            based on IPCC AR6 Chapter 7.
        use_disaggregated_lci: bool, optional
            Whether to use the disaggregated background LCI for the dynamic LCIA. Default is False.
            Use True if you want to perform a contribution analysis on the disaggregated background.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with the characterized inventory for the chosen metric and parameters.

        See Also
        --------
        - [`dynamic_characterization`](https://dynamic-characterization.readthedocs.io/en/latest/):
          Package handling the dynamic characterization.
        """

        DynamicLCIAInputs(
            metric=metric,
            time_horizon=time_horizon,
            fixed_time_horizon=fixed_time_horizon,
            time_horizon_start=time_horizon_start,
            characterization_functions=characterization_functions,
            characterization_function_co2=characterization_function_co2,
            use_disaggregated_lci=use_disaggregated_lci,
        )

        if not hasattr(self, "dynamic_inventory"):
            raise AttributeError(
                "Dynamic lci not yet calculated. Call TimexLCA.lci(build_dynamic_biosphere=True) first."
            )

        self.current_metric = metric
        self.current_time_horizon = time_horizon

        if use_disaggregated_lci:
            if not self.expanded_technosphere:
                raise NotImplementedError(
                    "Currently the disaggregation of background processes is only possible if the \
                        expanded matrix has been built. Please call TimexLCA.lci(expand_technosphere=True) first."
                )
            # Check if disaggregated inventory is available
            # otherwise disaggregate the background LCI
            if not hasattr(self, "dynamic_inventory_disaggregated"):
                logger.info("Disaggregating background LCI...")
                self.disaggregate_background_lci()
                logger.info("Background LCI's disaggregated.")
            dynamic_inventory_df = self.dynamic_inventory_disaggregated_df

        else:
            dynamic_inventory_df = self.dynamic_inventory_df

        cache_key = ("disaggregated" if use_disaggregated_lci else "aggregate")
        if cache_key not in self._dynamic_lcia_inventory_cache:
            inventory_rounded = dynamic_inventory_df.copy()
            inventory_rounded.date = round_datetime_series_to_year(
                inventory_rounded.date
            )
            self._dynamic_lcia_inventory_cache[cache_key] = (
                inventory_rounded.groupby(inventory_rounded.columns.tolist())
                .sum()
                .reset_index()
            )

        # Set a default for inventory_in_time_horizon using the full dynamic_inventory_df
        inventory_in_time_horizon = self._dynamic_lcia_inventory_cache[cache_key]

        # Calculate the latest considered impact date
        t0_date = pd.Timestamp(self.timeline_builder.edge_extractor.t0.date[0])
        latest_considered_impact = t0_date + pd.DateOffset(years=time_horizon)

        # Update inventory_in_time_horizon if a fixed time horizon is used
        if fixed_time_horizon:
            last_emission = dynamic_inventory_df.date.max()
            if latest_considered_impact < last_emission:
                logger.warning(
                    "An emission occurs outside of the specified time horizon and will not be \
                        characterized. Please make sure this is intended."
                )
                inventory_in_time_horizon = dynamic_inventory_df[
                    dynamic_inventory_df.date <= latest_considered_impact
                ]

        if not time_horizon_start:
            time_horizon_start = t0_date

        self.characterized_inventory = characterize(
            dynamic_inventory_df=inventory_in_time_horizon,
            metric=metric,
            characterization_functions=characterization_functions,
            base_lcia_method=self.method,
            time_horizon=time_horizon,
            fixed_time_horizon=fixed_time_horizon,
            time_horizon_start=time_horizon_start,
            characterization_function_co2=characterization_function_co2,
        )

        return self.characterized_inventory

    ###################
    # Core properties #
    ###################

    @property
    def base_score(self) -> float:
        """
        Score of the base LCA, i.e., the "normal" LCA without time-explicit information.
        Same as bw2calc.LCA.score
        """
        return self.base_lca.score

    @property
    def static_score(self) -> float:
        """
        Score resulting from the static LCIA of the time-explicit inventory.
        """
        if not hasattr(self, "lca"):
            raise AttributeError("LCI not yet calculated. Call TimexLCA.lci() first.")
        if not self.expanded_technosphere:
            # Characterized from the dynamic inventory, not from `self.lca`,
            # which has no expanded inventory to score.
            if getattr(self, "_static_score_from_timeline", None) is None:
                raise AttributeError(
                    "Static score not yet calculated. Call TimexLCA.static_lcia() first."
                )
            return self._static_score_from_timeline
        return self.lca.score

    @property
    def dynamic_score(self) -> float:
        """
        Score resulting from the dynamic LCIA of the time-explicit inventory.
        """
        if not hasattr(self, "characterized_inventory"):
            raise AttributeError(
                "Characterized inventory not yet calculated. Call TimexLCA.dynamic_lcia() first."
            )
        return self.characterized_inventory["amount"].sum()

    ###############################################
    # Other core functions for the inner workings #
    ###############################################

    def build_datapackage(self) -> list:
        """
        Creates the datapackages that contain the modifications to the technosphere and biosphere
        matrix using the `MatrixModifier` class.

        Returns
        -------
        list
            List of datapackages that contain the modifications to the technosphere and biosphere
            matrix

        See Also
        --------
        - [`MatrixModifier`][bw_timex.matrix_modifier.MatrixModifier]: Class that handles the
          technosphere and biosphere matrix modifications.
        """
        self.matrix_modifier = MatrixModifier(
            self.timeline,
            self.database_dates_static,
            self.demand_timing,
            self.nodes,
            self.interdatabase_activity_mapping,
        )
        self.node_collections["temporal_markets"] = (
            self.matrix_modifier.temporal_market_ids
        )
        self.node_collections["temporalized_processes"] = (
            self.matrix_modifier.temporalized_process_ids
        )
        return self.matrix_modifier.create_datapackage()

    def _solve_cache_key(self, expand_technosphere: bool) -> tuple:
        """Fingerprint identifying a unique fu solve on this scenario.

        Reuse from ``LCI_SOLVE_CACHE`` is only safe when the consuming
        scenario produces *identical* technosphere/biosphere matrices and
        demand RHS. We hash the matrix data directly so any change in the
        timeline (different relinking, different temporal_grouping, etc.)
        produces a different key — `len(activity_time_mapping)` alone
        collides for differently-relinked scenarios of equal size.
        """
        import hashlib

        T = self.lca.technosphere_matrix
        B = self.lca.biosphere_matrix
        d = self.lca.demand_array
        tech_hash = hashlib.md5(
            T.data.tobytes() + T.indices.tobytes() + T.indptr.tobytes()
        ).digest()
        bio_hash = hashlib.md5(
            B.data.tobytes() + B.indices.tobytes() + B.indptr.tobytes()
        ).digest()
        demand_hash = hashlib.md5(np.asarray(d).tobytes()).digest()
        return (
            bd.projects.current,
            bool(expand_technosphere),
            T.shape,
            B.shape,
            tech_hash,
            bio_hash,
            demand_hash,
        )

    def calculate_dynamic_inventory(
        self,
        expand_technosphere=True,
        keep_activity_dimension=True,
        group_background_by_time=False,
    ) -> None:
        """
        Calculates the dynamic inventory, by first creating a dynamic biosphere matrix using the
        `DynamicBiosphereBuilder` class and then multiplying it with the dynamic supply array. The
        dynamic inventory matrix is stored in the attribute `dynamic_inventory`. It is also
        converted to a DataFrame and stored in the attribute `dynamic_inventory_df`.

        Parameters
        ----------
        expand_technosphere: bool
            A boolean indicating if the dynamic biosphere matrix is built directly from the
            expanded matrices or from the timeline. Default is True (from expanded matrices).

        Returns
        -------
        None
            calculates the dynamic inventory and stores it in the attribute
            `dynamic_inventory` as a matrix and in `dynamic_inventory_df` as a DataFrame.
            Also calculates and stores the lci of the temporal markets in the attribute
            self.temporal_market_lcis for use in contribution analysis of the background processes.

        See Also
        --------
        - [`DynamicBiosphereBuilder`][bw_timex.dynamic_biosphere_builder.DynamicBiosphereBuilder]:
          Class for creating the dynamic biosphere matrix and inventory.
        """

        if not hasattr(self, "lca"):
            raise AttributeError(
                "Time-explicit LCA object does not exist. Call TimexLCA.lci() first."
            )

        self.biosphere_time_mapping = TimeMappingDict(start_id=0)
        # Recipes recorded below replace whatever a previous build left behind.
        self._temporal_market_lcis = None
        if self._background_solver is None:
            # Called directly rather than through `lci()`, which normally
            # builds the solver as part of planning the background solves.
            self._background_solver = self._build_background_solver()

        self.dynamic_biosphere_builder = DynamicBiosphereBuilder(
            self.lca,
            self.activity_time_mapping,
            self.biosphere_time_mapping,
            self.demand_timing,
            self.node_collections,
            self.temporal_grouping,
            self.database_dates,
            self.database_dates_static,
            self.timeline,
            self.interdatabase_activity_mapping,
            expand_technosphere=expand_technosphere,
            background_solver=self._background_solver,
            nodes=self.nodes,
            keep_activity_dimension=keep_activity_dimension,
            group_background_by_time=group_background_by_time,
        )

        # Which blocks are worth pre-factorizing is planned upfront by `lci()`.
        # Calling `calculate_dynamic_inventory` directly skips that planning
        # but still produces correct results.
        self.dynamic_biosphere_matrix = (
            self.dynamic_biosphere_builder.build_dynamic_biosphere_matrix(
                expand_technosphere=expand_technosphere,
            )
        )

        # Build the dynamic inventory
        if keep_activity_dimension:
            count = len(self.dynamic_biosphere_builder.dynamic_supply_array)
            # diagonalization of supply array keeps the dimension of the process, which we want to pass
            # as additional information to the dynamic inventory dict
            diagonal_supply_array = sparse.spdiags(
                [self.dynamic_biosphere_builder.dynamic_supply_array], [0], count, count
            )
            self.dynamic_inventory = self.dynamic_biosphere_matrix @ diagonal_supply_array
        else:
            # There is no activity dimension left to scale: the builder already
            # applied each activity's supply while accumulating.
            self.dynamic_inventory = self.dynamic_biosphere_matrix

        self.dynamic_inventory_df = self.create_dynamic_inventory_dataframe(
            expand_technosphere, keep_activity_dimension=keep_activity_dimension
        )
        self._dynamic_lcia_inventory_cache.clear()

    def create_dynamic_inventory_dataframe(
        self,
        expand_technosphere=True,
        use_disaggregated_lci=False,
        keep_activity_dimension=True,
    ) -> pd.DataFrame:
        """
        Brings the dynamic inventory from its matrix form in `dynamic_inventory` into the
        format of a pandas.DataFrame, with the right structure to later apply dynamic
        characterization functions.

        Format is:

        +------------+--------+------+----------+
        |   date     | amount | flow | activity |
        +============+========+======+==========+
        |  datetime  |   33   |  1   |    2     |
        +------------+--------+------+----------+
        |  datetime  |   32   |  1   |    2     |
        +------------+--------+------+----------+
        |  datetime  |   31   |  1   |    2     |
        +------------+--------+------+----------+

        - date: datetime, e.g. '2024-01-01 00:00:00'
        - flow: flow id
        - activity: activity id

        Parameters
        ----------
        expand_technosphere: bool
            A boolean indicating if the dynamic biosphere matrix is built directly from the
            expanded matrices or from the timeline. Default is True.

        Returns
        -------
        pandas.DataFrame, dynamic inventory in DataFrame format

        """
        dynamic_inventory = (
            self.dynamic_inventory_disaggregated
            if use_disaggregated_lci
            else self.dynamic_inventory
        )
        dynamic_inventory = dynamic_inventory.tocoo()

        if not keep_activity_dimension:
            # Single aggregated column: the emissions are no longer attributable
            # to an activity.
            activities = [-1] * len(dynamic_inventory.col)
        elif expand_technosphere:
            activities = [
                self.lca.activity_dict.reversed[col] for col in dynamic_inventory.col
            ]
        else:
            timeline_tmap = self.timeline["time_mapped_producer"].to_numpy()
            activities = timeline_tmap[dynamic_inventory.col].tolist()

        rows_resolved = [
            self.biosphere_time_mapping.reversed[row] for row in dynamic_inventory.row
        ]
        dates = [item[1] for item in rows_resolved]
        flows = [item[0] for item in rows_resolved]

        df = pd.DataFrame(
            {
                "date": dates,
                "amount": dynamic_inventory.data,
                "flow": flows,
                "activity": activities,
            }
        )

        df.date = df.date.astype("datetime64[s]")

        return df.sort_values(by=["date", "amount"], ascending=[True, False])

    #############
    # For setup #
    #############

    def clean_databases(self) -> None:
        """
        Reprocess the databases that have been modified since they were last processed.

        Editing a database invalidates its datapackage, and the next calculation has to
        rebuild it. For large background databases that takes tens of seconds each, so
        the databases concerned are logged instead of the calculation appearing to hang.

        Returns
        -------
        None
        """
        modified_databases = sorted(
            name for name in databases if databases[name].get("dirty")
        )
        if not modified_databases:
            return

        logger.info(
            f"Reprocessing {len(modified_databases)} modified database(s) before "
            f"calculating: {', '.join(modified_databases)}. "
            "This can take a while for large databases."
        )
        databases.clean()
        logger.info("Done reprocessing modified databases.")

    def prepare_base_lca_inputs(
        self,
        demand=None,
        method=None,
        weighting=None,
        normalization=None,
        demands=None,
        remapping=True,
        demand_database_last=True,
    ) -> tuple:
        """
        Prepare LCA input arguments in Brightway2.5 style.

        Adapted bw2data.compat.py

        The difference to the original method is that we load all available databases into the
        matrices instead of just the ones depending on the demand. We need this for the creation of
        the time mapping dict that creates a mapping between the producer id and the reference
        timing of the databases in the `database_dates`.

        Parameters
        ----------
        demand : dict[object: float]
            The demand for which the LCA will be calculated. The keys can be Brightway `Node`
            instances, `(database, code)` tuples, or integer ids.
        method : tuple
            Tuple defining the LCIA method, such as `('foo', 'bar')`. Only needed if not passing
            `data_objs`.
        weighting : tuple
            Tuple defining the LCIA weighting, such as `('foo', 'bar')`. Only needed if not passing
            `data_objs`.
        normalization: str
        demands: list of dicts of demands
        remapping: bool
            If True, remap dictionaries
        demand_database_last: bool
            If True, add the demand databases last in the list `database_names`.

        Returns
        -------
        tuple
            Indexed demand, data objects, and remapping dictionaries

        See Also
        --------
        - [`bw2data.compat.prepare_lca_inputs`](https://github.com/brightway-lca/brightway2-data/blob/main/bw2data/compat.py):
          Original code this function is adapted from.
        """
        if not projects.dataset.data.get("25"):
            raise Brightway2Project(
                "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
            )

        self.clean_databases()
        data_objs = []
        remapping_dicts = None

        # The base LCA only needs the databases that the demand depends on: the
        # dynamic (foreground) ones, the ones holding the demand, and whatever
        # those link to. Time-specific background databases that nothing
        # depends on would only inflate the base technosphere matrix here; they
        # are brought in later, when `lci()` relinks the processes to them.
        dynamic_database_names = {
            db
            for db, date in self.database_dates.items()
            if not isinstance(date, datetime)
        }
        demand_database_names = list(
            dynamic_database_names
            | {bd.get_node(id=get_id(key))["database"] for key in (demand or {})}
        )

        if demand_database_names:
            database_names = set.union(
                *[
                    Database(db_label).find_graph_dependents()
                    for db_label in demand_database_names
                ]
            )

            if demand_database_last:
                database_names = [
                    x for x in database_names if x not in demand_database_names
                ] + demand_database_names

            # Remembered so that `create_activity_time_mapping` knows for which
            # databases the base LCA's matrix is the authority on which nodes
            # get a technosphere column.
            self._base_lca_database_names = set(database_names)

            data_objs.extend([Database(obj).datapackage() for obj in database_names])

            if remapping:
                # This is technically wrong - we could have more complicated queries
                # to determine what is truly a product, activity, etc.
                # However, for the default database schema, we know that each node
                # has a unique ID, so this won't produce incorrect responses,
                # just too many values. As the dictionary only exists once, this is
                # not really a problem.
                reversed_mapping = {
                    i: (d, c)
                    for d, c, i in AD.select(AD.database, AD.code, AD.id)
                    .where(AD.database << database_names)
                    .tuples()
                }
                remapping_dicts = {
                    "activity": reversed_mapping,
                    "product": reversed_mapping,
                    "biosphere": reversed_mapping,
                }

        if method:
            assert method in methods
            data_objs.append(Method(method).datapackage())
        if weighting:
            assert weighting in weightings
            data_objs.append(Weighting(weighting).datapackage())
        if normalization:
            assert normalization in normalizations
            data_objs.append(Normalization(normalization).datapackage())

        if demands:
            indexed_demand = [{get_id(k): v for k, v in dct.items()} for dct in demands]
        elif demand:
            indexed_demand = {get_id(k): v for k, v in demand.items()}
        else:
            indexed_demand = None

        return indexed_demand, data_objs, remapping_dicts

    def _drop_unused_vintages_from_activity_time_mapping(self) -> None:
        """
        Forget the static activities of vintages the timeline never sources from.

        `lci()` loads only the databases
        [`databases_used_by_timeline`][bw_timex.timex_lca.TimexLCA.databases_used_by_timeline]
        returns, so the processes of the other ones get no technosphere column.
        `activity_time_mapping` is pre-populated with every mapped database
        before the timeline exists, and the dynamic biosphere matrix is sized
        by its length, so the two have to be pruned together.

        Returns
        -------
        None
            Removes the pruned databases' entries from `activity_time_mapping`.
        """
        used = set(self.databases_used_by_timeline())
        unused = set(self.database_dates) - used
        if not unused:
            return
        for key in [key for key in self.activity_time_mapping if key[0][0] in unused]:
            del self.activity_time_mapping[key]
        # `reversed` is cached and only refreshed when this flag is set.
        self.activity_time_mapping._modified = True

    def databases_used_by_timeline(self) -> list:
        """
        The mapped databases the time-explicit matrices actually reference.

        A project can hold vintages a given study never sources from - e.g. a
        2050 vintage for a system that ends in 2042, or the vintages of an
        unrelated study. They get no temporal market share, and the expanded
        technosphere only ever references a background database that a row's
        `temporal_market_shares` names, so their processes would only add
        columns to the matrix that has to be solved.

        Kept are the dynamic databases, the databases holding the demand, every
        database a temporal market draws on, and the databases of the traversed
        processes themselves (which is what `traverse_background` adds). Their
        graph dependents are added by the caller.

        Returns
        -------
        list
            Names of the databases to load, in `database_dates` order. All of
            them, if no timeline has been built yet.
        """
        timeline = getattr(self, "timeline", None)
        if timeline is None:
            return list(self.database_dates)

        used = {
            database
            for database, date in self.database_dates.items()
            if not isinstance(date, datetime)
        }
        used.update(
            bd.get_node(id=get_id(key))["database"] for key in (self.demand or {})
        )
        for shares in timeline["temporal_market_shares"]:
            if shares:
                used.update(shares)
        for node_id in set(timeline["producer"]).union(timeline["consumer"]):
            if node_id != -1:
                used.add(self.nodes[node_id]["database"])

        return [database for database in self.database_dates if database in used]

    def prepare_bw_timex_inputs(
        self,
        demand=None,
        method=None,
        weighting=None,
        normalization=None,
        demands=None,
        remapping=True,
        demand_database_last=True,
    ) -> tuple:
        """
        Prepare LCA input arguments in Brightway 2.5 style.

        ORIGINALLY FROM bw2data.compat.py

        Changes include:
        - always load all databases in demand_database_names
        - indexed_demand has the id of the new consumer_id of the "exploded" demand

        Parameters
        ----------
        demand : dict[object: float]
            The demand for which the LCA will be calculated. The keys can be Brightway `Node`
            instances, `(database, code)` tuples, or integer ids.
        method : tuple
            Tuple defining the LCIA method, such as `('foo', 'bar')`. Only needed if not passing
            `data_objs`.
        weighting : tuple
            Tuple defining the LCIA weighting, such as `('foo', 'bar')`. Only needed if not passing
            `data_objs`.
        normalization: str
        demands: list of dicts of demands
        remapping: bool
            If True, remap dictionaries
        demand_database_last: bool
            If True, add the demand databases last in the list `database_names`.

        Returns
        -------
        tuple
            Indexed demand, data objects, and remapping dictionaries

        See Also
        --------
        - [`bw2data.compat.prepare_lca_inputs`](https://github.com/brightway-lca/brightway2-data/blob/main/bw2data/compat.py):
          Original code this function is adapted from.
        """

        if not projects.dataset.data.get("25"):
            raise Brightway2Project(
                "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
            )

        self.clean_databases()
        data_objs = []
        remapping_dicts = None

        demand_database_names = self.databases_used_by_timeline()

        if demand_database_names:
            database_names = set.union(
                *[
                    Database(db_label).find_graph_dependents()
                    for db_label in demand_database_names
                ]
            )

            if demand_database_last:
                database_names = [
                    x for x in database_names if x not in demand_database_names
                ] + demand_database_names

            data_objs.extend([Database(obj).datapackage() for obj in database_names])

            if remapping:
                # This is technically wrong - we could have more complicated queries
                # to determine what is truly a product, activity, etc.
                # However, for the default database schema, we know that each node
                # has a unique ID, so this won't produce incorrect responses,
                # just too many values. As the dictionary only exists once, this is
                # not really a problem.
                reversed_mapping = {
                    i: (d, c)
                    for d, c, i in AD.select(AD.database, AD.code, AD.id)
                    .where(AD.database << database_names)
                    .tuples()
                }
                remapping_dicts = {
                    "activity": reversed_mapping,
                    "product": reversed_mapping,
                    "biosphere": reversed_mapping,
                }

        if method:
            assert method in methods
            data_objs.append(Method(method).datapackage())
        if weighting:
            assert weighting in weightings
            data_objs.append(Weighting(weighting).datapackage())
        if normalization:
            assert normalization in normalizations
            data_objs.append(Normalization(normalization).datapackage())

        if demands:
            indexed_demand = [
                self._build_indexed_demand(dct) for dct in demands
            ]
        elif demand:
            indexed_demand = self._build_indexed_demand(demand)
        else:
            indexed_demand = None

        return indexed_demand, data_objs, remapping_dicts

    def create_node_collections(self) -> None:
        """
        Creates a dict of collections of nodes that will be useful down the line, e.g. to determine
        static nodes for the graph traversal or create the dynamic biosphere matrix.
        Available collections are:

        - ``background``: set of node ids of all processes that depend on the demand processes and are in the background databases
        - ``foreground``: set of node ids of all processes that are not in the background databases
        - ``first_level_background_static``: set of node ids of all processes that are in the background databases and are directly linked to the demand processes

        Returns
        -------
            None
                adds the `node_collections containing` the above-mentioned collections,
                as well as interdatabase_activity_mapping
        """
        self.node_collections = {}

        # Original variable names preserved, set types for performance and uniqueness
        demand_database_names = {
            db
            for db in self.database_dates.keys()
            if db not in self.database_dates_static.keys()
        }

        demand_dependent_database_names = set()
        demand_dependent_database_names.update(demand_database_names)
        for db in demand_database_names:
            demand_dependent_database_names.update(
                bd.Database(db).find_graph_dependents()
            )

        demand_dependent_background_database_names = (
            demand_dependent_database_names & self.database_dates_static.keys()
        )

        # Only the ids are needed here, so we query them directly instead of
        # instantiating a node proxy (and unpickling its data blob) for every
        # process in the background databases.
        if demand_dependent_background_database_names:
            background = {
                row[0]
                for row in AD.select(AD.id)
                .where(AD.database << list(demand_dependent_background_database_names))
                .tuples()
            }
        else:
            background = set()
        self.node_collections["background"] = background

        first_level_background_static = set()
        foreground = set()
        for db_name in demand_database_names:
            for node in bd.Database(db_name):
                foreground.add(node.id)
                for exc in node.technosphere():
                    if (
                        exc.input["database"]
                        in demand_dependent_background_database_names
                    ):
                        first_level_background_static.add(exc.input.id)
                for exc in node.substitution():
                    if (
                        exc.input["database"]
                        in demand_dependent_background_database_names
                    ):
                        first_level_background_static.add(exc.input.id)

        self.node_collections["foreground"] = foreground
        self.node_collections["first_level_background_static"] = (
            first_level_background_static
        )

    def add_full_interdatabase_activity_mapping(self) -> None:
        """
        Populate ``interdatabase_activity_mapping`` for every background activity
        across all static variant databases.

        Unlike ``add_interdatabase_activity_mapping_from_timeline`` (which only
        maps producers that appear in the finished timeline), this pre-pass maps
        every static-database node to its sibling in every other static database,
        so the BFS extractor can resolve and read the respective (non-referenced)
        variant's exchanges while it is still traversing.
        """
        static_dbs = set(self.database_dates_static.keys())
        tuples_dict = {}
        for node in self.nodes.values():
            if node["database"] not in static_dbs:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            tuples_dict.setdefault(key, node.id)
        # Build the anchor -> {db: id} mapping in a plain dict first; indexing
        # the InterDatabaseMapping itself would prematurely trigger
        # make_reciprocal() before every entry has been added.
        built = {}
        for node in self.nodes.values():
            if node["database"] not in static_dbs:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            anchor = tuples_dict[key]
            built.setdefault(anchor, {})[node["database"]] = node.id
        self.interdatabase_activity_mapping.update(built)
        self.interdatabase_activity_mapping.make_reciprocal()

    def add_interdatabase_activity_mapping_from_timeline(self) -> None:
        """
        Fills the interdatabase_activity_mapping, which is a SetList of the matching processes
        across background databases in the format of {(id, database_name_1), (id, database_name_2)}
        with only those activities and background databases that are actually mapped in the
        timeline.


        Returns
        -------
        None
            Adds the ids of producers in other background databases
            (only those interpolated to in the timeline) to the `interdatabase_activity_mapping`.
        """
        if not hasattr(self, "timeline"):
            raise AttributeError(
                "Timeline not yet built. Call TimexLCA.build_timeline() first."
            )

        # The timeline builder already resolved every temporal-market producer to
        # its counterparts while computing the market shares. Reuse that instead
        # of scanning every background node a second time.
        # The builder's scan is scoped to static databases only; this is inert
        # because the mapping is only ever queried for static db names (drawn
        # from temporal_market_shares in the timeline, which excludes foreground).
        matches = getattr(self.timeline_builder, "market_producer_matches", None)
        if matches:
            self.interdatabase_activity_mapping.update(matches)
            self.interdatabase_activity_mapping.make_reciprocal()
            return

        filtered_timeline = self.timeline.loc[
            self.timeline.temporal_market_shares.notnull()
        ]
        unique_producers = filtered_timeline.producer.unique()

        self.interdatabase_activity_mapping.update(
            {producer: {} for producer in unique_producers}
        )

        producer_tuples_dict = {}
        for producer in unique_producers:
            producer_node = self.nodes[producer]
            producer_tuples_dict[
                (
                    producer_node["name"],
                    producer_node.get("reference product"),
                    producer_node["location"],
                )
            ] = producer

        unique_produces_tuples = producer_tuples_dict.keys()

        for node in self.nodes.values():
            node_tuple = (node["name"], node.get("reference product"), node["location"])
            if node_tuple in unique_produces_tuples:
                producer_id = producer_tuples_dict[node_tuple]
                self.interdatabase_activity_mapping[producer_id][
                    node["database"]
                ] = node.id

        self.interdatabase_activity_mapping.make_reciprocal()

    def collect_temporalized_processes_from_timeline(self) -> None:
        """
        Prepares the input for the LCA from the timeline.

        Returns
        -------
        None
            Adds "temporal_markets" and "temporalized_processes" to the
            node_collections based on the timeline.

        """
        unique_producers = (
            self.timeline.groupby(["producer", "time_mapped_producer"])
            .count()
            .index.values
        )

        market_time_mapped = set(
            self.timeline.loc[
                self.timeline.temporal_market_shares.notnull(),
                "time_mapped_producer",
            ]
        )

        temporal_market_ids = set()
        temporalized_process_ids = set()
        for producer, time_mapped_producer in unique_producers:
            if time_mapped_producer in market_time_mapped:
                temporal_market_ids.add(time_mapped_producer)
            else:
                temporalized_process_ids.add(time_mapped_producer)

        self.node_collections["temporal_markets"] = temporal_market_ids
        self.node_collections["temporalized_processes"] = temporalized_process_ids

    def add_static_activities_to_activity_time_mapping(self) -> None:
        """
        Adds all activities from the static LCA to `activity_time_mapping`, an instance of
        `TimeMappingDict`. This gives a unique mapping in the form of
        (('database', 'code'), datetime_as_integer): time_mapping_id) that is later used to uniquely
        identify time-resolved processes. Here, the activity_time_mapping is the
        pre-population with the static activities. The time-explicit activities (from other
        temporalized background databases) are added later on by the TimelineBuilder.
        Activities in the foreground database are mapped with
        (('database', 'code'), "dynamic"): time_mapping_id)" as their timing is not yet known.

        Returns
        -------
        None
            adds the static activities to the `activity_time_mapping`
        """
        static_db_time_mapping = {
            db: extract_date_as_integer(time, self.temporal_grouping)
            for db, time in self.database_dates.items()
            if isinstance(time, datetime)
        }
        dynamic_db_time_mapping = {
            db: time for db, time in self.database_dates.items() if isinstance(time, str)
        }

        # Only nodes that occupy a technosphere column are mapped. For the
        # databases in the base LCA, its matrix says exactly which those are;
        # the remaining time-specific databases are not in that matrix (they
        # are relinked to later), so there we go by node type, excluding
        # explicit product nodes, which are rows only.
        base_activity_ids = set(self.base_lca.dicts.activity.keys())
        base_lca_databases = self._base_lca_database_names
        product_node_types = set(bd.labels.product_node_types)

        for idx, node in self.nodes.items():  # activity ids
            key = node.key  # ('database', 'code')
            db_name = key[0]
            if db_name in base_lca_databases:
                if idx not in base_activity_ids:
                    continue
            elif node.get("type") in product_node_types:
                continue
            if db_name in dynamic_db_time_mapping:
                self.activity_time_mapping.add(
                    (key, dynamic_db_time_mapping[db_name]), unique_id=idx
                )
            elif db_name in static_db_time_mapping:
                self.activity_time_mapping.add(
                    (key, static_db_time_mapping[db_name]), unique_id=idx
                )
            else:
                raise ValueError(f"Time of activity {key} is neither datetime nor str.")

    def create_demand_timing(self) -> dict:
        """
        Generate a dictionary that maps demand id (key) to timing (value) for the demands in the
        product system. It searches the timeline for the FU rows (consumer == -1) and looks up the
        timing of the producing process. For demands keyed by an explicit product node, the producer
        in the timeline is the process producing that product, so we resolve the product → process
        relationship via the production exchange.

        Returns
        -------
        dict
            Dictionary mapping demand ids to reference timing for the specified demands.
        """
        process_id_by_demand_id = {
            bd.get_activity(key).id: self._resolve_demand_to_process_id(key)
            for key in self.demand.keys()
        }
        fu_rows = self.timeline[self.timeline["consumer"] == -1]
        timing_by_process_id = {
            row.producer: row.hash_producer for row in fu_rows.itertuples()
        }

        missing_process_ids = sorted(
            set(process_id_by_demand_id.values()) - set(timing_by_process_id)
        )
        if missing_process_ids:
            functional_unit_producers = sorted(set(timing_by_process_id))
            raise ValueError(
                "Could not find functional-unit timing rows for producing process id(s) "
                f"{missing_process_ids}. The existing timeline contains functional-unit "
                f"producer id(s) {functional_unit_producers}. This usually means the "
                "Brightway database or demand nodes changed after build_timeline(); "
                "recreate the TimexLCA object and rebuild the timeline before calling lci()."
            )


        self.demand_timing = {
            demand_id: timing_by_process_id[process_id]
            for demand_id, process_id in process_id_by_demand_id.items()
            if process_id in timing_by_process_id
        }
        return self.demand_timing

    def _build_indexed_demand(self, demand_dict) -> dict:
        """Map a demand dict to time-mapped producer ids, distributed across
        every install-vintage cohort produced by an output-side temporal
        distribution.

        Each FU row in the timeline corresponds to one cohort of the demand's
        producing process; ``row.amount`` is the cohort's share of the
        original demand value. Summing the FU rows reproduces the user's
        demand magnitude while preserving the cohort split, so that
        downstream matrix-modifier logic (which keys temporal markets by
        ``time_mapped_producer``) routes each cohort's inputs to the
        appropriate background database.
        """
        if not hasattr(self, "timeline"):
            raise AttributeError(
                "Timeline not yet built. Call TimexLCA.build_timeline() first."
            )

        fu_rows = self.timeline[self.timeline["consumer"] == -1]
        indexed = {}
        for k, v in demand_dict.items():
            process_id = self._resolve_demand_to_process_id(k)
            cohort_rows = fu_rows[fu_rows["producer"] == process_id]
            if cohort_rows.empty:
                raise ValueError(
                    f"No functional-unit rows in timeline for demand `{k}` "
                    f"(process id {process_id}). Did you call build_timeline?"
                )
            cohort_total = float(cohort_rows["amount"].sum())
            if cohort_total == 0:
                raise ValueError(
                    f"Functional-unit rows for demand `{k}` sum to zero amount."
                )
            scale = float(v) / cohort_total
            for row in cohort_rows.itertuples():
                indexed[row.time_mapped_producer] = (
                    indexed.get(row.time_mapped_producer, 0.0)
                    + float(row.amount) * scale
                )
        return indexed

    def _resolve_demand_to_process_id(self, key) -> int:
        """Return the id of the process producing ``key``.

        For demands keyed by a process node this is the demand id itself. For demands keyed by a
        product node (explicit process/product paradigm) we look up the process via the production
        exchange targeting that product.
        """
        node = bd.get_activity(key) if not hasattr(key, "id") else key
        if node.get("type") != "product":
            return node.id
        for exc in node.upstream(kinds=["production"]):
            return exc.output.id
        raise ValueError(
            f"Could not resolve product `{node}` to its producing process: no production "
            "exchange targets it."
        )

    def _resolve_demand_to_process_key(self, key):
        """Return the (database, code) key of the process producing ``key``."""
        node = bd.get_activity(key) if not hasattr(key, "id") else key
        if node.get("type") != "product":
            return node.key
        for exc in node.upstream(kinds=["production"]):
            return exc.output.key
        raise ValueError(
            f"Could not resolve product `{node}` to its producing process: no production "
            "exchange targets it."
        )

    ######################################
    # For creating human-friendly output #
    ######################################

    def create_labelled_technosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the technosphere matrix as a dataframe with comprehensible labels instead of ids.

        Returns
        -------
        pd.DataFrame
            technosphere matrix as a pandas.DataFrame with comprehensible labels instead
            of ids.
        """

        df = pd.DataFrame(self.lca.technosphere_matrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.lca.dicts.activity.reversed,
            columns=self.lca.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(  # from activity id to ((database, code), time)
            index=self.activity_time_mapping.reversed,
            columns=self.activity_time_mapping.reversed,
            inplace=True,
        )
        return df

    def create_labelled_biosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the biosphere matrix as a pandas.DataFrame with comprehensible labels instead of ids.

        Returns
        -------
        pd.DataFrame
            biosphere matrix as a pandas.DataFrame with comprehensible labels instead of
            ids.
        """

        df = pd.DataFrame(self.lca.biosphere_matrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.lca.dicts.biosphere.reversed,
            columns=self.lca.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(
            index=self.lca.remapping_dicts[
                "biosphere"
            ],  # from activity id to bioflow name
            columns=self.activity_time_mapping.reversed,  # id to ((database, code), time)
            inplace=True,
        )

        return df

    def create_labelled_dynamic_biosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the dynamic biosphere matrix as a dataframe with comprehensible labels instead of
        ids.

        Returns
        -------
        pd.DataFrame
            dynamic biosphere matrix as a pandas.DataFrame with comprehensible labels
            instead of ids.
        """
        df = pd.DataFrame(self.dynamic_biosphere_matrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.biosphere_time_mapping.reversed,
            columns=self.lca.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(  # from activity id to ((database, code), time)
            columns=self.activity_time_mapping.reversed,
            inplace=True,
        )

        df = df.loc[(df != 0).any(axis=1)]  # For readablity, remove all-zero rows

        return df

    def get_activity_name_from_time_mapped_id(self, time_mapped_id: int) -> str:
        """
        Get the activity name for a time-mapped activity ID.
        Uses the pre-built code-to-name cache for efficient lookups.

        Parameters
        ----------
        time_mapped_id : int
            The time-mapped activity ID from activity_time_mapping

        Returns
        -------
        str
            The name of the activity
        """
        # Extract the code from the activity_time_mapping
        # Structure: time_mapped_id -> (('database', 'code'), time)
        ((_, code), _) = self.activity_time_mapping.reversed[time_mapped_id]

        # Use the pre-built cache for O(1) lookup instead of database query
        return self._activity_code_to_name_cache.get(code, code)

    def create_labelled_dynamic_inventory_dataframe(self) -> pd.DataFrame:
        """
        Returns the dynamic_inventory_df with comprehensible labels for flows and activities instead
        of ids.

        Returns
        -------
        pd.DataFrame
            dynamic inventory matrix as a pandas.DataFrame with comprehensible labels
            instead of ids.
        """

        if not hasattr(self, "dynamic_inventory_df"):
            raise AttributeError(
                "Dynamic inventory not yet calculated. Call \
                    TimexLCA.lci(build_dynamic_biosphere=True) first."
            )

        df = self.dynamic_inventory_df.copy()
        df["flow"] = df["flow"].apply(lambda x: bd.get_node(id=x)["name"])

        # Build activity name cache efficiently using pre-built code-to-name cache
        activity_name_cache = {
            activity: self.get_activity_name_from_time_mapped_id(activity)
            for activity in df["activity"].unique()
        }

        df["activity"] = df["activity"].map(activity_name_cache)

        return df

    def plot_dynamic_inventory(self, bio_flows, cumulative=False) -> None:
        """
        Simple plot of dynamic inventory of a biosphere flow over time, with optional cumulative
        plotting.

        Parameters
        ----------
        bio_flows : list of int
            database ids of the biosphere flows to plot.
        cumulative : bool
            if True, plot cumulative amounts over time

        Returns
        -------
        None
            shows a plot
        """
        PlotDynamicInventoryInputs(bio_flows=bio_flows, cumulative=cumulative)

        plt.figure(figsize=(14, 6))

        filtered_df = self.dynamic_inventory_df[
            self.dynamic_inventory_df["flow"].isin(bio_flows)
        ]
        aggregated_df = filtered_df.groupby("date").sum()["amount"].reset_index()

        if cumulative:
            aggregated_df["amount"] = np.cumsum(aggregated_df["amount"])

        plt.plot(
            aggregated_df["date"], aggregated_df["amount"], marker="o", linestyle="none"
        )

        plt.ylim(bottom=0)
        plt.xlabel("time")
        plt.ylabel("amount [kg]")
        plt.grid(True)
        plt.tight_layout()  # Adjust layout to make room for the rotated date labels
        plt.show()

    def plot_dynamic_characterized_inventory(
        self,
        cumsum: bool = False,
        sum_emissions_within_activity: bool = False,
        sum_activities: bool = False,
    ) -> None:
        """
        Plot the characterized inventory of the dynamic LCI in a very simple plot.
        Legend and title are selected automatically based on the chosen metric.

        Parameters
        ----------
        cumsum : bool
            if True, plot cumulative amounts over time
        sum_emissions_within_activity : bool
            if True, sum emissions within each activity over time
        sum_activities : bool
            if True, sum emissions over all activities over time

        Returns
        -------
        None
            shows a plot
        """

        if not hasattr(self, "characterized_inventory"):
            raise AttributeError(
                "Characterized inventory not yet calculated. Call TimexLCA.dynamic_lcia() first."
            )

        metric_ylabels = {
            "radiative_forcing": "radiative forcing [W/m²]",
            "GWP": f"GWP{self.current_time_horizon} [kg CO₂-eq]",
            "pGWP": f"pGWP{self.current_time_horizon} [kg CO₂-eq]",
            "pGTP": f"pGTP{self.current_time_horizon} [kg CO₂-eq]",
            "prospective_radiative_forcing": "prospective radiative forcing [W/m²]",
        }

        # Fetch the inventory to use in plotting, modify based on flags
        plot_data = self.characterized_inventory.copy()

        if cumsum:
            plot_data["amount_sum"] = plot_data["amount"].cumsum()
            amount = "amount_sum"
        else:
            amount = "amount"

        if sum_emissions_within_activity:
            plot_data = plot_data.groupby(["date", "activity"]).sum().reset_index()
            plot_data["amount_sum"] = plot_data["amount"].cumsum()

        if sum_activities:
            plot_data = plot_data.groupby("date").sum().reset_index()
            plot_data["amount_sum"] = plot_data["amount"].cumsum()
            plot_data["activity_label"] = "All activities"

        else:  # plotting activities separate
            # Build activity name cache efficiently using pre-built code-to-name cache
            activity_name_cache = {
                activity: self.get_activity_name_from_time_mapped_id(activity)
                for activity in plot_data["activity"].unique()
            }

            plot_data["activity_label"] = plot_data["activity"].map(activity_name_cache)

        # Plotting
        plt.figure(figsize=(14, 6))
        axes = sb.scatterplot(x="date", y=amount, hue="activity_label", data=plot_data)

        # Determine y-axis limit flexibly
        if plot_data[amount].min() < 0:
            ymin = plot_data[amount].min() * 1.1
        else:
            ymin = 0

        axes.set_axisbelow(True)
        axes.set_ylim(bottom=ymin)
        axes.set_ylabel(metric_ylabels[self.current_metric])
        axes.set_xlabel("time")

        handles, labels = axes.get_legend_handles_labels()
        axes.legend(handles[::-1], labels[::-1])
        plt.grid(True)
        plt.show()
