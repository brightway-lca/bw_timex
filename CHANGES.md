# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.1] - 2026-08-14
* Fixed `ShapeMismatch` in `lci()` for processes with more than one biosphere exchange, by sizing the biosphere `flip_array` to the number of matrix entries (only raised with `bw_processing` >= 1.5; no numeric results change) ([#213](https://github.com/brightway-lca/bw_timex/pull/213))
* Fixed the conda package declaring dependencies that had drifted from `pyproject.toml`, by generating the recipe's `run:` requirements from it ([#211](https://github.com/brightway-lca/bw_timex/pull/211))

## [1.2.0] - 2026-08-14
* Migrated the documentation from Sphinx to [Zensical](https://zensical.org), with the example pages generated from the notebooks ([#202](https://github.com/brightway-lca/bw_timex/pull/202))
* Added name-based exchange lookup to `get_exchange`, `add_temporal_distribution_to_exchange` and `add_temporal_evolution_to_exchange`, via `input_name`/`output_name` (optionally narrowed down with `input_location`/`output_location` and `input_product`/`output_product`), which avoids having to know the machine-generated codes of e.g. ecoinvent nodes ([#202](https://github.com/brightway-lca/bw_timex/pull/202))
* Added a flat top-level namespace: `TemporalDistribution`, `easy_timedelta_distribution` and `easy_datetime_distribution` are re-exported from `bw_temporalis`, and the user-facing helpers from `bw_timex.utils` (`add_temporal_distribution_to_exchange`, `add_temporal_evolution_to_exchange`, `add_flows_to_characterization_functions`, `get_exchange`, `get_temporal_evolution_factor`, `interactive_td_widget`, `plot_characterized_inventory_as_waterfall`) are now importable directly from `bw_timex` ([#206](https://github.com/brightway-lca/bw_timex/pull/206))
* Fixed `lci()` raising `MultipleResults` when a temporalized process' code also exists in another database of the project, by resolving producers by node id instead of by code ([#203](https://github.com/brightway-lca/bw_timex/pull/203))
* Fixed supply scaling in `lci(expand_technosphere=False)` for processes with a production amount other than 1, whose supplies were mis-scaled and, for negative production amounts (waste treatment), sign-flipped ([#200](https://github.com/brightway-lca/bw_timex/pull/200))
* Fixed `temporal_market_lcis` being corrupted when several timeline rows share a time-mapped temporal market ([#200](https://github.com/brightway-lca/bw_timex/pull/200))
* Added the pending-solve planning and technosphere factorization to `lci(expand_technosphere=False)`, which previously only ran for the expanded path ([#200](https://github.com/brightway-lca/bw_timex/pull/200))
* Improved `TimexLCA` setup speed by making it independent of the background database size, via lazy node proxies and a base LCA restricted to the demand-relevant databases ([#204](https://github.com/brightway-lca/bw_timex/pull/204))
* Added support for several background databases sharing the same point in time in `database_dates`, by resolving temporal market shares per producer instead of per date ([#205](https://github.com/brightway-lca/bw_timex/pull/205))
* Fixed `dynamic_lcia()` input validation to accept the prospective metrics supported by `dynamic_characterization`: `pGWP`, `pGTP`, and `prospective_radiative_forcing` ([855ba84](https://github.com/brightway-lca/bw_timex/commit/855ba845a00e08e3a3d10459c7d715b4602d71f9))
* Fixed `MultipleTechnosphereExchanges` when several exchanges connect the same two nodes, by merging them into one edge ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Fixed processes consuming their own product losing that loop in the technosphere matrix, and `graph_traversal="bfs"` counting it twice ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Fixed `KeyError` in `build_timeline()` for node cohorts that receive no supply ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Fixed `traverse_background` losing everything upstream of a `cutoff` or `max_calc` truncation ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Fixed `ValueError: Empty array` for all-zero cohorts in the background descent ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Added `static_lcia()` for inventories built from the timeline (`lci(expand_technosphere=False)`) ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Added `lci(keep_activity_dimension=False)`, which accumulates the dynamic inventory per biosphere flow and time instead of per activity, trading contribution analysis for much lower memory use ([#207](https://github.com/brightway-lca/bw_timex/pull/207))
* Reduced memory and runtime of `lci(expand_technosphere=False)` on large time-explicit systems ([#207](https://github.com/brightway-lca/bw_timex/pull/207))

## [1.1.2]
* Fixed the option to calculate the lci from the timeline. This option is called with lci(expand_technosphere=False) which speeds up the calculation significantly for for large systems, but does not allow for detailed contribution analysis of background processes. https://github.com/brightway-lca/bw_timex/commit/12853dbd799764f6d2d2fa2335d6f6f19a97abed


## [1.1.1] - 2026-06-23
* Added module-level caching of database node proxies so repeated `TimexLCA` objects in a session reuse them instead of re-querying ([#196](https://github.com/brightway-lca/bw_timex/pull/196))
* Fixed `traverse_background` crashing on zero-amount background exchanges ([#196](https://github.com/brightway-lca/bw_timex/pull/196))
* Fixed `max_calc` not bounding the background traversal ([#196](https://github.com/brightway-lca/bw_timex/pull/196))

## [1.1.0] - 2026-06-22
* Added support for explicit `product` and `process` nodes instead of only `processwithreferenceproduct` ([#192](https://github.com/brightway-lca/bw_timex/pull/192))([#193](https://github.com/brightway-lca/bw_timex/pull/193))
* Added `traverse_background` option to include temporal distributions in the background system ([#195](https://github.com/brightway-lca/bw_timex/pull/195))

## [1.0.3] - 2026-06-11
* Fixed foreground evolution for breadth first graph traversal ([#191](https://github.com/brightway-lca/bw_timex/pull/191))

## [1.0.2] - 2026-05-22
* Added extensive caching to speed up repeated calculations ([#187](https://github.com/brightway-lca/bw_timex/pull/187))

## [1.0.1] - 2026-05-19
* Fixed absolute temporal distributions ([#189](https://github.com/brightway-lca/bw_timex/pull/189))

## [1.0.0] - 2026-03-26
* Added breadth first graph traversal option ([#159](https://github.com/brightway-lca/bw_timex/pull/159))
* Added support for foreground evolution ([#164](https://github.com/brightway-lca/bw_timex/pull/164))
* Added validation of user inputs via `pydantic` ([#181](https://github.com/brightway-lca/bw_timex/pull/181))

## [0.3.4] - 2026-01-05
* Fixed compatibility with latest Brightway versions

## [0.3.3] - 2025-10-12
* Updated dependency on dynamic_characterization to >=1.2.0

## [0.3.2] - 2025-10-09
* Added `bw_timex.utils.interactive_td_widget` for drafting and visualizing temporal distributions in jupyter notebooks
* Improved caching for activity name lookup
* Improved logging

## [0.3.1] - 2025-03-31
* Fixed an issue with non-unitary production exchanges

## [0.3.0] - 2025-02-07
* Renamed various variables. The main user-facing API change is `database_date_dict` -> `database_dates`. Others are mainly internal, see https://github.com/brightway-lca/bw_timex/commit/991943cd0ea9c0185baace3b84c75abd46b4bd59 and https://github.com/brightway-lca/bw_timex/commit/554a67cc7796264be888840c1c9431f64952fd66.
* Added a function to disaggregate the background LCI. This means that the aggregated biosphere flows of the upstream supply chains of temporal markets are distributed back to the original producers from the background database.
* Various speed improvements

## [0.2.6] - 2024-09-25
* Fixed rounding in dynamic_lcia to avoid duplicate entries in dynamic inventory

## [0.2.5] - 2024-09-25
* Added rounding to 3 decimal places for interpolation weights

## [0.2.4] - 2024-09-24
* Added support for passing Node objects to `bw_timex.utils.get_exchange` and `bw_timex.utils.add_temporal_distribution_to_exchange`.

## [0.2.3] - 2024-09-22
* Modified the date rounding behavior: Instead of always rounding off the dates in the timeline (using the resolution specified in temporal_grouping), we now round to the nearest year/month/day/hour (depending on temporal_grouping).
* Fixed interface to dynamic_characterization (see https://github.com/brightway-lca/dynamic_characterization/releases/tag/v1.0.0) and pinned version to >=1.0.0.

## [0.2.2] - 2024-09-18
* Added optional `starting_datetime` argument to `TimexLCA.build_timeline` explicitly. Before, it was buried in *args, which were passed to the underlying graph traversal (https://github.com/brightway-lca/bw_timex/pull/93)
* Allow multiple calls of `build_timeline` using the same `TimexLCA` object, e.g., using different `starting_datetime` (https://github.com/brightway-lca/bw_timex/pull/94)
* Fixed unintuitive rounding down of timestamps in dynamic characterization. 2024-12-31 would have been rounded to 2024, whereas 2025 makes more sense here. Now we round to the nearest year (https://github.com/brightway-lca/bw_timex/commit/21fa55bbcafee196447840c6518b5fee49fb6660)

## [0.2.1] - 2024-09-16
* Added labels and units to the y-axis in `TimexLCA.plot_dynamic_characterized_inventory()`
* Fixed functions for creating labelled matrix representations

## [0.2.0] - 2024-09-13
* Added utility function [`utils.add_temporal_distribution_to_exchange()`](https://github.com/brightway-lca/bw_timex/blob/a85349bdc43d98be559a7ce17d0b686098decec6/bw_timex/utils.py#L341) for easier temporalization of existing models
* Added more clarifying docstrings, created a "Getting Started" section in the docs as well as a [`getting_started.ipynb`](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/tutorials/1_getting_started.ipynb). Also overhauled existing example notebooks.
* Changed naming of the different score attributes to be more clear and [turned them into a @property:](https://github.com/brightway-lca/bw_timex/blob/a85349bdc43d98be559a7ce17d0b686098decec6/bw_timex/timex_lca.py#L437)
    * `TimexLCA.base_score` := `TimexLCA.static_lca.score` (no time-explicit information)
    * `TimexLCA.static_score` := `TimexLCA.lca.score` (time-explicit LCI w/ static characterization)
    * `TimexLCA.dynamic_score` := `TimexLCA.characterized_inventory["amount"].sum()` (time-explicit LCI w/ dynamic characterization, summed overall score)
* Fixed amounts for negative production amounts (https://github.com/brightway-lca/bw_timex/pull/83)

## [0.1.9] - 2024-08-09
* Allow absolute temporal distributions (https://github.com/brightway-lca/bw_timex/pull/81)

## [0.1.8] - 2024-07-17
* Moved dynamic characterization functionality completely to [dynamic_characterization](https://github.com/brightway-lca/dynamic_characterization). In the course of this, it was dynamic characterization was updated and is much faster now. See also https://github.com/brightway-lca/dynamic_characterization/pull/3

## [0.1.7] - 2024-07-11
* Fixed some dependencies

## [0.1.6] - 2024-07-11
* Performance improvements
* Added option to calculate the dynamic LCI directly from the timeline without expanding the technosphere matrix

## [0.1.5] - 2024-06-28
* Refactored dynamic characterization to separate package [dynamic_characterization](https://github.com/brightway-lca/dynamic_characterization)

## [0.1.4] - 2024-06-15
* Handled emissions occurring outside of fixed time horizon in dynamic characterization [#46](https://github.com/brightway-lca/bw_timex/issues/46)
* Fix substitution exchanges [#53](https://github.com/brightway-lca/bw_timex/issues/53)
* Fix non-unitary production exchanges [#55](https://github.com/brightway-lca/bw_timex/issues/55)

## [0.1.3] - 2024-06-07
* Renamed repo and package to bw_timex
* Fixed error in dynamic characterization if IDs were stored as flow identifiers in methods

## [0.1.2] - 2024-05-27
* Update to match Bugfix in [bw_temporalis v1.1](https://github.com/brightway-lca/bw_temporalis/commit/5ec8c850f325f6b5aa88cd2357bb56401304ddda): static_activity_indices are database IDs instead of matrix IDs

## [0.1.1] - 2024-05-05
* Improved user-friendliness for SETAC

## [0.1.0] - 2024-04-29
* Initial version with core functionalities
