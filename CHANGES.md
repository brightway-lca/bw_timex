# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
* Added more clarifying docstrings, created a "Getting Started" section in the docs as well as a [`getting_started.ipynb`](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/getting_started.ipynb). Also overhauled existing example notebooks.
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
