# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2024-09-18
* Added optional `starting_datetime` argument to `TimexLCA.build_timeline` explicitly. Before, it was buried in *args, which were passed to the underlying graph traversal (https://github.com/brightway-lca/bw_timex/pull/93)
* Allow multiple calls of `build_timeline` using the same `TimexLCA` object, e.g., using different `starting_datetime` (https://github.com/brightway-lca/bw_timex/pull/94)
* Fixed unintuitive rounding down of timestamps in dynamic characterization. 2024-12-31 would have been rounded to 2024, whereas 2025 makes more sense here. Now we round to the nearest year (https://github.com/brightway-lca/bw_timex/commit/21fa55bbcafee196447840c6518b5fee49fb6660)

## [0.2.1] - 2024-09-16
* Added labels and units to the yaxis in `TimexLCA.plot_dynamic_characterized_inventory()`
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
* Handled emissions occuring outside of fixed time horizon in dynamic characterization [#46](https://github.com/brightway-lca/bw_timex/issues/46)
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
* Inital version with core functionalities
