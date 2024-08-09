## v0.1.9 (2024-08-09)
* Allow absolute temporal distributions (https://github.com/brightway-lca/bw_timex/pull/81)

## v0.1.8 (2024-07-17)
* Moved dynamic characterization functionality completely to [dynamic_characterization](https://github.com/brightway-lca/dynamic_characterization). In the course of this, it was dynamic characterization was updated and is much faster now. See also https://github.com/brightway-lca/dynamic_characterization/pull/3

## v0.1.7 (2024-07-11)
* Fixed some dependencies

## v0.1.6 (2024-07-11)
* Performance improvements
* Added option to calculate the dynamic LCI directly from the timeline without expanding the technosphere matrix

## v0.1.5 (2024-06-28)
* Refactored dynamic characterization to separate package [dynamic_characterization](https://github.com/brightway-lca/dynamic_characterization)

## v0.1.4 (2024-06-15)
* Handled emissions occuring outside of fixed time horizon in dynamic characterization [#46](https://github.com/brightway-lca/bw_timex/issues/46)
* Fix substitution exchanges [#53](https://github.com/brightway-lca/bw_timex/issues/53)
* Fix non-unitary production exchanges [#55](https://github.com/brightway-lca/bw_timex/issues/55)

## v0.1.3 (2024-06-07)
* Renamed repo and package to bw_timex
* Fixed error in dynamic characterization if IDs were stored as flow identifiers in methods

## v0.1.2 (2024-05-27)
* Update to match Bugfix in [bw_temporalis v1.1](https://github.com/brightway-lca/bw_temporalis/commit/5ec8c850f325f6b5aa88cd2357bb56401304ddda): static_activity_indices are database IDs instead of matrix IDs

## v0.1.1 (2024-05-05)
* Improved user-friendliness for SETAC

## v0.1.0 (2024-04-29)
* Inital version with core functionalities
