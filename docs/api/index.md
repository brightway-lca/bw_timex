---
icon: lucide/code-2
---

# API Reference

This section contains the API documentation generated from the `bw_timex` source code docstrings.

The main user-facing class is [`TimexLCA`](timex_lca.md). It orchestrates the other components, which most users never need to instantiate directly:

- [`timex_lca`](timex_lca.md) — the `TimexLCA` class: the entry point for a time-explicit LCA.
- [`timeline_builder`](timeline_builder.md) — traverses the graph and builds the process timeline.
- [`matrix_modifier`](matrix_modifier.md) — expands the technosphere and biosphere matrices with time-explicit rows and columns.
- [`dynamic_biosphere_builder`](dynamic_biosphere_builder.md) — builds the dynamic biosphere matrix carrying emission timing.
- [`edge_extractor`](edge_extractor.md) — extracts and convolves temporal distributions during graph traversal.
- [`helper_classes`](helper_classes.md) — supporting data structures used across the package.
- [`utils`](utils.md) — utility functions.
