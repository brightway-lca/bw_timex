# Timeline Builder

Builds a process timeline from the temporal distributions of exchanges. It relies on the `EdgeExtractor` to do a priority-first graph traversal that extracts a timeline of exchanges with temporal information, then groups identical edges within a chosen temporal resolution (e.g. year, month, day, hour) and sums their amounts.

::: bw_timex.timeline_builder
