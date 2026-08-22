"""Errors raised by `bw_timex`."""

from __future__ import annotations


class UnmappedDatabaseError(ValueError):
    """A database reached by the graph traversal is missing from the mapping.

    `bw_timex` places every traversed process in time via the database it
    lives in, so each of them must either represent a point in time or be
    marked as `"dynamic"`. Databases holding the functional unit are treated
    as dynamic automatically; every other database has to say what it
    represents, through its `representative_time` metadata or through
    `database_dates`.
    """
