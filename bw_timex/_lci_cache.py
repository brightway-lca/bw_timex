"""Module-level cache for background unit LCI matrices.

This cache persists across :class:`~bw_timex.TimexLCA` objects within a single
Python session / ``bw_timex`` import (e.g. one Jupyter notebook kernel).

Only *stable* background process identities are stored here: keys of the form
``("db_code", db, code, modified)``, where ``modified`` is the background
database's ``modified`` token. Editing a background database bumps that token,
so stale entries are automatically missed instead of silently reused.

Unstable keys (the time-mapped ``activity_id`` and the per-run
``temporalized`` database) are deliberately kept per-object by the
:class:`~bw_timex.dynamic_biosphere_builder.DynamicBiosphereBuilder` and never
reach this module-level cache.
"""

BACKGROUND_UNIT_LCI_CACHE = {}

# Cached biosphere exchanges per (db, code, modified). Keyed by the source
# database's `modified` token so foreground/background edits invalidate
# stale entries automatically.
BIOSPHERE_EXCHANGES_CACHE = {}


def clear_background_lci_cache() -> None:
    """Clear all module-level bw_timex caches (unit LCI + biosphere exchanges)."""
    BACKGROUND_UNIT_LCI_CACHE.clear()
    BIOSPHERE_EXCHANGES_CACHE.clear()
