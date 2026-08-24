"""Module-level caches for background LCI results.

These caches persist across :class:`~bw_timex.TimexLCA` objects within a single
Python session / ``bw_timex`` import (e.g. one Jupyter notebook kernel).

Only *stable* background process identities are stored here: keys of the form
``("db_code", project, db, code, modified)``, where ``modified`` is the
background database's ``modified`` token. Editing a background database bumps
that token, so stale entries are automatically missed instead of silently
reused.

Unstable keys (the time-mapped ``activity_id`` and the per-run
``temporalized`` database) are deliberately kept per-object by the
:class:`~bw_timex.background_solver.BackgroundSolver` and never reach these
module-level caches.
"""

# Cached supply columns and biosphere aggregates for background unit LCIs.
# Payloads are pairs of 1-D numpy arrays `(node_ids, values)` holding only the
# nonzero entries, in stable node-id space rather than any one lca_obj's
# row/column index space (see `bw_timex.background_solver`). Caching a dense
# `B @ diag(x)` matrix per background activity is what made large runs run out
# of memory; supply columns (0.35 MB) and aggregates are orders of magnitude
# smaller than the inventory matrices (5.5 MB) they replace.
BACKGROUND_SUPPLY_CACHE = {}
BACKGROUND_AGGREGATE_CACHE = {}

# Cached biosphere exchanges per (project, db, code, modified). Keyed by the
# source database's `modified` token so foreground/background edits
# invalidate stale entries automatically.
BIOSPHERE_EXCHANGES_CACHE = {}

# Cached LCA solve results: maps a scenario fingerprint to
# ``(supply_array, inventory)`` so identical scenarios re-run in the same
# session can skip the ~1.4 s `spsolve` for the functional unit.
LCI_SOLVE_CACHE = {}

# Cached node proxies per database. Keyed by ``("nodes", project, db,
# modified)`` so each ``TimexLCA`` reuses the ``Activity`` proxies built from
# the database rows instead of re-querying. Editing a database bumps its
# ``modified`` token, invalidating stale entries automatically.
NODES_CACHE = {}


def clear_background_lci_cache() -> None:
    """Clear all module-level bw_timex caches (supply/aggregate, biosphere
    exchanges, solve, nodes)."""
    BACKGROUND_SUPPLY_CACHE.clear()
    BACKGROUND_AGGREGATE_CACHE.clear()
    BIOSPHERE_EXCHANGES_CACHE.clear()
    LCI_SOLVE_CACHE.clear()
    NODES_CACHE.clear()
