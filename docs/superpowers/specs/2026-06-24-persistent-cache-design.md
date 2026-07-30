# Persistent Disk Cache for Solve Results (P3) — Design

**Date:** 2026-06-24
**Status:** Design — approved, pending spec review
**Branch:** `feat/persistent-cache` (off `feat/adjoint-traversal-scoring`)
**Depends on:** P1 (adjoint static-score intensities — `bw_timex/adjoint_scoring.py`)
**Scope:** Second cycle of the trails-learnings effort. Persist the two
*expensive, stable, serializable* solve-result caches across Python sessions.

## Background

`bw_timex` currently caches only in-session, via module-level dicts in
`bw_timex/_lci_cache.py`, keyed with bw2data `modified` tokens so background
edits invalidate stale entries. Nothing survives a process restart, so every
new session repays the background linear solves. `trails` persists comparable
score/LCI data to a `platformdirs` user cache and reuses it across runs.

Not every cache benefits from persistence — persistence pays only when an item
is expensive to recompute, stable across sessions, and cleanly serializable;
for cheap or per-run items the serialize + I/O + deserialize round-trip is
often slower than recomputing and adds correctness/versioning risk. Triage of
the existing caches:

| Cache | Persist? | Rationale |
|---|---|---|
| **λ adjoint intensities** (P1) | yes | one linear solve to compute; tiny 1-D array; modified-token stable |
| **`BACKGROUND_UNIT_LCI_CACHE`** | yes | the expensive background `redo_lci` solves; stable per `(db, code, modified)`; numpy-serializable triplets. Largest cross-session win |
| `BIOSPHERE_EXCHANGES_CACHE` | no | cheap DB reads; benefit too small to justify the risk |
| `LCI_SOLVE_CACHE` | no | keyed per scenario (demand + timeline) → low cross-session hit rate; inventory arrays are large |
| `NODES_CACHE` | no | live bw2data `Activity` proxy objects; fragile to pickle, cheap to rebuild |

This spec persists exactly the two `yes` rows.

## Goal

When adjoint scoring / background LCI runs, transparently reuse λ and
background unit LCI results from disk across sessions, keyed and invalidated by
bw2data `modified` tokens. On by default, with an off switch and a clear
helper. Results must be identical to the no-cache path.

Non-goals: persisting the other three caches; content-hash keying; premise
Frictionless ingestion (next spec).

## Existing shapes (verified)

- `BACKGROUND_UNIT_LCI_CACHE` values are structure-independent triplets
  `(bioflow_ids: np.int64[], activity_ids: np.int64[], values: np.float64[])`
  (`dynamic_biosphere_builder._inventory_to_triplets`). The dict only ever
  holds stable keys of the form `("db_code", db, code, modified)`; non-stable
  identities route to a separate per-object `_instance_unit_lci_cache`, so a
  persistence wrapper on this dict never sees an unpersistable key.
- λ is `AdjointCachingSolver.lambda_vector` (1-D `np.float64`), computed in
  `set_score_row` from the `base_lca` matrices. The solver has no bw2data
  identity context, so its persistent key is constructed one level up
  (`TimexLCA`), which knows the method and the involved databases.

## Architecture

New module `bw_timex/persistent_cache.py`: disk I/O, keying, and atomic writes
only — no domain logic. Two consumers wire to it.

Cache root (via `platformdirs.user_cache_path`):
`<user_cache>/bw_timex/v1/{background_unit_lci,adjoint_intensities}/`.
The `v1` path segment is the format version; bumping it invalidates all prior
entries instantly. A module function `cache_root() -> Path` resolves it and is
overridable for tests via an environment variable
`BW_TIMEX_CACHE_DIR` (when set, used verbatim as the root).

## Components

### 1. `PersistentDict(collections.abc.MutableMapping)`

Wraps an in-memory `dict` plus a disk directory. Semantics:

- `__contains__(key)`: true if in memory, else true if the key's file exists.
- `__getitem__(key)`: return from memory; on memory miss, load the npz from
  disk, populate memory, return; on disk miss raise `KeyError`; on any
  load/parse error treat as miss (`KeyError`) and best-effort delete the bad
  file.
- `__setitem__(key, value)`: store in memory and write-through to disk via an
  atomic temp-file + `os.replace`.
- `__delitem__`, `__iter__`, `__len__`: memory-backed (disk iteration is not
  required by consumers; documented).

Key → filename: a stable hash (blake2b hex) of the key tuple's repr. Value
serialization: the three numpy arrays via `np.savez`. This is a drop-in for
`BACKGROUND_UNIT_LCI_CACHE`, so the builder's existing
`if cache_key not in cache: … cache[cache_key] = …` logic is unchanged.

### 2. λ persistence hook

`AdjointScoringGraphTraversal.__init__` gains an optional `lambda_cache`
parameter exposing `load(key) -> np.ndarray | None` and `save(key, array)`.
In the solver's `set_score_row`:

- if a cache + key are present and `load(key)` returns an array, assign it to
  `lambda_vector` and **skip the adjoint solve** (so `solve_count` stays 0);
- otherwise solve as today and `save(key, lambda_vector)`.

`TimexLCA` constructs the key
`("lambda", method_id, tuple(sorted((db, modified) for db in base_lca dbs)))`
and supplies a small `LambdaDiskCache` (backed by `persistent_cache`) plus the
key down through `build_timeline → TimelineBuilder → EdgeExtractor →
AdjointScoringGraphTraversal`. The solver stays db-agnostic; only the
controller knows identities.

### 3. Wiring in `TimexLCA`

- New constructor parameter `persistent_cache: bool = True`. When `True` (and
  `use_global_lci_cache=True`), `_background_unit_lci_cache` is a
  `PersistentDict` over the `background_unit_lci` dir backed by the existing
  module dict; when `False`, behavior is exactly as today (memory-only).
- The λ key + `LambdaDiskCache` are threaded only when both
  `persistent_cache=True` and `adjoint_scoring=True` at `build_timeline`.

### 4. Clearing

`clear_persistent_cache() -> None` removes the on-disk `bw_timex/v1` tree.
`clear_background_lci_cache()` (existing) is extended to also call it, so one
call clears both memory and disk; `clear_persistent_cache` is additionally
exported for disk-only clears. Both are exported from `bw_timex/__init__.py`.

## Keying / invalidation

modified-token + method. Background entries already embed `modified` in the
key. The λ key folds every involved database's `modified` token plus the method
id. Editing a database via bw2data bumps `modified`, so prior entries simply
never match again (they are left on disk, unused, until `clear_*`). This
mirrors the accepted limitation of the in-session caches: edits that bypass
bw2data (raw SQL) do not bump `modified` and are not detected.

## Error handling

The cache is never load-bearing. Any corrupt, unreadable, or wrong-version
file is treated as a miss: recompute, then overwrite. Writes are atomic
(temp file in the same dir + `os.replace`) so concurrent processes never read a
torn file. No cache operation raises out of the cache layer; failures degrade
to recompute. A failed disk write is swallowed (logged at debug) — the
in-memory value still stands for the session.

## Dependencies

Promote `platformdirs` from a transitive to a direct dependency in
`pyproject.toml` (already resolved in `uv.lock`).

## Testing

All tests set `BW_TIMEX_CACHE_DIR` to a pytest `tmp_path` so the real user
cache is never touched.

- **`PersistentDict`** (unit): round-trips triplet values; a second instance
  over the same dir reads what the first wrote (cross-session proxy); missing
  key raises `KeyError`; a deliberately corrupted file is treated as a miss and
  removed, not raised; `__setitem__` leaves no `.tmp` partial behind.
- **λ hook** (unit): with a populated `LambdaDiskCache`, `set_score_row` skips
  the solve (`solve_count == 0`) and uses the stored vector; with an empty
  cache it solves once (`solve_count == 1`) and writes; a changed
  `modified`-token key misses and recomputes.
- **End-to-end** (fixtures, reuse `temporal_grouping_db_monthly` and
  `background_td_deep_chain_db`): score + timeline identical between a cold-cache
  run and a warm-cache run (exact); `persistent_cache=False` performs zero disk
  I/O (assert the cache dir stays empty); `clear_persistent_cache()` empties the
  dir; a second `TimexLCA` in the same session/dir reuses background unit LCI
  from disk.
- Full suite passes; no new warnings.

## Deliverables

1. `bw_timex/persistent_cache.py`: `cache_root()`, `PersistentDict`,
   `LambdaDiskCache`, `clear_persistent_cache()`, atomic-write + npz helpers.
2. λ `lambda_cache` hook in `AdjointScoringGraphTraversal` / `AdjointCachingSolver`.
3. `TimexLCA` wiring: `persistent_cache` param, λ key construction, background
   dict swap.
4. `clear_background_lci_cache()` extension + exports.
5. `platformdirs` as a direct dependency.
6. Tests above.

## Follow-on (not this cycle)

premise Frictionless datapackage ingestion adapter (next spec).
