# Persistent Disk Cache (P3) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the two expensive, stable, serializable solve-result caches — adjoint λ intensities and background unit LCI — to a platformdirs disk cache so they survive across Python sessions, keyed/invalidated by bw2data `modified` tokens, on by default with an off switch and a clear helper.

**Architecture:** A new `bw_timex/persistent_cache.py` owns all disk I/O, keying, and atomic writes. `PersistentDict` (a `MutableMapping` wrapping the existing in-memory `BACKGROUND_UNIT_LCI_CACHE` plus a disk dir) transparently persists background unit-LCI triplets with zero change to the builder's cache logic. `LambdaDiskCache` persists the 1-D λ vector; `AdjointCachingSolver.set_score_row` consults it (reached via attributes stashed on `base_lca`, because `bw_temporalis` instantiates the traversal class itself). `TimexLCA` gains a `persistent_cache` flag, builds the λ key from method + involved-database `modified` tokens, and swaps the background dict.

**Tech Stack:** Python 3.13, numpy (`np.savez`/`np.load`), platformdirs, scipy.sparse, bw2data, pytest, uv.

## Global Constraints

- Package manager: `uv` for all Python (`uv run pytest ...`). Never pip/conda.
- COMMITS ENABLED on branch `feat/persistent-cache` (off `feat/adjoint-traversal-scoring`). Commit per task. End every commit message body with exactly these two trailers:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01NadULkEstbs67wxr2W8DtL`
- Cache is NEVER load-bearing: any corrupt/unreadable/wrong-version file is treated as a miss (recompute, overwrite); cache operations never raise out of the cache layer.
- Writes are atomic: write to a unique temp file in the same dir, then `os.replace`.
- All tests set env var `BW_TIMEX_CACHE_DIR` to a pytest `tmp_path` so the real user cache is never touched.
- Keying is modified-token + method (NOT content hashing). Background key form is exactly `("db_code", project, db, code, modified)`. λ key form is exactly `("lambda", project, method, tuple(sorted((db, modified) for db in databases)))`.
- On by default, gated by the existing `use_global_lci_cache=True`; new `TimexLCA(..., persistent_cache=True)` is the off switch (`False` ⇒ behavior identical to today, zero disk I/O).
- Format-version path segment is `v1`; cache root is `<base>/bw_timex/v1/{background_unit_lci,adjoint_intensities}/`.
- Do not edit anything under `.venv/`.

---

## File Structure

- **Create `bw_timex/persistent_cache.py`** — disk cache primitives: `cache_root()`, `_atomic_write_bytes()`, `_key_to_filename()`, `PersistentDict`, `LambdaDiskCache`, `clear_persistent_cache()`. One responsibility: persistence. No bw_timex domain logic.
- **Modify `bw_timex/adjoint_scoring.py`** — `AdjointCachingSolver.set_score_row` consults a λ cache + key read from `self.lca` attributes (`_bw_timex_lambda_cache`, `_bw_timex_lambda_key`); on hit, use stored λ and skip the solve (`solve_count` stays 0); on miss, solve then save.
- **Modify `bw_timex/_lci_cache.py`** — extend `clear_background_lci_cache()` to also clear the disk cache.
- **Modify `bw_timex/__init__.py`** — export `clear_persistent_cache` (and re-export remains for `clear_background_lci_cache`).
- **Modify `bw_timex/timex_lca.py`** — `__init__` gains `persistent_cache: bool = True`; when enabled, wrap the background cache in `PersistentDict`; before the traversal in `build_timeline`, stash `_bw_timex_lambda_cache`/`_bw_timex_lambda_key` on `base_lca` (only when adjoint scoring is active); clean them up afterward.
- **Modify `pyproject.toml`** — promote `platformdirs` to a direct dependency.
- **Create `tests/test_persistent_cache.py`** — unit tests for the primitives + λ hook; end-to-end cache tests on fixtures.

Reference facts (verified in the current tree):
- `bw_timex/dynamic_biosphere_builder.py: get_background_lci_cache_key` returns `("db_code", bd.projects.current, db, code, modified)` for stable entries, `("temporalized", code)` / `("activity_id", act)` otherwise. `BACKGROUND_UNIT_LCI_CACHE` only ever receives `db_code` keys (others route to `_instance_unit_lci_cache`).
- Background value = triplets `(bioflow_ids: np.int64[], activity_ids: np.int64[], values: np.float64[])`.
- `bw_timex/timex_lca.py:206-207`: `self._background_unit_lci_cache = BACKGROUND_UNIT_LCI_CACHE if use_global_lci_cache else {}`.
- `AdjointCachingSolver(lca)` stores `self.lca`; `set_score_row` computes `lambda_vector` via `spsolve(A.T, score_row)` and sets `solve_count`.
- `AdjointScoringGraphTraversal` is passed as a CLASS to `bw_temporalis.TemporalisLCA`, which instantiates it via `.calculate()`; the `base_lca` passed in becomes `self.lca` on the traversal and solver — hence the attribute-stash seam.

---

### Task 1: `persistent_cache.py` core — `cache_root`, atomic write, `PersistentDict`, `clear_persistent_cache`

**Files:**
- Create: `bw_timex/persistent_cache.py`
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: numpy, platformdirs, stdlib (`os`, `io`, `uuid`, `hashlib`, `shutil`, `pathlib`).
- Produces:
  - `cache_root() -> pathlib.Path` (`<base>/bw_timex/v1`, base = `$BW_TIMEX_CACHE_DIR` if set else `platformdirs.user_cache_path("bw_timex", "bw_timex")`)
  - `clear_persistent_cache() -> None`
  - `class PersistentDict(collections.abc.MutableMapping)` with `__init__(self, memory: dict, disk_dir: pathlib.Path)`; persists values of the form `(np.ndarray, np.ndarray, np.ndarray)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_persistent_cache.py
import os
import numpy as np
import pytest


@pytest.fixture(autouse=True)
def _cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("BW_TIMEX_CACHE_DIR", str(tmp_path))
    # Re-import is unnecessary: cache_root() reads the env var at call time.
    yield tmp_path


def _triplet():
    return (
        np.array([0, 1], dtype=np.int64),
        np.array([2, 3], dtype=np.int64),
        np.array([1.5, -2.0], dtype=np.float64),
    )


def test_cache_root_uses_env_override(tmp_path):
    from bw_timex.persistent_cache import cache_root
    root = cache_root()
    assert str(root).startswith(str(tmp_path))
    assert root.name == "v1"


def test_persistent_dict_roundtrip_across_instances(tmp_path):
    from bw_timex.persistent_cache import PersistentDict
    disk = tmp_path / "bg"
    key = ("db_code", "proj", "background", "x", 123)

    d1 = PersistentDict(memory={}, disk_dir=disk)
    d1[key] = _triplet()

    # Fresh memory, same disk dir == cross-session reuse.
    d2 = PersistentDict(memory={}, disk_dir=disk)
    assert key in d2
    bio, act, val = d2[key]
    np.testing.assert_array_equal(bio, _triplet()[0])
    np.testing.assert_array_equal(act, _triplet()[1])
    np.testing.assert_allclose(val, _triplet()[2])


def test_persistent_dict_missing_key_raises(tmp_path):
    from bw_timex.persistent_cache import PersistentDict
    d = PersistentDict(memory={}, disk_dir=tmp_path / "bg")
    with pytest.raises(KeyError):
        _ = d[("db_code", "p", "db", "code", 1)]


def test_persistent_dict_corrupt_file_is_miss(tmp_path):
    from bw_timex.persistent_cache import PersistentDict
    disk = tmp_path / "bg"
    key = ("db_code", "p", "db", "code", 1)
    d = PersistentDict(memory={}, disk_dir=disk)
    d[key] = _triplet()
    # Corrupt every file on disk.
    for f in disk.iterdir():
        f.write_bytes(b"not a real npz")
    d2 = PersistentDict(memory={}, disk_dir=disk)
    with pytest.raises(KeyError):
        _ = d2[key]
    # Corrupt file was removed.
    assert not any(disk.iterdir())


def test_persistent_dict_no_tmp_leftovers(tmp_path):
    from bw_timex.persistent_cache import PersistentDict
    disk = tmp_path / "bg"
    d = PersistentDict(memory={}, disk_dir=disk)
    d[("db_code", "p", "db", "code", 1)] = _triplet()
    assert not any(p.name.endswith(".tmp") for p in disk.iterdir())


def test_clear_persistent_cache_removes_tree(tmp_path):
    from bw_timex.persistent_cache import PersistentDict, clear_persistent_cache, cache_root
    disk = cache_root() / "background_unit_lci"
    d = PersistentDict(memory={}, disk_dir=disk)
    d[("db_code", "p", "db", "code", 1)] = _triplet()
    assert cache_root().exists()
    clear_persistent_cache()
    assert not cache_root().exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'bw_timex.persistent_cache'`.

- [ ] **Step 3: Write the implementation**

```python
# bw_timex/persistent_cache.py
"""Persistent (cross-session) disk cache for expensive, stable solve results.

Two consumers use this module: the background unit-LCI triplet cache
(:class:`PersistentDict`) and the adjoint lambda vector cache
(:class:`LambdaDiskCache`). The cache is never load-bearing — any unreadable or
wrong-version entry is treated as a miss and recomputed. Keys are invalidated
by bw2data ``modified`` tokens embedded in the key, so this module performs no
content hashing of matrices.
"""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import uuid
from collections.abc import MutableMapping
from pathlib import Path

import numpy as np
import platformdirs

_VERSION = "v1"


def cache_root() -> Path:
    """Return the versioned cache root, honoring ``BW_TIMEX_CACHE_DIR``."""
    override = os.environ.get("BW_TIMEX_CACHE_DIR")
    base = Path(override) if override else Path(
        platformdirs.user_cache_path(appname="bw_timex", appauthor="bw_timex")
    )
    return base / _VERSION


def clear_persistent_cache() -> None:
    """Delete the entire on-disk bw_timex cache tree (best effort)."""
    root = cache_root()
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)


def _key_to_filename(key, suffix: str) -> str:
    digest = hashlib.blake2b(repr(key).encode("utf-8"), digest_size=20).hexdigest()
    return f"{digest}{suffix}"


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically; swallow write failures."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.stem}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass


class PersistentDict(MutableMapping):
    """In-memory dict mirrored to disk; values are triplets of numpy arrays.

    ``memory`` is the existing in-session dict (so cross-object sharing is
    preserved); ``disk_dir`` is where entries persist as ``.npz`` files.
    """

    def __init__(self, memory: dict, disk_dir: Path):
        self._mem = memory
        self._dir = Path(disk_dir)

    def _path(self, key) -> Path:
        return self._dir / _key_to_filename(key, ".npz")

    def __contains__(self, key) -> bool:
        return key in self._mem or self._path(key).exists()

    def __getitem__(self, key):
        if key in self._mem:
            return self._mem[key]
        path = self._path(key)
        if not path.exists():
            raise KeyError(key)
        try:
            with np.load(path) as npz:
                value = (npz["bio"], npz["act"], npz["val"])
        except Exception:
            try:
                path.unlink()
            except OSError:
                pass
            raise KeyError(key)
        self._mem[key] = value
        return value

    def __setitem__(self, key, value) -> None:
        self._mem[key] = value
        bio, act, val = value
        buf = io.BytesIO()
        np.savez(buf, bio=bio, act=act, val=val)
        _atomic_write_bytes(self._path(key), buf.getvalue())

    def __delitem__(self, key) -> None:
        existed = self._mem.pop(key, None) is not None
        path = self._path(key)
        if path.exists():
            path.unlink()
            existed = True
        if not existed:
            raise KeyError(key)

    def __iter__(self):
        return iter(self._mem)

    def __len__(self) -> int:
        return len(self._mem)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/persistent_cache.py tests/test_persistent_cache.py
git commit  # message: "feat: add persistent_cache core (PersistentDict, cache_root, clear)"
```

---

### Task 2: `LambdaDiskCache` — persist the 1-D λ vector

**Files:**
- Modify: `bw_timex/persistent_cache.py`
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: `_key_to_filename`, `_atomic_write_bytes`, `cache_root` (Task 1).
- Produces: `class LambdaDiskCache` with `__init__(self, disk_dir: pathlib.Path)`, `load(self, key) -> np.ndarray | None`, `save(self, key, array: np.ndarray) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_persistent_cache.py
def test_lambda_cache_roundtrip(tmp_path):
    from bw_timex.persistent_cache import LambdaDiskCache
    disk = tmp_path / "lam"
    key = ("lambda", "proj", ("m", "x"), (("db", 7),))
    c1 = LambdaDiskCache(disk)
    assert c1.load(key) is None
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    c1.save(key, arr)
    c2 = LambdaDiskCache(disk)  # fresh instance, same dir
    loaded = c2.load(key)
    np.testing.assert_allclose(loaded, arr)


def test_lambda_cache_corrupt_is_none(tmp_path):
    from bw_timex.persistent_cache import LambdaDiskCache
    disk = tmp_path / "lam"
    key = ("lambda", "p", ("m",), (("db", 1),))
    c = LambdaDiskCache(disk)
    c.save(key, np.array([1.0]))
    for f in disk.iterdir():
        f.write_bytes(b"garbage")
    assert LambdaDiskCache(disk).load(key) is None
    assert not any(disk.iterdir())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_persistent_cache.py::test_lambda_cache_roundtrip -v`
Expected: FAIL with `ImportError: cannot import name 'LambdaDiskCache'`.

- [ ] **Step 3: Write the implementation**

```python
# append to bw_timex/persistent_cache.py
class LambdaDiskCache:
    """Persist/reuse the adjoint lambda vector (a 1-D float array)."""

    def __init__(self, disk_dir: Path):
        self._dir = Path(disk_dir)

    def _path(self, key) -> Path:
        return self._dir / _key_to_filename(key, ".npz")

    def load(self, key):
        path = self._path(key)
        if not path.exists():
            return None
        try:
            with np.load(path) as npz:
                return npz["lam"]
        except Exception:
            try:
                path.unlink()
            except OSError:
                pass
            return None

    def save(self, key, array) -> None:
        buf = io.BytesIO()
        np.savez(buf, lam=np.asarray(array, dtype=np.float64))
        _atomic_write_bytes(self._path(key), buf.getvalue())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: all tests PASS (8 total).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/persistent_cache.py tests/test_persistent_cache.py
git commit  # message: "feat: add LambdaDiskCache for persistent adjoint lambda vector"
```

---

### Task 3: λ cache hook in `AdjointCachingSolver`

**Files:**
- Modify: `bw_timex/adjoint_scoring.py` (`AdjointCachingSolver.set_score_row`)
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: `LambdaDiskCache` (Task 2); `self.lca` carries optional attributes `_bw_timex_lambda_cache` (a `LambdaDiskCache` or None) and `_bw_timex_lambda_key` (a hashable key or None).
- Produces: modified `set_score_row` — on a cache hit, `lambda_vector` is loaded and `solve_count` stays 0 (no `spsolve`); on a miss, it solves once (`solve_count == 1`) and writes to the cache.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_persistent_cache.py (reuse _make_lca from tests/test_adjoint_scoring.py pattern)
import scipy.sparse as sp


class _FakeLCA:
    def __init__(self, A, B, cfs):
        self.technosphere_matrix = sp.csr_matrix(A)
        self._biosphere = sp.csr_matrix(B)
        self._cfs = np.asarray(cfs, dtype=float)
        self.solver = None
    def characterized_biosphere(self):
        return sp.csr_matrix(sp.diags(self._cfs) @ self._biosphere)
    def decompose_technosphere(self):
        pass
    def solve_linear_system(self, demand):
        from scipy.sparse.linalg import spsolve
        return spsolve(self.technosphere_matrix.tocsc(), demand)


def _make_lca():
    A = np.array([[1.0, -0.2, 0.0], [-0.1, 1.0, -0.3], [0.0, -0.4, 1.0]])
    B = np.array([[2.0, 0.0, 1.0], [0.0, 3.0, 0.0]])
    return _FakeLCA(A, B, [1.0, 0.5])


def test_solver_saves_then_skips_solve_on_hit(tmp_path):
    from bw_timex.persistent_cache import LambdaDiskCache
    from bw_timex.adjoint_scoring import AdjointCachingSolver
    cache = LambdaDiskCache(tmp_path / "lam")
    key = ("lambda", "p", ("m",), (("db", 1),))

    lca1 = _make_lca()
    lca1._bw_timex_lambda_cache = cache
    lca1._bw_timex_lambda_key = key
    s1 = AdjointCachingSolver(lca1)
    s1.set_score_row(lca1.characterized_biosphere())
    assert s1.solve_count == 1            # miss → solved once
    saved = s1.lambda_vector.copy()

    lca2 = _make_lca()
    lca2._bw_timex_lambda_cache = cache
    lca2._bw_timex_lambda_key = key
    s2 = AdjointCachingSolver(lca2)
    s2.set_score_row(lca2.characterized_biosphere())
    assert s2.solve_count == 0            # hit → no solve
    np.testing.assert_allclose(s2.lambda_vector, saved)


def test_solver_without_cache_attrs_behaves_as_before(tmp_path):
    from bw_timex.adjoint_scoring import AdjointCachingSolver
    lca = _make_lca()  # no cache attrs set
    s = AdjointCachingSolver(lca)
    s.set_score_row(lca.characterized_biosphere())
    assert s.solve_count == 1
    assert s.lambda_vector is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_persistent_cache.py::test_solver_saves_then_skips_solve_on_hit -v`
Expected: FAIL — `assert s2.solve_count == 0` fails (currently always solves, count becomes 1).

- [ ] **Step 3: Modify `set_score_row`**

Replace the body of `AdjointCachingSolver.set_score_row` in `bw_timex/adjoint_scoring.py` with:

```python
    def set_score_row(self, characterized_biosphere) -> None:
        # Sets ``self.score_row`` (length = number of technosphere columns).
        # Cheap column-sum; needed for inherited consumers even on a cache hit.
        super().set_score_row(characterized_biosphere)

        cache = getattr(self.lca, "_bw_timex_lambda_cache", None)
        key = getattr(self.lca, "_bw_timex_lambda_key", None)

        if cache is not None and key is not None:
            stored = cache.load(key)
            if stored is not None:
                self.lambda_vector = np.asarray(stored, dtype=float).ravel()
                self._prefill_score_cache()
                return  # cache hit: skip the adjoint solve (solve_count stays 0)

        a_transpose = self.lca.technosphere_matrix.transpose().tocsc()
        self.lambda_vector = np.asarray(
            spsolve(a_transpose, np.asarray(self.score_row, dtype=float))
        ).ravel()
        self.solve_count += 1
        self._prefill_score_cache()

        if cache is not None and key is not None:
            cache.save(key, self.lambda_vector)

    def _prefill_score_cache(self) -> None:
        # Pre-fill the inherited per-index cache so any code path that consults
        # it agrees with the lookup-based ``scores`` below.
        for index, unit_score in enumerate(self.lambda_vector):
            self._score_cache[index] = float(unit_score)
```

(This extracts the existing prefill loop into `_prefill_score_cache` and reuses it on both paths.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_persistent_cache.py tests/test_adjoint_scoring.py -v`
Expected: new tests PASS and all existing adjoint tests still PASS (the no-cache-attrs path is unchanged: `solve_count == 1`).

- [ ] **Step 5: Commit**

```bash
git add bw_timex/adjoint_scoring.py tests/test_persistent_cache.py
git commit  # message: "feat: consult persistent lambda cache in AdjointCachingSolver.set_score_row"
```

---

### Task 4: platformdirs dependency + clear-cache wiring + exports

**Files:**
- Modify: `pyproject.toml` (dependencies list)
- Modify: `bw_timex/_lci_cache.py` (`clear_background_lci_cache`)
- Modify: `bw_timex/__init__.py`
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: `clear_persistent_cache` (Task 1).
- Produces: `bw_timex.clear_persistent_cache` importable from package root; `clear_background_lci_cache()` also clears the disk cache; `platformdirs` declared as a direct dependency.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_persistent_cache.py
def test_exports_and_combined_clear(tmp_path):
    import bw_timex
    from bw_timex.persistent_cache import PersistentDict, cache_root
    assert hasattr(bw_timex, "clear_persistent_cache")
    # clear_background_lci_cache also wipes disk.
    d = PersistentDict(memory={}, disk_dir=cache_root() / "background_unit_lci")
    d[("db_code", "p", "db", "code", 1)] = _triplet()
    assert cache_root().exists()
    bw_timex.clear_background_lci_cache()
    assert not cache_root().exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_persistent_cache.py::test_exports_and_combined_clear -v`
Expected: FAIL (`AttributeError: module 'bw_timex' has no attribute 'clear_persistent_cache'`, or the disk tree persists after the combined clear).

- [ ] **Step 3: Implement**

In `pyproject.toml`, add `"platformdirs"` to the `dependencies` list (after `"pydantic>=2.0",`):

```toml
    "pydantic>=2.0",
    "platformdirs",
]
```

In `bw_timex/_lci_cache.py`, extend `clear_background_lci_cache` so it also clears disk:

```python
def clear_background_lci_cache() -> None:
    """Clear all module-level bw_timex caches (unit LCI, biosphere exchanges, solve, nodes) and the persistent disk cache."""
    BACKGROUND_UNIT_LCI_CACHE.clear()
    BIOSPHERE_EXCHANGES_CACHE.clear()
    LCI_SOLVE_CACHE.clear()
    NODES_CACHE.clear()
    from .persistent_cache import clear_persistent_cache
    clear_persistent_cache()
```

In `bw_timex/__init__.py`, add after the existing `from .adjoint_scoring import ...` line:

```python
from .persistent_cache import clear_persistent_cache
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: all PASS. Then confirm the dep resolves: `uv sync` exits 0 (platformdirs already in `uv.lock`).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml bw_timex/_lci_cache.py bw_timex/__init__.py tests/test_persistent_cache.py
git commit  # message: "feat: platformdirs dep, clear_persistent_cache export, combined clear"
```

---

### Task 5: `TimexLCA` wiring — `persistent_cache` flag, background swap, λ key stash

**Files:**
- Modify: `bw_timex/timex_lca.py:102-107` (`__init__` signature), `:206-207` (background cache), `build_timeline` (λ stash around traversal)
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: `PersistentDict`, `LambdaDiskCache`, `cache_root` (Tasks 1–2); `AdjointCachingSolver`'s attribute contract (`base_lca._bw_timex_lambda_cache`, `base_lca._bw_timex_lambda_key`) (Task 3).
- Produces:
  - `TimexLCA.__init__(..., persistent_cache: bool = True)`.
  - When `persistent_cache and use_global_lci_cache`, `self._background_unit_lci_cache` is a `PersistentDict` over `cache_root()/"background_unit_lci"` backed by `BACKGROUND_UNIT_LCI_CACHE`.
  - `TimexLCA._build_lambda_cache_key() -> tuple` = `("lambda", bd.projects.current, str(self.method), tuple(sorted((db, bd.databases[db].get("modified")) for db in self.database_dates)))`.
  - During `build_timeline(adjoint_scoring=True, ...)` with `persistent_cache` on, `self.base_lca._bw_timex_lambda_cache`/`_bw_timex_lambda_key` are set before the traversal and removed afterward (in a `finally`).

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_persistent_cache.py
from datetime import datetime


def _tlca(persistent):
    import bw2data as bd
    from bw_timex import TimexLCA
    fu = bd.get_node(database="foreground", code="A")
    return TimexLCA(
        demand={fu.id: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2022": datetime.strptime("2022", "%Y"),
            "db_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",
        },
        persistent_cache=persistent,
    )


def test_background_cache_is_persistentdict_when_enabled(temporal_grouping_db_monthly):
    from bw_timex.persistent_cache import PersistentDict
    tlca = _tlca(persistent=True)
    assert isinstance(tlca._background_unit_lci_cache, PersistentDict)


def test_background_cache_plain_dict_when_disabled(temporal_grouping_db_monthly):
    from bw_timex.persistent_cache import PersistentDict
    tlca = _tlca(persistent=False)
    assert not isinstance(tlca._background_unit_lci_cache, PersistentDict)


def test_lambda_key_includes_method_and_modified(temporal_grouping_db_monthly):
    tlca = _tlca(persistent=True)
    key = tlca._build_lambda_cache_key()
    assert key[0] == "lambda"
    assert key[2] == str(("GWP", "example"))
    # db modified tokens are present
    dbs = dict(key[3])
    assert "db_2022" in dbs and "foreground" in dbs


def test_lambda_attrs_cleaned_after_build(temporal_grouping_db_monthly):
    tlca = _tlca(persistent=True)
    tlca.build_timeline(adjoint_scoring=True)
    assert getattr(tlca.base_lca, "_bw_timex_lambda_cache", None) is None
    assert getattr(tlca.base_lca, "_bw_timex_lambda_key", None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_persistent_cache.py::test_background_cache_is_persistentdict_when_enabled -v`
Expected: FAIL — `TimexLCA.__init__` has no `persistent_cache` kwarg (`TypeError`).

- [ ] **Step 3: Implement**

In `bw_timex/timex_lca.py`, add the parameter to `__init__`:

```python
    def __init__(
        self,
        demand: dict,
        method: tuple,
        database_dates: dict = None,
        use_global_lci_cache: bool = True,
        persistent_cache: bool = True,
    ) -> None:
```

Store it early in `__init__` (next to other attribute assignments, before the background-cache assignment at line ~206):

```python
        self.persistent_cache = persistent_cache
```

Replace the background-cache assignment (`timex_lca.py:206-207`) with:

```python
        if use_global_lci_cache and persistent_cache:
            from .persistent_cache import PersistentDict, cache_root
            self._background_unit_lci_cache = PersistentDict(
                memory=BACKGROUND_UNIT_LCI_CACHE,
                disk_dir=cache_root() / "background_unit_lci",
            )
        else:
            self._background_unit_lci_cache = (
                BACKGROUND_UNIT_LCI_CACHE if use_global_lci_cache else {}
            )
```

Add the key builder method (place it near `create_demand_timing`):

```python
    def _build_lambda_cache_key(self) -> tuple:
        """Persistent-cache key for the adjoint lambda vector.

        Keyed by project, method, and the ``modified`` tokens of every database
        in ``database_dates`` so any tracked database edit invalidates it.
        """
        dbs = tuple(
            sorted(
                (db, bd.databases[db].get("modified") if db in bd.databases else None)
                for db in self.database_dates
            )
        )
        return ("lambda", bd.projects.current, str(self.method), dbs)
```

In `build_timeline`, wrap the call that runs the traversal (the line that constructs `TimelineBuilder(...)` / triggers `EdgeExtractor` traversal — currently around `timex_lca.py:384`) so the λ attributes are set on `base_lca` only when adjoint scoring + persistence are both active, and always cleaned up:

```python
        lambda_attrs_set = False
        if adjoint_scoring and self.persistent_cache:
            from .persistent_cache import LambdaDiskCache, cache_root
            self.base_lca._bw_timex_lambda_cache = LambdaDiskCache(
                cache_root() / "adjoint_intensities"
            )
            self.base_lca._bw_timex_lambda_key = self._build_lambda_cache_key()
            lambda_attrs_set = True
        try:
            self.timeline_builder = TimelineBuilder(
                # ... existing arguments unchanged ...
            )
            # ... existing body that builds self.timeline ...
        finally:
            if lambda_attrs_set:
                self.base_lca._bw_timex_lambda_cache = None
                self.base_lca._bw_timex_lambda_key = None
```

Note for the implementer: `adjoint_scoring` is already a parameter of `build_timeline` (added in P1). Keep all existing `TimelineBuilder(...)` arguments exactly as they are; only wrap them with the stash/cleanup shown above. Place the stash AFTER `base_lca` is guaranteed to exist (it is created in `__init__`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_persistent_cache.py
git commit  # message: "feat: wire persistent_cache flag + lambda key into TimexLCA"
```

---

### Task 6: End-to-end validation gate

**Files:**
- Test: `tests/test_persistent_cache.py`

**Interfaces:**
- Consumes: the full wiring (Tasks 1–5); fixtures `temporal_grouping_db_monthly` and `background_td_deep_chain_db` (registered in `tests/conftest.py`).
- Produces: regression tests proving cold/warm cache equivalence, zero-disk-IO when disabled, λ reuse across instances, and clear behavior.

- [ ] **Step 1: Write the equivalence + behavior tests**

```python
# append to tests/test_persistent_cache.py
def _run_full(persistent):
    tlca = _tlca(persistent=persistent)
    tlca.build_timeline(adjoint_scoring=True)
    tlca.lci(expand_technosphere=True, build_dynamic_biosphere=True)
    tlca.static_lcia()
    return tlca.static_score


def test_cold_vs_warm_cache_scores_identical(temporal_grouping_db_monthly):
    cold = _run_full(persistent=True)   # populates disk
    warm = _run_full(persistent=True)   # reuses disk
    np.testing.assert_allclose(warm, cold, rtol=1e-12)


def test_persistent_false_writes_no_disk(temporal_grouping_db_monthly):
    from bw_timex.persistent_cache import cache_root
    _run_full(persistent=False)
    # No cache files written when disabled.
    assert not cache_root().exists() or not any(cache_root().rglob("*.npz"))


def test_lambda_reused_from_disk_second_instance(temporal_grouping_db_monthly):
    import bw_timex.adjoint_scoring as adj
    counts = []
    real = adj.AdjointCachingSolver.set_score_row

    def spy(self, cb):
        real(self, cb)
        counts.append(self.solve_count)

    import pytest as _pytest
    with _pytest.MonkeyPatch.context() as mp:
        mp.setattr(adj.AdjointCachingSolver, "set_score_row", spy)
        _tlca(persistent=True).build_timeline(adjoint_scoring=True)  # miss → solve
        first = list(counts)
        counts.clear()
        _tlca(persistent=True).build_timeline(adjoint_scoring=True)  # hit → no solve

    assert any(c == 1 for c in first)        # first build actually solved
    assert counts and all(c == 0 for c in counts)  # second build reused from disk


def test_clear_persistent_cache_empties_dir(temporal_grouping_db_monthly):
    from bw_timex.persistent_cache import cache_root, clear_persistent_cache
    _run_full(persistent=True)
    assert any(cache_root().rglob("*.npz"))
    clear_persistent_cache()
    assert not cache_root().exists()
```

- [ ] **Step 2: Run the new tests**

Run: `uv run pytest tests/test_persistent_cache.py -v`
Expected: all PASS. If `test_cold_vs_warm_cache_scores_identical` FAILS, the cache changed results — debug with `superpowers:systematic-debugging`; do NOT loosen the tolerance.

- [ ] **Step 3: Add a deep-chain background-LCI reuse test**

```python
# append to tests/test_persistent_cache.py
def test_background_unit_lci_reused_across_instances(background_td_deep_chain_db):
    import bw2data as bd
    from datetime import datetime
    from bw_timex import TimexLCA
    from bw_timex.persistent_cache import cache_root

    def build():
        fu = bd.get_node(database="foreground", code="fu")
        t = TimexLCA(
            demand={fu.id: 1},
            method=("GWP", "example"),
            database_dates={
                "background_2020": datetime.strptime("2020", "%Y"),
                "background_2030": datetime.strptime("2030", "%Y"),
                "foreground": "dynamic",
            },
            persistent_cache=True,
        )
        t.build_timeline(adjoint_scoring=True)
        t.lci(expand_technosphere=True, build_dynamic_biosphere=True)
        t.static_lcia()
        return t.static_score

    first = build()
    # Background unit LCI triplets now on disk.
    assert any((cache_root() / "background_unit_lci").rglob("*.npz"))
    second = build()
    np.testing.assert_allclose(second, first, rtol=1e-12)
```

Note: confirm `background_td_deep_chain_db`'s database names / demand code by reading `tests/fixtures/background_td_deep_chain_db_fixture.py` (it uses `background_2020`/`background_2030` and demand code `"fu"`); adapt the dict above if the fixture differs.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: all pre-existing tests still PASS, plus the new `tests/test_persistent_cache.py` tests; no new warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/test_persistent_cache.py
git commit  # message: "test: end-to-end persistent-cache validation gate"
```

---

## Self-Review

**Spec coverage:**
- platformdirs root + `v1` version + `BW_TIMEX_CACHE_DIR` override → Task 1 (`cache_root`). ✓
- `PersistentDict` (MutableMapping, write-through, fall-through, corrupt=miss, atomic) → Task 1. ✓
- `LambdaDiskCache` (load/save 1-D) → Task 2. ✓
- λ skip-on-hit (`solve_count` stays 0), save-on-miss; reached via `base_lca` attributes (spec's literal "param on traversal" refined to attribute-stash because `bw_temporalis.calculate()` controls instantiation) → Task 3 + Task 5 stash. ✓
- modified-token + method keying; background 5-tuple key; λ key form → Tasks 3/5 + Global Constraints. ✓
- on-by-default + off switch (`persistent_cache`) gated by `use_global_lci_cache` → Task 5. ✓
- `clear_persistent_cache()` + combined `clear_background_lci_cache()` + exports → Task 4. ✓
- platformdirs direct dep → Task 4. ✓
- Error handling (never load-bearing, atomic writes, swallow) → Task 1 helpers + Tasks 1/2 load paths. ✓
- Tests use `BW_TIMEX_CACHE_DIR` tmp_path; cold/warm parity; `persistent_cache=False` no disk IO; cross-instance reuse; clear empties → Tasks 1–6. ✓
- Out-of-scope caches (LCI_SOLVE/NODES/biosphere) untouched. ✓

**Placeholder scan:** No TBD/TODO/"add error handling". Two explicit "confirm against fixture/keep existing args" notes (Task 5 `TimelineBuilder` args; Task 6 deep-chain fixture) point at concrete files, not vague work. ✓

**Type consistency:** `PersistentDict(memory, disk_dir)`, `LambdaDiskCache(disk_dir)`, `cache_root()`, `clear_persistent_cache()`, `_build_lambda_cache_key()`, attributes `_bw_timex_lambda_cache`/`_bw_timex_lambda_key`, `persistent_cache` flag — all used identically across Tasks 1–6. Background value triplet `(bio, act, val)` consistent between serialization (Task 1) and the builder's existing `_inventory_to_triplets`. ✓
