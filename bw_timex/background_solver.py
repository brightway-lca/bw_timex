"""Per-block background solving with a supply-column cache.

`bw_timex.block_structure` finds the diagonal blocks of a time-explicit
technosphere. This module solves them: a unit demand for a background
activity is placed on its product row, solved within the owning block only,
and the resulting supply column - not the inventory matrix a naive
`redo_lci` would build - is what gets cached.

Caching supply columns instead of `B @ diag(x)` is the point of this module.
An inventory matrix is one column per activity in the technosphere times the
number of biosphere flows it touches; a supply column is one number per
background activity actually produced along the way. On a premise-sized
background that difference is the gap between ~5.5 MB and ~0.35 MB per
cached entry, and it is what keeps `lci()` from running out of memory.

Cache payloads are node-id keyed, not index keyed: a `(node_ids, values)`
pair of 1-D arrays holding only the nonzero entries, translated back into a
consuming solver's local index space with `np.searchsorted` over a sorted id
array. That indirection is what lets the *same* cache entry serve solvers
built over different lca_obj index spaces (different timelines, expand
modes, or - for the module-level `shared_cache` - different `TimexLCA`
objects in one session).
"""

from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np
from scipy.sparse.linalg import factorized, spsolve

from ._lci_cache import BACKGROUND_AGGREGATE_CACHE, BACKGROUND_SUPPLY_CACHE
from .block_structure import BlockStructure


@dataclass(frozen=True, eq=False)
class UnitSupply:
    """A unit supply column, dense within the block that produced it.

    `values` is aligned with `structure.blocks[block_index].columns` - i.e.
    `values[i]` is the supply of the activity at column
    `structure.blocks[block_index].columns[i]`, not of column `i` of the
    full matrix.
    """

    block_index: int
    values: np.ndarray


class BackgroundSolver:
    """Solves unit background LCIs block by block, caching supply columns.

    Parameters
    ----------
    technosphere_matrix
        The (square) technosphere matrix, in any scipy sparse format.
    biosphere_matrix
        The biosphere matrix, rows are elementary flows, columns line up
        with `technosphere_matrix`'s columns.
    activity_dict
        Maps activity/process node id -> technosphere column index (a
        `bw2calc` `ReversibleRemappableDictionary`, e.g. `lca.dicts.activity`).
    product_dict
        Maps product node id -> technosphere row index
        (`lca.dicts.product`). A unit demand for a background activity is
        placed here, not on its column - this is how `bw2calc.LCA` builds a
        demand array.
    biosphere_dict
        Maps biosphere flow node id -> biosphere matrix row index
        (`lca.dicts.biosphere`).
    structure
        The `BlockStructure` describing how `technosphere_matrix` splits
        into diagonal blocks.
    """

    def __init__(
        self,
        *,
        technosphere_matrix,
        biosphere_matrix,
        activity_dict,
        product_dict,
        biosphere_dict,
        structure: BlockStructure,
    ) -> None:
        self.technosphere_matrix = technosphere_matrix.tocsc()
        self.biosphere_matrix = biosphere_matrix.tocsc()
        self.activity_dict = activity_dict
        self.product_dict = product_dict
        self.biosphere_dict = biosphere_dict
        self.structure = structure

        # Cache key routing: a `("db_code", ...)` key names a background
        # process identity that is stable across `TimexLCA` objects (same
        # project/db/code, unchanged since), so it is safe to share; anything
        # else stays local to this solver instance. Mirrors
        # `DynamicBiosphereBuilder.get_background_lci_cache_key`'s split.
        # Callers that can establish a stable identity (e.g. `TimexLCA`,
        # which knows the time mapping) are expected to override this.
        self.cache_key: Callable = self._default_cache_key

        # Module-level, cross-object caches. Reassignable per-instance (tests
        # inject an isolated dict); default to the real module caches so
        # normal use shares results across `TimexLCA` objects in a session.
        self.shared_cache = BACKGROUND_SUPPLY_CACHE
        self.shared_aggregate_cache = BACKGROUND_AGGREGATE_CACHE
        # Per-instance caches for everything that is not a stable db/code
        # identity (time-mapped activity ids, temporalized processes, ...).
        self._instance_supply_cache: dict = {}
        self._instance_aggregate_cache: dict = {}

        # Solves performed; cache hits do not count. Exposed for tests and
        # for callers deciding whether pre-factorizing was worth it.
        self.n_solves = 0

        # Factorizations are per-instance only, never module-level: an LU of
        # an ecoinvent-sized block is large (several times the matrix it
        # came from), and several of them cached across a session would
        # undo the memory savings this module exists for.
        self._block_solvers: dict = {}
        self.factorized_blocks: set = set()
        self._block_submatrices: dict = {}
        self._block_biosphere_submatrices: dict = {}

        # Translated results, memoized per instance. The caches above hold
        # node-id-keyed payloads so they can be shared between solvers; every
        # read of one has to scatter it back into *this* solver's index space,
        # which is a `searchsorted` plus a dense allocation. Building from the
        # timeline asks for the same background activity once per row that
        # consumes it, so that scatter ran far more often than the solve it
        # was caching. These memos hold the scattered arrays, sized to this
        # solver, and die with it.
        #
        # Aggregates are memoized on first sight - they are small (one value
        # per biosphere flow) and the matrix build re-reads them constantly.
        # Supply columns are not: they are an order of magnitude larger, and
        # the build asks for each exactly once, through `unit_aggregate`'s
        # miss path. Keeping those would be pure memory cost on the path that
        # runs out of it first. `_seen_supply` defers the memo to the second
        # request, which is where the re-reads actually live
        # (`TimexLCA.temporal_market_lcis` and the disaggregation behind it).
        self._translated_supply: dict = {}
        self._translated_aggregate: dict = {}
        self._seen_supply: set = set()

        # Lazily built, cached once per instance.
        self._column_node_ids_cache: Optional[np.ndarray] = None
        self._biosphere_node_ids_cache: Optional[np.ndarray] = None
        self._sorted_column_ids_cache: dict = {}
        self._sorted_biosphere_ids_cache: Optional[tuple] = None
        self._row_block_index = self._build_row_block_index()

    # -- id-space translation ------------------------------------------------

    @staticmethod
    def _default_cache_key(act) -> tuple:
        """Fallback cache key: the raw node id, always instance-local.

        `BackgroundSolver` has no notion of a time mapping, so it cannot by
        itself tell a stable background-process identity from a time-mapped
        or temporalized one. Callers that can (e.g. `TimexLCA`, which knows
        which ids are time-mapped) should set `solver.cache_key` to a
        function returning `("db_code", ...)` for those that are stable.
        """
        return ("activity_id", act)

    def _select_cache(self, cache_key: tuple, shared: dict, instance: dict) -> dict:
        return shared if cache_key[0] == "db_code" else instance

    def _column_node_ids(self) -> np.ndarray:
        """Technosphere column index -> node id, built once per instance."""
        if self._column_node_ids_cache is None:
            n = self.technosphere_matrix.shape[1]
            arr = np.full(n, -1, dtype=np.int64)
            for index, node_id in self.activity_dict.reversed.items():
                arr[index] = node_id
            self._column_node_ids_cache = arr
        return self._column_node_ids_cache

    def _biosphere_node_ids(self) -> np.ndarray:
        """Biosphere row index -> node id, built once per instance."""
        if self._biosphere_node_ids_cache is None:
            n = self.biosphere_matrix.shape[0]
            arr = np.full(n, -1, dtype=np.int64)
            for index, node_id in self.biosphere_dict.reversed.items():
                arr[index] = node_id
            self._biosphere_node_ids_cache = arr
        return self._biosphere_node_ids_cache

    def _sorted_column_ids(self, block_index: int) -> tuple:
        """Sorted node ids of a block's columns, plus the local positions
        they came from (`sorted_ids[k]` is the id of local column
        `order[k]`). Used to translate a node-id-keyed cache payload back
        into this block's local column space with `np.searchsorted`.
        """
        if block_index not in self._sorted_column_ids_cache:
            block = self.structure.blocks[block_index]
            ids = self._column_node_ids()[block.columns]
            order = np.argsort(ids)
            self._sorted_column_ids_cache[block_index] = (ids[order], order)
        return self._sorted_column_ids_cache[block_index]

    def _sorted_biosphere_ids(self) -> tuple:
        if self._sorted_biosphere_ids_cache is None:
            ids = self._biosphere_node_ids()
            order = np.argsort(ids)
            self._sorted_biosphere_ids_cache = (ids[order], order)
        return self._sorted_biosphere_ids_cache

    @staticmethod
    def _translate(
        payload: tuple, sorted_ids: np.ndarray, order: np.ndarray, size: int
    ) -> np.ndarray:
        """Scatter a node-id-keyed `(ids, values)` payload into a dense
        array of length `size`, via `order` (local positions aligned with
        `sorted_ids`). Ids absent from `sorted_ids` are skipped - a cache
        entry built from a wider set of databases than the current solver
        covers is simply missing those entries, not wrong.
        """
        ids, values = payload
        result = np.zeros(size)
        if len(ids) == 0 or len(sorted_ids) == 0:
            return result
        positions = np.searchsorted(sorted_ids, ids)
        positions = np.clip(positions, 0, len(sorted_ids) - 1)
        found = sorted_ids[positions] == ids
        result[order[positions[found]]] = values[found]
        return result

    def _build_row_block_index(self) -> np.ndarray:
        """Product row index -> owning block index, vectorized over blocks
        (not rows: this is a handful of blocks, never a Python loop over
        the hundreds of thousands of rows in each of them)."""
        n_rows = self.technosphere_matrix.shape[0]
        arr = np.full(n_rows, -1, dtype=np.int64)
        for block_index, block in enumerate(self.structure.blocks):
            arr[block.rows] = block_index
        return arr

    # -- public API -----------------------------------------------------------

    def block_index_for(self, activity_id) -> int:
        """The block that solving a unit demand for `activity_id` lands in."""
        row = self.product_dict[activity_id]
        return int(self._row_block_index[row])

    def unit_supply(self, activity_id) -> UnitSupply:
        """Unit supply column for `activity_id`, from cache or a fresh solve.

        `values` is a fresh array on every call - callers are free to write
        into it without disturbing the memo behind it.
        """
        cache_key = self.cache_key(activity_id)
        block_index = self.block_index_for(activity_id)

        memoized = self._translated_supply.get(cache_key)
        if memoized is not None:
            return UnitSupply(block_index=block_index, values=memoized.copy())

        cache = self._select_cache(cache_key, self.shared_cache, self._instance_supply_cache)
        block = self.structure.blocks[block_index]

        if cache_key in cache:
            sorted_ids, order = self._sorted_column_ids(block_index)
            values = self._translate(cache[cache_key], sorted_ids, order, len(block.columns))
        else:
            row = self.product_dict[activity_id]
            local_row = np.searchsorted(block.rows, row)
            rhs = np.zeros(len(block.rows))
            rhs[local_row] = 1.0
            values = self.solve_block(block_index, rhs)

            nonzero = np.flatnonzero(values)
            column_ids = self._column_node_ids()[block.columns[nonzero]]
            cache[cache_key] = (column_ids, values[nonzero].copy())

        if cache_key in self._seen_supply:
            self._translated_supply[cache_key] = values
        else:
            self._seen_supply.add(cache_key)
        return UnitSupply(block_index=block_index, values=values.copy())

    def unit_aggregate(self, activity_id) -> np.ndarray:
        """Unit LCI aggregated over biosphere rows: `B[:, cols] @ x`.

        Dense, over *all* biosphere rows (not block-scoped, unlike
        `unit_supply`) - a background LCI touches a small fraction of a
        large biosphere, but which fraction depends on the activity, not
        the block. A fresh array on every call, so callers may write into it.
        """
        cache_key = self.cache_key(activity_id)

        memoized = self._translated_aggregate.get(cache_key)
        if memoized is not None:
            return memoized.copy()

        cache = self._select_cache(
            cache_key, self.shared_aggregate_cache, self._instance_aggregate_cache
        )

        if cache_key in cache:
            sorted_ids, order = self._sorted_biosphere_ids()
            aggregate = self._translate(
                cache[cache_key], sorted_ids, order, self.biosphere_matrix.shape[0]
            )
        else:
            supply = self.unit_supply(activity_id)
            aggregate = np.asarray(
                self._biosphere_submatrix(supply.block_index) @ supply.values
            ).ravel()

            nonzero = np.flatnonzero(aggregate)
            bio_ids = self._biosphere_node_ids()[nonzero]
            cache[cache_key] = (bio_ids, aggregate[nonzero].copy())

        self._translated_aggregate[cache_key] = aggregate
        return aggregate.copy()

    def solve_block(self, block_index: int, rhs: np.ndarray) -> np.ndarray:
        """Solve `A[block.rows][:, block.columns] x = rhs` for one block.

        Uses a cached LU (from `prepare`) when one exists for this block,
        else a one-off `spsolve`. Every call is a real linear solve and
        increments `n_solves` - caching happens one level up, in
        `unit_supply`/`unit_aggregate`.

        One right-hand side at a time, and not for want of trying: bundling
        `k` of them into a single `(n_rows, k)` solve is not available here,
        because with `scikit-umfpack` installed `scipy`'s `factorized`
        returns a UMFPACK solver that rejects a 2-D right-hand side, and
        falling back to SuperLU to get one costs more than the bundling
        saves.
        """
        solve = self._block_solvers.get(block_index)
        if solve is not None:
            result = solve(rhs)
        else:
            sub = self._submatrix(block_index)
            result = spsolve(sub, rhs)
        self.n_solves += 1
        return np.asarray(result, dtype=float)

    def prepare(self, activity_ids, n_jobs: Optional[int] = None) -> None:
        """Pre-factorize blocks that will pay off, for a batch of activities.

        Groups the *uncached* ids by the block a solve for them would use,
        and factorizes only blocks with more than one pending solve:
        factorizing an ecoinvent-sized block costs roughly a hundred times a
        single `spsolve` on it, so it only pays off once several solves in
        that block share the cost.

        `activity_ids` is counted by *identity*, not by occurrence. Callers
        collect it per temporal market, and distinct markets of the same
        process at different times demand the very same background vintages -
        so repeats are the norm. Counting them twice would buy an LU for a
        block that needs a single solve, which is the exact trade this method
        exists to avoid.

        `n_jobs` is accepted for a future parallel implementation and
        ignored here.
        """
        pending_counts: dict = {}
        pending_keys: set = set()
        for activity_id in activity_ids:
            cache_key = self.cache_key(activity_id)
            if cache_key in pending_keys:
                continue
            cache = self._select_cache(
                cache_key, self.shared_cache, self._instance_supply_cache
            )
            if cache_key in cache:
                continue
            pending_keys.add(cache_key)
            block_index = self.block_index_for(activity_id)
            pending_counts[block_index] = pending_counts.get(block_index, 0) + 1

        for block_index, count in pending_counts.items():
            if count > 1:
                self._factorize_block(block_index)

    # -- internals --------------------------------------------------------

    def _submatrix(self, block_index: int):
        if block_index not in self._block_submatrices:
            block = self.structure.blocks[block_index]
            self._block_submatrices[block_index] = self.technosphere_matrix[
                block.rows
            ][:, block.columns].tocsc()
        return self._block_submatrices[block_index]

    def _biosphere_submatrix(self, block_index: int):
        """`B[:, block.columns]`, memoized per block.

        Rebuilding this slice per background activity is an O(nnz) walk of
        the biosphere matrix on every unit LCI; a block is sliced once and
        then reused by every activity that lands in it.
        """
        if block_index not in self._block_biosphere_submatrices:
            block = self.structure.blocks[block_index]
            self._block_biosphere_submatrices[block_index] = self.biosphere_matrix[
                :, block.columns
            ].tocsr()
        return self._block_biosphere_submatrices[block_index]

    def _factorize_block(self, block_index: int) -> None:
        if block_index in self.factorized_blocks:
            return
        sub = self._submatrix(block_index)
        self._block_solvers[block_index] = factorized(sub)
        self.factorized_blocks.add(block_index)
