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
    """A unit supply column, dense over every column of the technosphere.

    An activity's own block can be a pure consumer - solved first, with no
    footprint of its own - while its actual impact lives in a block further
    down the chain that its demand cascades into (a small hand-modified
    database referencing the real background it draws materials from, for
    instance). So a unit demand is not confined to the block that owns the
    requested activity: it is solved there, then propagated into every
    later block (consumer-first order, so a later block can depend on an
    earlier one but never the reverse) whose right-hand side comes out
    nonzero as a result, and so on until nothing new is touched.

    `touched_blocks` names which blocks actually received a solve, so a
    caller (`unit_aggregate`) can multiply only those blocks' biosphere
    columns instead of the full-width matrix.
    """

    values: np.ndarray
    touched_blocks: frozenset


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
        self._sorted_column_ids_cache: Optional[tuple] = None
        self._sorted_biosphere_ids_cache: Optional[tuple] = None
        self._row_block_index = self._build_row_block_index()
        self._column_block_index = self._build_column_block_index()

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

    def _sorted_column_ids_all(self) -> tuple:
        """Sorted node ids of every technosphere column, plus the positions
        they came from - like `_sorted_biosphere_ids`, but for columns.
        Used to translate a node-id-keyed supply cache payload back into
        this solver's full column index space with `np.searchsorted`; a
        cached supply is no longer confined to one block (see
        `_cascading_solve`), so the lookup can no longer be either.
        """
        if self._sorted_column_ids_cache is None:
            ids = self._column_node_ids()
            order = np.argsort(ids)
            self._sorted_column_ids_cache = (ids[order], order)
        return self._sorted_column_ids_cache

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

    def _build_column_block_index(self) -> np.ndarray:
        """Technosphere column index -> owning block index. The column-space
        counterpart of `_build_row_block_index`, used to tell which blocks a
        cached (translated) supply touches without re-walking `structure`."""
        n_columns = self.technosphere_matrix.shape[1]
        arr = np.full(n_columns, -1, dtype=np.int64)
        for block_index, block in enumerate(self.structure.blocks):
            arr[block.columns] = block_index
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

        memoized = self._translated_supply.get(cache_key)
        if memoized is not None:
            values, touched_blocks = memoized
            return UnitSupply(values=values.copy(), touched_blocks=touched_blocks)

        cache = self._select_cache(cache_key, self.shared_cache, self._instance_supply_cache)

        if cache_key in cache:
            sorted_ids, order = self._sorted_column_ids_all()
            values = self._translate(
                cache[cache_key], sorted_ids, order, self.technosphere_matrix.shape[1]
            )
            touched_blocks = self._blocks_touched_by(values)
        else:
            block_index = self.block_index_for(activity_id)
            block = self.structure.blocks[block_index]
            rhs = np.zeros(len(block.rows))
            rhs[self._local_row(block, activity_id)] = 1.0
            values, touched_blocks = self._cascading_solve({block_index: rhs})

            nonzero = np.flatnonzero(values)
            column_ids = self._column_node_ids()[nonzero]
            cache[cache_key] = (column_ids, values[nonzero].copy())

        if cache_key in self._seen_supply:
            self._translated_supply[cache_key] = (values, touched_blocks)
        else:
            self._seen_supply.add(cache_key)
        return UnitSupply(values=values.copy(), touched_blocks=touched_blocks)

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
            aggregate = self._aggregate_over_touched_blocks(supply)

            nonzero = np.flatnonzero(aggregate)
            bio_ids = self._biosphere_node_ids()[nonzero]
            cache[cache_key] = (bio_ids, aggregate[nonzero].copy())

        self._translated_aggregate[cache_key] = aggregate
        return aggregate.copy()

    def aggregate_for_demand(self, demand: dict) -> np.ndarray:
        """Biosphere aggregate of a *combined* demand `{activity_id: amount}`.

        Equal to `sum(unit_aggregate(act) * amount)` by linearity, but reached
        with one solve per block the demand touches instead of one per
        activity. That is the whole point: a caller that has already summed
        many timeline rows into one demand pays for the blocks, not the rows.

        A temporal market interpolates between vintages living in different
        background databases - different diagonal blocks - so a demand
        routinely spans more than one, and each block is solved separately;
        so can any block a seeded activity's demand cascades into further
        downstream (see `_cascading_solve`), and those are included too.

        Nothing is cached here. The demand is a sum specific to one caller's
        grouping, so it has no stable identity to key on, unlike the per
        activity unit LCIs of `unit_supply` / `unit_aggregate`.
        """
        seeds: dict = {}
        for activity_id, amount in demand.items():
            block_index = self.block_index_for(activity_id)
            block = self.structure.blocks[block_index]
            rhs = seeds.setdefault(block_index, np.zeros(len(block.rows)))
            rhs[self._local_row(block, activity_id)] += amount

        values, touched_blocks = self._cascading_solve(seeds)
        return self._aggregate_over_touched_blocks(
            UnitSupply(values=values, touched_blocks=touched_blocks)
        )

    def _aggregate_over_touched_blocks(self, supply: "UnitSupply") -> np.ndarray:
        """`B[:, cols] @ supply.values`, done per touched block.

        A handful of small slices (`_biosphere_submatrix` is memoized per
        block) instead of one sparse-times-dense product over the full
        column width, most of which is zero.
        """
        aggregate = np.zeros(self.biosphere_matrix.shape[0])
        for block_index in supply.touched_blocks:
            block = self.structure.blocks[block_index]
            local = supply.values[block.columns]
            aggregate += np.asarray(
                self._biosphere_submatrix(block_index) @ local
            ).ravel()
        return aggregate

    def _blocks_touched_by(self, values: np.ndarray) -> frozenset:
        """Which blocks have at least one nonzero entry in a global `values`."""
        nonzero_columns = np.flatnonzero(values)
        if len(nonzero_columns) == 0:
            return frozenset()
        return frozenset(int(b) for b in np.unique(self._column_block_index[nonzero_columns]))

    def _cascading_solve(self, seeds: dict) -> tuple:
        """Solve `seeds` (`{block_index: local rhs}`) and propagate forward.

        A block's own row equations can depend only on already-solved,
        earlier blocks (`BlockStructure.detect` orders blocks consumer
        first: a block's columns may carry entries in the rows of *later*
        blocks only, never the reverse). So one pass through every block in
        that order, always accumulating `-(A[block.rows, :] @ full_supply)`
        from whatever has been solved so far on top of the block's own seed
        (if any), correctly reproduces the full monolithic solve - not just
        for the seeded blocks, but for every block a seed's demand cascades
        into, however many hops away.

        Blocks with neither a seed nor a nonzero cross-term contribution
        are skipped entirely (no solve bought for them), which is the
        common case: a real project's demand touches a handful of a much
        larger set of blocks.

        Returns
        -------
        tuple
            `(full_supply, touched_blocks)`: the supply, dense over every
            technosphere column, and the `frozenset` of block indices that
            were actually solved.
        """
        full_supply = np.zeros(self.technosphere_matrix.shape[1])
        touched_blocks = set()
        for block_index, block in enumerate(self.structure.blocks):
            rhs = -np.asarray(
                self.technosphere_matrix[block.rows, :] @ full_supply
            ).ravel()
            seed = seeds.get(block_index)
            if seed is not None:
                rhs = rhs + seed
            if not np.any(rhs):
                continue
            values = self.solve_block(block_index, rhs)
            full_supply[block.columns] = values
            touched_blocks.add(block_index)
        return full_supply, frozenset(touched_blocks)

    def _local_row(self, block, activity_id) -> int:
        """Position of `activity_id`'s product row within `block.rows`."""
        return int(np.searchsorted(block.rows, self.product_dict[activity_id]))

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

    def prepare_blocks(self, block_indices) -> None:
        """Pre-factorize every block that will be solved more than once.

        The counterpart of `prepare` for callers that solve combined demands
        (`aggregate_for_demand`) rather than per-activity unit LCIs: they know
        which blocks they will hit and how often, but not which activities.
        Same trade as `prepare` - an LU of an ecoinvent-sized block costs
        roughly a hundred solves, so a block solved once must not buy one.
        """
        counts: dict = {}
        for block_index in block_indices:
            counts[block_index] = counts.get(block_index, 0) + 1
        for block_index, count in counts.items():
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
