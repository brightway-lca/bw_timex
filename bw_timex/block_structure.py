"""Block-triangular decomposition of a (time-explicit) technosphere matrix.

A time-explicit technosphere is a handful of new foreground, temporalized and
temporal-market columns sitting on top of several unmodified copies of a
background database. Background processes never consume from the foreground, and
one background vintage never consumes from another, so the matrix is block lower
triangular: solving it as one system factorizes hundreds of thousands of columns
that could have been solved - or skipped, or reused from a previous run - block
by block.

This module finds those blocks. It works on a matrix and a per-column label
(the source database), so it can be tested without Brightway.
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import scipy.sparse as sp
from scipy.sparse.csgraph import connected_components


@dataclass(frozen=True)
class Block:
    """One diagonal block: the columns solved together and their matrix rows."""

    columns: np.ndarray
    rows: np.ndarray
    labels: frozenset


class BlockStructure:
    """Diagonal blocks of a technosphere matrix, in the order they solve.

    Blocks come back consumer first: a block's columns may carry entries in the
    rows of *later* blocks only. Solving therefore goes

    ``A[block.rows][:, block.columns] x_block = d[block.rows] - A[block.rows][:, solved] x_solved``

    for each block in turn, which reproduces the monolithic solve exactly.
    """

    def __init__(self, blocks: list, is_degenerate: bool, n_columns: int):
        self.blocks = blocks
        self.is_degenerate = is_degenerate
        self.n_columns = n_columns

    def __len__(self) -> int:
        return len(self.blocks)

    @classmethod
    def detect(
        cls,
        matrix,
        column_labels: np.ndarray,
        row_labels: Optional[np.ndarray] = None,
    ) -> "BlockStructure":
        """Group the matrix into diagonal blocks by label.

        Parameters
        ----------
        matrix
            The technosphere matrix, square, in any scipy sparse format.
        column_labels
            One label per column, e.g. the database a process comes from.
        row_labels
            One label per row. Defaults to `column_labels`, which is right when
            rows and columns share an index space. Under the explicit
            process/product paradigm a product row can carry a different node id
            than its process column, but both belong to the same database.

        Returns
        -------
        BlockStructure
            Blocks in consumer-first order. Labels that depend on each other in
            both directions are merged into a single block, so a cyclic topology
            stays correct - it just yields a bigger block. If no useful split
            exists, the structure holds one block covering everything and
            `is_degenerate` is True.
        """
        matrix = matrix.tocsc()
        if row_labels is None:
            row_labels = column_labels
        column_labels = np.asarray(column_labels)
        row_labels = np.asarray(row_labels)

        names, group_of_column = np.unique(column_labels, return_inverse=True)
        n_groups = len(names)
        if n_groups < 2:
            return cls._degenerate(matrix)

        # Rows carrying a label that no column has cannot be assigned to a
        # block; that is a matrix we do not understand, so do not split it.
        row_lookup = {name: index for index, name in enumerate(names)}
        if not set(np.unique(row_labels)).issubset(row_lookup):
            return cls._degenerate(matrix)
        group_of_row = np.array([row_lookup[label] for label in row_labels])

        # Which group's rows does each group's columns touch? Vectorized over
        # the nonzeros: a Python loop here costs more than the solve it saves.
        entries_per_column = np.diff(matrix.indptr)
        column_group_per_entry = np.repeat(group_of_column, entries_per_column)
        row_group_per_entry = group_of_row[matrix.indices]
        pairs = np.unique(
            np.stack([column_group_per_entry, row_group_per_entry], axis=1), axis=0
        )
        off_diagonal = pairs[pairs[:, 0] != pairs[:, 1]]

        # "Column group C has entries in row group R" means C consumes from R,
        # so C must be solved before R.
        adjacency = sp.csr_matrix(
            (
                np.ones(len(off_diagonal), dtype=np.int8),
                (off_diagonal[:, 0], off_diagonal[:, 1]),
            ),
            shape=(n_groups, n_groups),
        )
        n_components, component_of_group = connected_components(
            adjacency, directed=True, connection="strong"
        )
        if n_components < 2:
            return cls._degenerate(matrix)

        order = cls._topological_order(
            adjacency, component_of_group, n_components
        )

        blocks = []
        for component in order:
            groups = np.flatnonzero(component_of_group == component)
            columns = np.flatnonzero(np.isin(group_of_column, groups))
            rows = np.flatnonzero(np.isin(group_of_row, groups))
            if len(columns) != len(rows):
                # A block that is not square cannot be solved on its own.
                return cls._degenerate(matrix)
            blocks.append(
                Block(
                    columns=columns,
                    rows=rows,
                    labels=frozenset(names[groups].tolist()),
                )
            )
        return cls(blocks, is_degenerate=False, n_columns=matrix.shape[1])

    @staticmethod
    def _topological_order(adjacency, component_of_group, n_components):
        """Order the condensed graph so consumers come before their suppliers."""
        sources, targets = adjacency.nonzero()
        edges = {
            (component_of_group[source], component_of_group[target])
            for source, target in zip(sources, targets)
            if component_of_group[source] != component_of_group[target]
        }
        successors = {component: set() for component in range(n_components)}
        in_degree = dict.fromkeys(range(n_components), 0)
        for consumer, supplier in edges:
            successors[consumer].add(supplier)
            in_degree[supplier] += 1

        # Deterministic order for equal-priority components keeps results
        # reproducible between runs.
        ready = sorted(c for c, degree in in_degree.items() if degree == 0)
        order = []
        while ready:
            component = ready.pop(0)
            order.append(component)
            for supplier in sorted(successors[component]):
                in_degree[supplier] -= 1
                if in_degree[supplier] == 0:
                    ready.append(supplier)
                    ready.sort()
        # `connected_components(connection="strong")` condenses every cycle, so
        # the condensation is acyclic and every component is emitted.
        return order

    @classmethod
    def _degenerate(cls, matrix) -> "BlockStructure":
        indices = np.arange(matrix.shape[1])
        block = Block(
            columns=indices,
            rows=np.arange(matrix.shape[0]),
            labels=frozenset(),
        )
        return cls([block], is_degenerate=True, n_columns=matrix.shape[1])
