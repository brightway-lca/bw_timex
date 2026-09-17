"""Tests for the block-triangular decomposition of a technosphere matrix."""

import numpy as np
import scipy.sparse as sp

from bw_timex.block_structure import BlockStructure


def _matrix(entries, n):
    rows, cols, values = zip(*entries)
    return sp.csc_matrix((values, (rows, cols)), shape=(n, n))


def _columns(structure):
    return [sorted(block.columns.tolist()) for block in structure.blocks]


def test_detects_triangular_blocks_in_consumer_first_order():
    # Column 0 ("fg") consumes row 1 ("bg"); "bg" is self-contained.
    matrix = _matrix([(0, 0, 1.0), (1, 0, -0.5), (1, 1, 1.0)], 2)
    labels = np.array(["fg", "bg"])

    structure = BlockStructure.detect(matrix, labels)

    assert _columns(structure) == [[0], [1]]
    assert [sorted(block.rows.tolist()) for block in structure.blocks] == [[0], [1]]
    assert not structure.is_degenerate


def test_merges_mutually_dependent_groups_into_one_block():
    # "fg" consumes "bg" and "bg" consumes "fg": no valid ordering exists, so
    # the two groups have to be solved together.
    matrix = _matrix(
        [(0, 0, 1.0), (1, 0, -0.5), (1, 1, 1.0), (0, 1, -0.25)], 2
    )
    labels = np.array(["fg", "bg"])

    structure = BlockStructure.detect(matrix, labels)

    assert len(structure.blocks) == 1
    assert structure.is_degenerate


def test_independent_background_groups_stay_separate():
    matrix = _matrix(
        [(0, 0, 1.0), (1, 0, -0.5), (2, 0, -0.5), (1, 1, 1.0), (2, 2, 1.0)], 3
    )
    labels = np.array(["fg", "bg2020", "bg2030"])

    structure = BlockStructure.detect(matrix, labels)

    assert len(structure.blocks) == 3
    assert _columns(structure)[0] == [0]
    assert sorted(_columns(structure)[1] + _columns(structure)[2]) == [1, 2]


def test_chained_groups_are_ordered_transitively():
    # fg -> mid -> bg
    matrix = _matrix(
        [
            (0, 0, 1.0),
            (1, 0, -0.5),  # fg consumes mid
            (1, 1, 1.0),
            (2, 1, -0.5),  # mid consumes bg
            (2, 2, 1.0),
        ],
        3,
    )
    labels = np.array(["fg", "mid", "bg"])

    structure = BlockStructure.detect(matrix, labels)

    assert _columns(structure) == [[0], [1], [2]]


def test_row_labels_may_differ_from_column_labels():
    # Explicit process/product paradigm: the product row of a process can carry
    # a different node id, but it belongs to the same database.
    matrix = _matrix([(0, 0, 1.0), (1, 0, -0.5), (1, 1, 1.0)], 2)
    col_labels = np.array(["fg", "bg"])
    row_labels = np.array(["fg", "bg"])

    structure = BlockStructure.detect(matrix, col_labels, row_labels)

    assert _columns(structure) == [[0], [1]]


def test_unbalanced_labels_degenerate_instead_of_producing_bad_blocks():
    # A label with two columns but one row cannot form a square diagonal block.
    matrix = _matrix([(0, 0, 1.0), (1, 1, 1.0), (1, 2, -1.0), (2, 2, 1.0)], 3)
    col_labels = np.array(["fg", "bg", "bg"])
    row_labels = np.array(["fg", "fg", "bg"])

    structure = BlockStructure.detect(matrix, col_labels, row_labels)

    assert structure.is_degenerate
    assert len(structure.blocks) == 1
    assert _columns(structure) == [[0, 1, 2]]


def test_solving_blockwise_reproduces_a_direct_solve():
    rng = np.random.default_rng(0)
    n = 12
    dense = np.eye(n)
    # fg (0-3) consumes bg (4-11); bg only consumes bg.
    dense[4:, :4] -= rng.random((8, 4)) * 0.1
    dense[4:, 4:] -= (rng.random((8, 8)) * 0.05) * (1 - np.eye(8))
    matrix = sp.csc_matrix(dense)
    labels = np.array(["fg"] * 4 + ["bg"] * 8)
    demand = np.zeros(n)
    demand[0] = 1.0

    structure = BlockStructure.detect(matrix, labels)
    supply = np.zeros(n)
    solved = np.zeros(n, dtype=bool)
    for block in structure.blocks:
        rhs = demand[block.rows] - matrix[block.rows][:, solved] @ supply[solved]
        supply[block.columns] = sp.linalg.spsolve(
            matrix[block.rows][:, block.columns].tocsc(), rhs
        )
        solved[block.columns] = True

    expected = sp.linalg.spsolve(matrix, demand)
    assert np.allclose(supply, expected)


def test_repeated_cross_group_entries_give_the_same_blocks():
    # Summed duplicates in the COO input, so the same group pair is seen many
    # times over. The decomposition must not depend on how often it appears.
    entries = [(0, 0, 1.0), (1, 1, 1.0)] + [(1, 0, -0.1)] * 20
    matrix = _matrix(entries, 2)
    labels = np.array(["fg", "bg"])

    structure = BlockStructure.detect(matrix, labels)

    assert _columns(structure) == [[0], [1]]


def test_unsorted_column_indices_still_decompose_correctly():
    matrix = _matrix(
        [(0, 0, 1.0), (2, 0, -0.5), (1, 1, 1.0), (2, 1, -0.5), (2, 2, 1.0)], 3
    )
    for column in range(3):
        start, end = matrix.indptr[column], matrix.indptr[column + 1]
        matrix.indices[start:end] = matrix.indices[start:end][::-1]
        matrix.data[start:end] = matrix.data[start:end][::-1]
    matrix.has_sorted_indices = False
    labels = np.array(["fg", "fg", "bg"])

    structure = BlockStructure.detect(matrix, labels)

    assert _columns(structure) == [[0, 1], [2]]


def test_many_groups_decompose_into_blocks_that_reproduce_a_direct_solve():
    # A wide, randomly linked group graph: the encoding of group pairs has to
    # survive more groups than the small fixtures above exercise.
    rng = np.random.default_rng(0)
    n_groups, per_group = 12, 4
    n = n_groups * per_group
    labels = np.array([f"db{index // per_group}" for index in range(n)])
    dense = np.eye(n)
    for column in range(n):
        group = column // per_group
        # Consume only from later groups, so the system stays triangular.
        for other in range(group + 1, n_groups):
            if rng.random() < 0.3:
                dense[other * per_group + rng.integers(per_group), column] = -0.1
    matrix = sp.csc_matrix(dense)

    structure = BlockStructure.detect(matrix, labels)

    assert len(structure.blocks) == n_groups
    assert not structure.is_degenerate
    demand = np.zeros(n)
    demand[0] = 1.0
    supply = np.zeros(n)
    for block in structure.blocks:
        rhs = demand[block.rows] - np.asarray(matrix[block.rows, :] @ supply)
        supply[block.columns] = sp.linalg.spsolve(
            matrix[block.rows][:, block.columns].tocsc(), rhs
        )
    assert np.allclose(supply, sp.linalg.spsolve(matrix, demand))
