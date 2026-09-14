"""Batched, multiple-RHS background solving."""

import bw2data as bd
import numpy as np
import pytest
import scipy.sparse as sp

from .test_background_solver import (  # noqa: F401
    _setup,
    two_background_activities_db,
)


@pytest.mark.usefixtures("two_background_activities_db")
class TestCascadingSolveWithMatrixRHS:

    def test_matrix_cascade_matches_column_by_column(self):
        lca, structure, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        c2 = bd.get_node(database="db_2020", code="C2")
        block_index = solver.block_index_for(c1.id)
        block = structure.blocks[block_index]

        seed = np.zeros((len(block.rows), 2))
        seed[solver._local_row(block, c1.id), 0] = 1.0
        seed[solver._local_row(block, c2.id), 1] = 1.0

        batched, touched = solver._cascading_solve({block_index: seed})

        assert batched.shape == (lca.technosphere_matrix.shape[1], 2)
        for j, node in enumerate((c1, c2)):
            demand = np.zeros(lca.technosphere_matrix.shape[0])
            demand[lca.dicts.product[node.id]] = 1
            expected = sp.linalg.spsolve(lca.technosphere_matrix.tocsc(), demand)
            assert np.allclose(batched[:, j], expected)

    def test_one_dimensional_cascade_still_returns_a_vector(self):
        lca, structure, solver = _setup()
        c1 = bd.get_node(database="db_2020", code="C1")
        block_index = solver.block_index_for(c1.id)
        block = structure.blocks[block_index]
        seed = np.zeros(len(block.rows))
        seed[solver._local_row(block, c1.id)] = 1.0

        values, _ = solver._cascading_solve({block_index: seed})

        assert values.ndim == 1
