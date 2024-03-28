"""
Testing the results of the static LCA and the MedusaLCA to see if the new interpolated amounts are correct.
"""

import math
import unittest
import bw2data as bd
import bw2calc as bc
import numpy as np
from timex_lca import MedusaLCA
from tests.databases import db_abc_loopA_with_biosphere_tds_CO2_and_CH4
from datetime import datetime


class TestBioflows(unittest.TestCase):

    def test_advanced_bioflows(self):
        import warnings

        warnings.filterwarnings("ignore")

        db_abc_loopA_with_biosphere_tds_CO2_and_CH4()
        database_date_dict = {
            "background_2008": datetime.strptime("2008", "%Y"),
            "background_2024": datetime.strptime("2024", "%Y"),
            "foreground": "dynamic",  # flag databases that should be temporally distributed with "dynamic"
        }
        SKIPPABLE = [node.id for node in bd.Database("background_2008")] + [
            node.id for node in bd.Database("background_2024")
        ]

        def filter_function(database_id: int) -> bool:
            return database_id in SKIPPABLE

        demand = {("foreground", "A"): 1}
        method = ("GWP", "example")

        mlca = MedusaLCA(
            demand, method, filter_function, database_date_dict, max_calc=5000
        )
        mlca.build_timeline()
        mlca.build_datapackage()
        mlca.lci()
        mlca.lcia()
        self.assertTrue(
            math.isclose(mlca.score, mlca.static_lca.score, rel_tol=1e-2),
            f"Total scores didn't match up. Medusa LCA score is {mlca.score}, static LCA score is {mlca.static_lca.score}",
        )

        mlca.build_dynamic_biosphere()
        mlca.calculate_dynamic_biosphere_lci()

        for idx, row in enumerate(mlca.lca.inventory.toarray()):
            bioflow_matrix_id = mlca.lca.dicts.biosphere.reversed[idx]
            bioflow_name = bd.get_node(id=bioflow_matrix_id)["code"]
            self.assertTrue(
                math.isclose(
                    row.sum(),
                    mlca.dynamic_inventory[bioflow_name]["amount"].sum(),
                    rel_tol=1e-7,
                ),
                f"Something didn't match. lca.inventory says {row.sum()} and dynamic_inventory says {mlca.dynamic_inventory[bioflow_name]['amount'].sum()}",
            )


if __name__ == "__main__":
    unittest.main()
