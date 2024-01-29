"""
Testing the results of the static LCA and the MedusaLCA to see if the new interpolated amounts are correct.
"""

import math
import unittest
from bw_temporalis import easy_timedelta_distribution, TemporalDistribution
from edge_extractor import EdgeExtracter
from medusa_tools import *
import bw2data as bd
import bw2calc as bc
import numpy as np


class TestAmounts(unittest.TestCase):
    def test_amounts(self):
        bd.projects.set_current("__test_medusa__")
        bd.Database("temporalis-bio").write(
            {
                ("temporalis-bio", "CO2"): {
                    "type": "emission",
                    "name": "carbon dioxide",
                    "temporalis code": "co2",
                },
            }
        )

        bd.Database("background_2024").write(
            {
                ("background_2024", "electricity_mix"): {
                    "name": "Electricity mix",
                    "location": "somewhere",
                    "reference product": "electricity mix",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("background_2024", "electricity_mix"),
                        },
                        {
                            "amount": 1,
                            "type": "technosphere",
                            "input": ("background_2024", "electricity_wind"),
                        },
                    ],
                },
                ("background_2024", "electricity_wind"): {
                    "name": "Electricity production, wind",
                    "location": "somewhere",
                    "reference product": "electricity, wind",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("background_2024", "electricity_wind"),
                        },
                        {
                            "amount": 0.9,
                            "type": "biosphere",
                            "input": ("temporalis-bio", "CO2"),
                        },
                    ],
                },
            }
        )

        bd.Database("background_2020").write(
            {
                ("background_2020", "electricity_mix"): {
                    "name": "Electricity mix",
                    "location": "somewhere",
                    "reference product": "electricity mix",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("background_2020", "electricity_mix"),
                        },
                        {
                            "amount": 1,
                            "type": "technosphere",
                            "input": ("background_2020", "electricity_wind"),
                        },
                    ],
                },
                ("background_2020", "electricity_wind"): {
                    "name": "Electricity production, wind",
                    "location": "somewhere",
                    "reference product": "electricity, wind",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("background_2020", "electricity_wind"),
                        },
                        {
                            "amount": 1.2,
                            "type": "biosphere",
                            "input": ("temporalis-bio", "CO2"),
                        },
                    ],
                },
            }
        )

        bd.Database("foreground").write(
            {
                ("foreground", "electrolysis"): {
                    "name": "Hydrogen production, electrolysis",
                    "location": "somewhere",
                    "reference product": "hydrogen",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "electrolysis"),
                        },
                        {
                            "amount": 5,
                            "type": "technosphere",
                            "input": ("background_2024", "electricity_mix"),
                            "temporal_distribution": easy_timedelta_distribution(  # e.g. because some hydrogen was stored in the meantime
                                start=-1,
                                end=0,  # Range includes both start and end
                                resolution="Y",  # M for months, Y for years, etc.
                                steps=2,
                            ),
                        },
                    ],
                },
                ("foreground", "heat_from_hydrogen"): {
                    "name": "Heat production, hydrogen",
                    "location": "somewhere",
                    "reference product": "heat",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "heat_from_hydrogen"),
                        },
                        {
                            "amount": 0.7,
                            "type": "technosphere",
                            "input": ("foreground", "electrolysis"),
                            "temporal_distribution": easy_timedelta_distribution(  # e.g. because some hydrogen was stored in the meantime
                                start=-2,
                                end=0,  # Range includes both start and end
                                resolution="Y",  # M for months, Y for years, etc.
                                steps=2,
                            ),
                        },
                    ],
                },
            }
        )

        bd.Method(("GWP", "example")).write(
            [
                (("temporalis-bio", "CO2"), 1),
            ]
        )

        demand = {("foreground", "heat_from_hydrogen"): 1}
        gwp = ("GWP", "example")

        # calculate static LCA'
        slca = bc.LCA(demand, gwp)
        slca.lci()
        slca.lcia()

        # calculate Medusa LCA
        SKIPPABLE = [node.id for node in bd.Database("background_2020")] + [
            node.id for node in bd.Database("background_2024")
        ]

        def filter_function(database_id: int) -> bool:
            return database_id in SKIPPABLE

        eelca = EdgeExtracter(slca, edge_filter_function=filter_function)
        timeline = eelca.build_edge_timeline()

        database_date_dict = {
            datetime.strptime("2020", "%Y"): "background_2020",
            datetime.strptime("2024", "%Y"): "background_2024",
        }

        timeline_df = create_grouped_edge_dataframe(
            timeline, database_date_dict, interpolation_type="linear"
        )

        demand_timing_dict = create_demand_timing_dict(timeline_df, demand)

        dp = create_datapackage_from_edge_timeline(
            timeline_df, database_date_dict, demand_timing_dict
        )

        fu, data_objs, remapping = prepare_medusa_lca_inputs(
            demand=demand, demand_timing_dict=demand_timing_dict, method=gwp
        )
        lca = bc.LCA(fu, data_objs=data_objs + [dp], remapping_dicts=remapping)
        lca.lci()
        lca.lcia()

        # calculate expected score:
        expected_score = 3.4125

        print("Expected score: ", expected_score)
        print("Static LCA score: ", slca.score)
        print("Medusa LCA score: ", lca.score)
        # Check if the results are equal using math.isclose
        self.assertTrue(math.isclose(lca.score, expected_score, rel_tol=1e-1))


if __name__ == "__main__":
    unittest.main()
