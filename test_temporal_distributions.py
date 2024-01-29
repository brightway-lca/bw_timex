"""
unit tests for testing if consecutive distributions are fine:
 test 1: test_two_consecutive_TD_in_the_past_direct_match_db
 test 2: test_two_consecutive_TD_in_the_past_interpol_db_no_overlap

"""

import math
import unittest
from bw_temporalis import easy_timedelta_distribution, TemporalDistribution
from edge_extractor import EdgeExtracter
from medusa_tools import *
import bw2data as bd
import bw2calc as bc
import numpy as np


class TestTemporalDistributions(unittest.TestCase):
    
    def test_two_consecutive_TD_in_the_past_direct_match_db(self):
        ''' 
        Test if two consecutive temporal distributions in the past are correctly aggregated
        TD has been selected to match time of the background databases directly, so no interpolation is needed
        Only unitary processes implemented
        
        '''
        
        def prepare_medusa_dbs():
            bd.projects.set_current("__test_medusa__")
            
            bd.Database('temporalis-bio').write({
                ('temporalis-bio', "CO2"): {
                    "type": "emission",
                    "name": "carbon dioxide",
                    "temporalis code": "co2",
                },
                ('temporalis-bio', "CH4"): {
                    "type": "emission",
                    "name": "methane",
                    "temporalis code": "ch4",
                },
            })

            bd.Database('background_2022').write({
                ('background_2022', 'C'): {
                'name': 'process C',
                "location": "somewhere",
                'reference product': 'C',
                'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('background_2022', 'C'),
                        },
                        {
                            'amount': 10,
                            'type': 'biosphere',
                            'input': ('temporalis-bio', 'CO2'),
                        },  ]},
            },

                    )

            bd.Database('background_2020').write({
                ('background_2020', 'C'): {
                'name': 'process C',
                "location": "somewhere",
                'reference product': 'C',
                'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('background_2020', 'C'),
                        },
                        {
                            'amount': 30,
                            'type': 'biosphere',
                            'input': ('temporalis-bio', 'CO2'),
                        },  ]},
            },

                    )

            bd.Database('foreground').write({
                ('foreground', 'A'): {
                    'name': 'process A',
                    "location": "somewhere",
                    'reference product': 'A',
                    'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('foreground', 'A'),
                        },
                        {
                            'amount': 1,
                            'type': 'technosphere',
                            'input': ('foreground', 'B'),
                            'temporal_distribution': TemporalDistribution(
                                np.array([-2, 0], dtype='timedelta64[Y]'),
                                np.array([0.5, 0.5])),  
                        },
                     
                       
                    ]
                },
                ('foreground', 'B'):
                {
                    "name": "process B",
                    "location": "somewhere",
                    'reference product': 'B',
                    "exchanges": [
                        {
                            'amount': 1,
                            'type': 'technosphere',
                            'input': ('background_2022', 'C'),
                            'temporal_distribution': TemporalDistribution(
                                np.array([-2, -0], dtype='timedelta64[Y]'), 
                                np.array([0.5, 0.5])), 
                        },
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('foreground', 'B'), 
                        }
                    ]

                },
                

    })
            
            
            bd.Method(("GWP", "example")).write([
            (("temporalis-bio", "CO2"), 1),
            ])    
            return 
        
        prepare_medusa_dbs()
        bd.projects.set_current("__test_medusa__") # monthly grouping
        bd.databases
        
        demand = {('foreground', 'A'): 1}
        method = ("GWP", "example")
        
        # calculate static LCA'
        slca = bc.LCA(demand, method)
        slca.lci()
        slca.lcia()     
        
        # calculate Medusa LCA
        SKIPPABLE = [node.id for node in bd.Database('background_2022')] + [
                    node.id for node in bd.Database('background_2020')
        ]

        def filter_function(database_id: int) -> bool:
            return database_id in SKIPPABLE
        
        eelca = EdgeExtracter(slca, edge_filter_function=filter_function)
        
        timeline = eelca.build_edge_timeline()
        
        database_date_dict = {
                datetime.strptime("2020-01", "%Y-%m"): 'background_2020',
                datetime.strptime("2022-01", "%Y-%m"): 'background_2022',        
            }

        timeline_df = create_grouped_edge_dataframe(timeline, database_date_dict, temporal_grouping= 'month', interpolation_type="linear") # monthly grouping

        demand_timing_dict = create_demand_timing_dict(timeline_df, demand)

        dp = create_datapackage_from_edge_timeline(timeline_df, database_date_dict, demand_timing_dict)
        
        fu, data_objs, remapping = prepare_medusa_lca_inputs(demand=demand, demand_timing_dict=demand_timing_dict, method=method) 
        
        lca = bc.LCA(fu, data_objs = data_objs + [dp], remapping_dicts=remapping)
        lca.lci()
        lca.lcia()
        
        #calculate expected score:
        expected_score = 0.25 * 30 + 0.5 * 30 + 0.25 * 10   
        
        print('\nTest test two consecutive TD in the past, direct match with db (weights are only 1, no shares):')
        print(timeline_df)
        print('Static LCA Score:', slca.score)
        print('MEDUSA LCA Score:', lca.score)
        print('Expected MEDUSA LCA Score:', expected_score)
        
        
        # Check if the results are equal using math.isclose
        self.assertTrue(math.isclose(lca.score, expected_score, rel_tol=1e-9))
             
    def test_two_consecutive_TD_in_the_past_interpol_db_no_overlap(self):
        ''' 
        Test if two consecutive temporal distributions in the past are correctly aggregated
        TD has been selected to notvmatch time of the background databases directly, so that interpolation between dbs is needed
        Only unitary processes implemented
        
        '''
        
        def prepare_medusa_dbs():
            bd.projects.set_current("__test_tds__")
            
            bd.Database('temporalis-bio').write({
                ('temporalis-bio', "CO2"): {
                    "type": "emission",
                    "name": "carbon dioxide",
                    "temporalis code": "co2",
                },
                ('temporalis-bio', "CH4"): {
                    "type": "emission",
                    "name": "methane",
                    "temporalis code": "ch4",
                },
            })

            bd.Database('background_2022').write({
                ('background_2022', 'C'): {
                'name': 'process C',
                "location": "somewhere",
                'reference product': 'C',
                'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('background_2022', 'C'),
                        },
                        {
                            'amount': 10,
                            'type': 'biosphere',
                            'input': ('temporalis-bio', 'CO2'),
                        },  ]},
            },

                    )

            bd.Database('background_2020').write({
                ('background_2020', 'C'): {
                'name': 'process C',
                "location": "somewhere",
                'reference product': 'C',
                'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('background_2020', 'C'),
                        },
                        {
                            'amount': 30,
                            'type': 'biosphere',
                            'input': ('temporalis-bio', 'CO2'),
                        },  ]},
            },

                    )

            bd.Database('foreground').write({
                ('foreground', 'A'): {
                    'name': 'process A',
                    "location": "somewhere",
                    'reference product': 'A',
                    'exchanges': [
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('foreground', 'A'),
                        },
                        {
                            'amount': 1,
                            'type': 'technosphere',
                            'input': ('foreground', 'B'),
                            'temporal_distribution': TemporalDistribution(
                                np.array([-3, -1], dtype='timedelta64[Y]'),
                                np.array([0.5, 0.5])),  
                        },
                     
                       
                    ]
                },
                ('foreground', 'B'):
                {
                    "name": "process B",
                    "location": "somewhere",
                    'reference product': 'B',
                    "exchanges": [
                        {
                            'amount': 1,
                            'type': 'technosphere',
                            'input': ('background_2022', 'C'),
                            'temporal_distribution': TemporalDistribution(
                                np.array([-1, 0], dtype='timedelta64[Y]'), 
                                np.array([0.5, 0.5])), 
                        },
                        {
                            'amount': 1,
                            'type': 'production',
                            'input': ('foreground', 'B'), 
                        }
                    ]

                },
                

    })
            
            
            bd.Method(("GWP", "example")).write([
            (("temporalis-bio", "CO2"), 1),
            ])    
            return 
        
        prepare_medusa_dbs()
        bd.projects.set_current("__test_tds__")
        bd.databases
        
        demand = {('foreground', 'A'): 1}
        method = ("GWP", "example")
        
        # calculate static LCA'
        slca = bc.LCA(demand, method)
        slca.lci()
        slca.lcia()     
        
        # calculate Medusa LCA
        SKIPPABLE = []
        # [node.id for node in bd.Database('background_2022')] + [
        #             node.id for node in bd.Database('background_2020')
        # ]

        def filter_function(database_id: int) -> bool:
            return database_id in SKIPPABLE
        
        eelca = EdgeExtracter(slca, edge_filter_function=filter_function)
        
        timeline = eelca.build_edge_timeline()
        
        database_date_dict = {
                datetime.strptime("2020-01", "%Y-%m"): 'background_2020',
                datetime.strptime("2022-01", "%Y-%m"): 'background_2022',        
            }

        timeline_df = create_grouped_edge_dataframe(timeline, database_date_dict, temporal_grouping= 'year', interpolation_type="linear")

        demand_timing_dict = create_demand_timing_dict(timeline_df, demand)

        dp = create_datapackage_from_edge_timeline(timeline_df, database_date_dict, demand_timing_dict)
        
        fu, data_objs, remapping = prepare_medusa_lca_inputs(demand=demand, demand_timing_dict=demand_timing_dict, method=method) 
        
        lca = bc.LCA(fu, data_objs = data_objs + [dp], remapping_dicts=remapping)
        lca.lci()
        lca.lcia()
        
        #calculate expected score:
        expected_score = 0.25 * 30 + 0.25 * 0.5 *30 + 0.25 * 0.5 * 10 + 0.25 *10 + 0.25 * 10  # 2020 -> 30, 2021 50:50: 30 & 10, 2022 -> 10, 2023 -> 10
        
        print('\nTest test two consecutive TD in the past, interpolation between dbs:')
        print(timeline_df)
        print('Static LCA Score:', slca.score)
        print('MEDUSA LCA Score:', lca.score)
        print('Expected MEDUSA LCA Score:', expected_score)
        
        techno=pd.DataFrame(lca.technosphere_matrix.toarray())
        matrix_ids= [item[1] for item in lca.dicts.activity.reversed.items()]
        techno.index = matrix_ids
        techno.columns = matrix_ids
        techno.to_csv("techno.csv", sep = ';')
        
        for key in lca.activity_dict:
            if key < 2000:
                print(key, "->",bd.get_activity(key)['name'], bd.get_activity(key)["database"]) #BW does not find the "exploded nodes", because they exist only in the datapackages?
        
        print(techno)
        
        # Check if the results are equal using math.isclose
        self.assertTrue(math.isclose(lca.score, expected_score, rel_tol=1e-9))
     
    """    
    def test_temporal_grouping_years(self):
        # create a simple foreground database with 2 processes A and B, where B consumes C from background database
        # C is produced in background database in 2022 and 2024, with different CO2 emissions
        # A consumes B with a temporal distribution of 1/3, 1/3, 1/3
        # The expected result is a medusa LCA score of 12.5 for A
        # The test checks if the monthly aggregation and LCA score is calculated correctly
        
        def prepare_medusa_dbs():
            bd.projects.set_current("__test_medusa__")

            bd.Database('medusa-bio').write({
                        ('medusa-bio', "CO2"): {
                            "type": "biosphere",
                            "name": "carbon dioxide",
                            "temporalis code": "co2",
                        },
                    },
            )
            
            bd.Database("background_2024").write(
                {
                    ("background_2024", "C"): {
                        'name': 'C',
                        'location': 'somewhere',
                        'reference product': 'C',
                        "exchanges": [
                            {
                                'amount': 1,
                                'type': 'production',
                                'input': ("background_2024", 'C'),
                            },
                            
                            {
                                "amount": 10,
                                "input": ("medusa-bio", "CO2"),
                                "type": "biosphere",
                            },
                        ],       
                    },
                },
                
            )
            
            bd.Database("background_2022").write(
                {
                    ("background_2022", "C"): {
                        'name': 'C',
                        'location': 'somewhere',
                        'reference product': 'C',
                        "exchanges": [
                            {
                                'amount': 1,
                                'type': 'production',
                                'input': ("background_2022", 'C'),
                            },                   
                            {
                                "amount": 15,
                                "input": ("medusa-bio", "CO2"),
                                "type": "biosphere",
                            },
                        ],   
                    },
                },
                
            )
            
            bd.Database("foreground").write(
                {
                    ("foreground", "A"): {
                        'name': 'A',
                        'location': 'somewhere',
                        'reference product': 'A',
                        "exchanges": [
                            {
                                'amount': 1,
                                'type': 'production',
                                'input': ('foreground', 'A'),
                            },                 
                            {
                                "amount": 1,
                                "input": ("foreground", "B"),
                                'temporal_distribution': TemporalDistribution(
                                        np.array([-36, -24, -12, 0], dtype='timedelta64[M]'),
                                        np.array([1/4, 1/4, 1/4, 1/4])),  
                                "type": "technosphere",
                            },
                        ],
                    },
                    
                    ("foreground", "B"): {
                        'name': 'B',
                        'location': 'somewhere',
                        'reference product': 'B',
                        "exchanges": [
                            {
                                'amount': 1,
                                'type': 'production',
                                'input': ('foreground', 'B'),
                            },
                            {
                                "amount": 1,
                                "input": ("background_2024", "C"),
                                "type": "technosphere",
                            },
                        ],
                    },
                }
            )
        
            bd.Method(("GWP", "example")).write([
            (("medusa-bio", "CO2"), 1),
            ])    
            return 
        
        prepare_medusa_dbs()
        bd.projects.set_current("__test_medusa__") # monthly grouping
        bd.databases
        
        demand = {('foreground', 'A'): 1}
        method = ("GWP", "example")
        
        # calculate static LCA'
        slca = bc.LCA(demand, method)
        slca.lci()
        slca.lcia()       
        
        # calculate Medusa LCA
        SKIPPABLE = [node.id for node in bd.Database('background_2022')] + [
                    node.id for node in bd.Database('background_2024')
        ]

        def filter_function(database_id: int) -> bool:
            return database_id in SKIPPABLE
        
        eelca = EdgeExtracter(slca, edge_filter_function=filter_function)
        
        timeline = eelca.build_edge_timeline()
        
        database_date_dict = {
                datetime.strptime("2022-01", "%Y-%m"): 'background_2022',
                datetime.strptime("2024-01", "%Y-%m"): 'background_2024',        
            }

        timeline_df = create_grouped_edge_dataframe(timeline, database_date_dict, temporal_grouping= 'year', interpolation_type="linear") # yearly grouping

        demand_timing_dict = create_demand_timing_dict(timeline_df, demand)

        dp = create_datapackage_from_edge_timeline(timeline_df, database_date_dict, demand_timing_dict)
        
        fu, data_objs, remapping = prepare_medusa_lca_inputs(demand=demand, demand_timing_dict=demand_timing_dict, method=method) 
        
        lca = bc.LCA(fu, data_objs = data_objs + [dp], remapping_dicts=remapping)
        lca.lci()
        lca.lcia()
        
        #calculate expected score:
        expected_score = 1/4*15 + 1/4*15 +1/8*15 + 1/8*10 + 1/4*10
        
        print('\nTest Temporal Grouping Years:')     
        print(timeline_df)
        print('Static LCA Score:', slca.score)
        print('MEDUSA LCA Score:', lca.score)
        print('Expected MEDUSA LCA Score:', expected_score)
        
        
        # Check if the results are equal using math.isclose
        self.assertTrue(math.isclose(lca.score, expected_score, rel_tol=1e-9))
        
        """

if __name__ == '__main__':
    unittest.main()

     

    

