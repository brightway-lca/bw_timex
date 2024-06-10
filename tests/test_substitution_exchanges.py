"""
Testing the results of the static LCA and the timexLCA for a simple test case of electric vehicle to see if the new interpolated amounts are correct.
"""
from pathlib import Path
import pytest
import bw2calc as bc
import bw2data as bd
from bw_timex import TimexLCA
from datetime import datetime
from bw_temporalis import TemporalDistribution
import numpy as np

#from .fixtures.substitution_db_fixture import substitution_db_level1, substitution_db_level2


# make sure the test db is loaded
def test_db_fixture(substitution_db):
    assert len(bd.databases) == 4


# for now, one test class, but could be set up more modularly
@pytest.mark.usefixtures("substitution_db")
class TestClass_substitution:
    
    # @pytest.fixture(autouse=True)
    # def setup_method(self, substitution_db):
        
    #     self.node_a = bd.get_node(database="foreground", code="A")

        
    #     database_date_dict = {
    #         "db_2020": datetime.strptime("2020", "%Y"),
    #         "db_2030": datetime.strptime("2030", "%Y"),
    #         "foreground": "dynamic",  
    #     }

    #     self.tlca = TimexLCA(demand={self.node_a.key: 1}, method=("GWP", "example"), database_date_dict = database_date_dict)

    #     self.tlca.build_timeline()
    #     self.tlca.lci()
    #     self.tlca.static_lcia()

    
    def test_substitution_level1(self):

        bd.projects.set_current("test_substitution_level1")
        bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
            },
        },
        )

        bd.Database("db_2020").write(
            {
                ("db_2020", "D"): {
                    "name": "d",
                    "location": "somewhere",
                    "reference product": "d",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2020", "D"),
                        },
                        {
                            "amount": 0.5,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                },

                ("db_2020", "E"): {
                    "name": "e",
                    "location": "somewhere",
                    "reference product": "e",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2020", "E"),
                        },
                        {
                            "amount": 2,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                }
            }
        )  
        
        bd.Database("db_2030").write(
            {
                ("db_2030", "D"): {
                    "name": "d",
                    "location": "somewhere",
                    "reference product": "d",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2030", "D"),
                        },
                        {
                            "amount": 0.5,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                },

                ("db_2030", "E"): {
                    "name": "e",
                    "location": "somewhere",
                    "reference product": "e",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2030", "E"),
                        },
                        {
                            "amount": 2,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                }
            }
        )

        bd.Database("foreground").write(
            {
                ("foreground", "A"): {
                    "name": "a",
                    "location": "somewhere",
                    "reference product": "a",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "A"),
                        },
                        {
                            "amount": 1,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                        {
                            "amount": 5,  
                            "type": "substitution",
                            "input": ("foreground", "B"),
                            "temporal_distribution": TemporalDistribution(np.array([4], dtype="timedelta64[Y]"), np.array([1])),
                        },

                    ],
                },

                ("foreground", "B"): {
                    "name": "b",
                    "location": "somewhere",
                    "reference product": "b",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "B"),
                        },

                        {
                            "amount": 1,
                            "type": "technosphere",
                            "input": ("db_2020", "D"),
                        },

                        # {
                        #     "amount": 1,
                        #     "type": "substitution",
                        #     "input": ("db_2020", "E"),
                        # },

                        {
                            "amount": 1,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ],
                },

            }
        )

        bd.Method(("GWP", "example")).write(
            [
                (("bio", "CO2"), 1),
            ]
        )


        node_a = bd.get_node(database="foreground", code="A")
        
        database_date_dict = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",  
        }

        tlca = TimexLCA(demand={node_a.key: 1}, method=("GWP", "example"), database_date_dict = database_date_dict)

        tlca.build_timeline()
        tlca.lci()
        tlca.static_lcia()

        expected_substitution_score = (1 + #direct emissions at A                                        
                                    (-5) * 1 + # direct emissions at B
                                    -5 * 1 * 0.5 * 0.8 + # emissions from D in 2028 from db_2030
                                    -5 * 1 * 0.5 * 0.2) # emissions from D in 2028 from db_2020

    
        assert tlca.static_score == expected_substitution_score

    def test_substitution_level2(self):

        bd.projects.set_current("__test_substitution_level2__")
        bd.Database("bio").write(
        {
            ("bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
            },
        },
        )

        bd.Database("db_2020").write(
            {
                ("db_2020", "D"): {
                    "name": "d",
                    "location": "somewhere",
                    "reference product": "d",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2020", "D"),
                        },
                        {
                            "amount": 0.5,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                },

                ("db_2020", "E"): {
                    "name": "e",
                    "location": "somewhere",
                    "reference product": "e",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2020", "E"),
                        },
                        {
                            "amount": 2,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                }
            }
        )  
        
        bd.Database("db_2030").write(
            {
                ("db_2030", "D"): {
                    "name": "d",
                    "location": "somewhere",
                    "reference product": "d",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2030", "D"),
                        },
                        {
                            "amount": 0.5,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                },

                ("db_2030", "E"): {
                    "name": "e",
                    "location": "somewhere",
                    "reference product": "e",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("db_2030", "E"),
                        },
                        {
                            "amount": 2,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ]
                }
            }
        )

        bd.Database("foreground").write(
            {
                ("foreground", "A"): {
                    "name": "a",
                    "location": "somewhere",
                    "reference product": "a",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "A"),
                        },
                        {
                            "amount": 1,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                        {
                            "amount": 5,  
                            "type": "substitution",
                            "input": ("foreground", "B"),
                            "temporal_distribution": TemporalDistribution(np.array([4], dtype="timedelta64[Y]"), np.array([1])),
                        },

                    ],
                },

                ("foreground", "B"): {
                    "name": "b",
                    "location": "somewhere",
                    "reference product": "b",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": ("foreground", "B"),
                        },

                        {
                            "amount": 1,
                            "type": "technosphere",
                            "input": ("db_2020", "D"),
                        },

                        {
                            "amount": 1,
                            "type": "substitution",
                            "input": ("db_2020", "E"),
                        },

                        {
                            "amount": 1,  
                            "type": "biosphere",
                            "input": ("bio", "CO2"),
                        },
                    ],
                },

            }
        )

        bd.Method(("GWP", "example")).write(
            [
                (("bio", "CO2"), 1),
            ]
        )


        node_a = bd.get_node(database="foreground", code="A")
        
        database_date_dict = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",  
        }

        tlca = TimexLCA(demand={node_a.key: 1}, method=("GWP", "example"), database_date_dict = database_date_dict)

        tlca.build_timeline()
        tlca.lci()
        tlca.static_lcia()

        expected_substitution_score = (1 + #direct emissions at A                                        
                                    (-5) * 1 + # direct emissions at B
                                    -5 * 1 * 0.5 * 0.8 + # emissions from D in 2028 from db_2030
                                    -5 * 1 * 0.5 * 0.2 + # emissions from D in 2028 from db_2020
                                    -5 * -1 * 1 * 0.8 + # emissions from E in 2028 from db_2030 (double substitution)
                                    -5 * -1 * 1 * 0.2) # emissions from E in 2028 from db_2020 (double substitution)
   
        assert tlca.static_score == expected_substitution_score


     