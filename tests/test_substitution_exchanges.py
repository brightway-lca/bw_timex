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
    
    @pytest.fixture(autouse=True)
    def setup_method(self):
        
        self.node_a = bd.get_node(database="foreground", code="A")

        
        database_date_dict = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "foreground": "dynamic",  
        }

        self.tlca = TimexLCA(demand={self.node_a.key: 1}, method=("GWP", "example"), database_date_dict = database_date_dict)

        self.tlca.build_timeline()
        self.tlca.lci()
        self.tlca.static_lcia()

    
    def test_substitution(self):
        expected_substitution_score = (1 + #direct emissions at A   
                                    1 * -0.75 * 0.5 + # substituted emissions from Sub via A in 2020 from db_2020                                     
                                    5 * -1 * 0.7 * 0.8 + # substituted emissions from Sub  via B in 2028 from db_2030 
                                    5 * -1 * 0.5 * 0.2)  # substituted emissions from Sub bia B in 2028 from db_2020
        
        print(self.tlca.timeline)
        assert self.tlca.static_score == pytest.approx(expected_substitution_score, rel = 0.5)
                                    
