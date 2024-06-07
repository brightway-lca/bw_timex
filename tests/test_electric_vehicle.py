"""
Testing the results of the static LCA and the timexLCA for a simple test case of electric vehicle to see if the new interpolated amounts are correct.
"""
from pathlib import Path
import pytest
import bw2calc as bc
import bw2data as bd
from bw_timex import TimexLCA
from datetime import datetime


# make sure the test db is loaded
def test_vehicle_db_fixture(vehicle_db):
    assert len(bd.databases) == 5


# for now, one test class, but could be set up more modularly
@pytest.mark.usefixtures("vehicle_db")
class TestClass_EV:
    
    @pytest.fixture(autouse=True)
    def setup_method(self, vehicle_db):
        self.electric_vehicle = bd.get_node(database="foreground", code="EV")

        
        database_date_dict = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "db_2040": datetime.strptime("2040", "%Y"),
            "foreground": "dynamic",  
        }

        self.tlca = TimexLCA(demand={self.electric_vehicle.key: 1}, method=("GWP", "example"), database_date_dict = database_date_dict)

        self.tlca.build_timeline()
        self.tlca.lci()
        self.tlca.static_lcia()

    
    def test_static_lca_score(self):
        slca = bc.LCA({self.electric_vehicle.key: 1}, method=("GWP", "example"))
        slca.lci()
        slca.lcia()
        expected_static_score = slca.score
        
        assert self.tlca.static_lca.score == expected_static_score

        
    def test_bw_timex_score(self):
        ELECTRICITY_CONSUMPTION = 0.2  # kWh/km
        MILEAGE = 150_000  # km
        LIFETIME = 16  # years

        # Overall mass: 1200 kg
        MASS_GLIDER = 840  # kg
        MASS_POWERTRAIN = 80  # kg
        MASS_BATTERY = 280  # kg

        expected_glider_score = ( 0.6 * MASS_GLIDER * 0.8 * [exc["amount"] for exc in bd.get_activity(("db_2020", "glider")).biosphere()][0] +  # 2022 -> 80% 2020, 20% 2030
                         0.6 * MASS_GLIDER * 0.2 * [exc["amount"] for exc in bd.get_activity(("db_2030", "glider")).biosphere()][0] + 
                         0.4 * MASS_GLIDER * 0.7 * [exc["amount"] for exc in bd.get_activity(("db_2020", "glider")).biosphere()][0] +  # 2023 -> 70% 2020, 30% 2030
                         0.4 * MASS_GLIDER * 0.3 * [exc["amount"] for exc in bd.get_activity(("db_2030", "glider")).biosphere()][0])

        expected_powertrain_score = ( 0.6 * MASS_POWERTRAIN * 0.8 * [exc["amount"] for exc in bd.get_activity(("db_2020", "powertrain")).biosphere()][0] +  # 2022 -> 80% 2020, 20% 2030
                                0.6 * MASS_POWERTRAIN * 0.2 * [exc["amount"] for exc in bd.get_activity(("db_2030", "powertrain")).biosphere()][0] + 
                                0.4 * MASS_POWERTRAIN * 0.7 * [exc["amount"] for exc in bd.get_activity(("db_2020", "powertrain")).biosphere()][0] +  # 2023 -> 70% 2020, 30% 2030
                                0.4 * MASS_POWERTRAIN * 0.3 * [exc["amount"] for exc in bd.get_activity(("db_2030", "powertrain")).biosphere()][0])

        expected_battery_score = ( 0.6 * MASS_BATTERY * 0.8 * [exc["amount"] for exc in bd.get_activity(("db_2020", "battery")).biosphere()][0] +  # 2022 -> 80% 2020, 20% 2030
                                    0.6 * MASS_BATTERY * 0.2 * [exc["amount"] for exc in bd.get_activity(("db_2030", "battery")).biosphere()][0] +
                                    0.4 * MASS_BATTERY * 0.7 * [exc["amount"] for exc in bd.get_activity(("db_2020", "battery")).biosphere()][0] +  # 2023 -> 70% 2020, 30% 2030
                                    0.4 * MASS_BATTERY * 0.3 * [exc["amount"] for exc in bd.get_activity(("db_2030", "battery")).biosphere()][0])

        expected_electricity_score = (MILEAGE * ELECTRICITY_CONSUMPTION * 0.8 * [exc["amount"] for exc in bd.get_activity(("db_2030", "electricity")).biosphere()][0] +  # electricity in year 2032 -> 80% 2030, 20% 2040
                                    MILEAGE * ELECTRICITY_CONSUMPTION * 0.2 * [exc["amount"] for exc in bd.get_activity(("db_2040", "electricity")).biosphere()][0])

        expected_glider_recycling_score = (MASS_GLIDER * [exc["amount"] for exc in bd.get_activity(("db_2040", "dismantling")).biosphere()][0])  # dismantling 2041 -> 100% 2040


        expected_battery_recycling_score = -(MASS_BATTERY * [exc["amount"] for exc in bd.get_activity(("db_2040", "battery_recycling")).biosphere()][0])  # dismantling 2041 -> 100% 2040, negative sign because of waste flow

        expected_timex_score = expected_glider_score + expected_powertrain_score + expected_battery_score + expected_electricity_score + expected_glider_recycling_score + expected_battery_recycling_score
        
        assert self.tlca.static_score == pytest.approx(expected_timex_score, abs = 0.5) #allow for some rounding errors

