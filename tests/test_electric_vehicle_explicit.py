"""
Mirror of ``test_electric_vehicle.py`` using the explicit process/product
paradigm instead of the chimaera node convention. Verifies that demanding a
bare product node and routing all edges through a separate process node
yields the same TimexLCA static score as the chimaera-modeled equivalent.
"""

from datetime import datetime

import bw2calc as bc
import bw2data as bd
import pytest

from bw_timex import TimexLCA


def test_vehicle_explicit_db_fixture(vehicle_explicit_db):
    assert len(bd.databases) == 5


@pytest.mark.usefixtures("vehicle_explicit_db")
class TestClass_EV_Explicit:

    @pytest.fixture(autouse=True)
    def setup_method(self, vehicle_explicit_db):
        self.ev_product = bd.get_node(database="foreground", code="EV_product")

        database_dates = {
            "db_2020": datetime.strptime("2020", "%Y"),
            "db_2030": datetime.strptime("2030", "%Y"),
            "db_2040": datetime.strptime("2040", "%Y"),
            "foreground": "dynamic",
        }

        self.tlca = TimexLCA(
            demand={self.ev_product.key: 1},
            method=("GWP", "example"),
            database_dates=database_dates,
        )

        self.tlca.build_timeline(
            starting_datetime=datetime.strptime("2024-01-02", "%Y-%m-%d")
        )
        self.tlca.lci()
        self.tlca.static_lcia()

    def test_base_lca_score(self):
        slca = bc.LCA({self.ev_product.key: 1}, method=("GWP", "example"))
        slca.lci()
        slca.lcia()
        assert self.tlca.base_lca.score == slca.score

    def test_bw_timex_score_matches_chimaera_manual(self):
        # Identical hand-computation to test_electric_vehicle.py — the
        # explicit product/process model must produce the same temporal
        # interpolation result as the chimaera node.
        ELECTRICITY_CONSUMPTION = 0.2
        MILEAGE = 150_000
        MASS_GLIDER = 840
        MASS_POWERTRAIN = 80
        MASS_BATTERY = 280

        glider_2020 = [
            exc["amount"] for exc in bd.get_activity(("db_2020", "glider")).biosphere()
        ][0]
        glider_2030 = [
            exc["amount"] for exc in bd.get_activity(("db_2030", "glider")).biosphere()
        ][0]
        powertrain_2020 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2020", "powertrain")).biosphere()
        ][0]
        powertrain_2030 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2030", "powertrain")).biosphere()
        ][0]
        battery_2020 = [
            exc["amount"] for exc in bd.get_activity(("db_2020", "battery")).biosphere()
        ][0]
        battery_2030 = [
            exc["amount"] for exc in bd.get_activity(("db_2030", "battery")).biosphere()
        ][0]
        elec_2030 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2030", "electricity")).biosphere()
        ][0]
        elec_2040 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2040", "electricity")).biosphere()
        ][0]
        dismantling_2040 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2040", "dismantling")).biosphere()
        ][0]
        battery_recycling_2040 = [
            exc["amount"]
            for exc in bd.get_activity(("db_2040", "battery_recycling")).biosphere()
        ][0]

        # 2022 -> 80% 2020, 20% 2030; 2023 -> 70% 2020, 30% 2030
        expected_glider_score = (
            0.6 * MASS_GLIDER * (0.8 * glider_2020 + 0.2 * glider_2030)
            + 0.4 * MASS_GLIDER * (0.7 * glider_2020 + 0.3 * glider_2030)
        )
        expected_powertrain_score = (
            0.6 * MASS_POWERTRAIN * (0.8 * powertrain_2020 + 0.2 * powertrain_2030)
            + 0.4 * MASS_POWERTRAIN * (0.7 * powertrain_2020 + 0.3 * powertrain_2030)
        )
        expected_battery_score = (
            0.6 * MASS_BATTERY * (0.8 * battery_2020 + 0.2 * battery_2030)
            + 0.4 * MASS_BATTERY * (0.7 * battery_2020 + 0.3 * battery_2030)
        )
        # electricity in 2032 -> 80% 2030, 20% 2040
        expected_electricity_score = (
            MILEAGE * ELECTRICITY_CONSUMPTION * (0.8 * elec_2030 + 0.2 * elec_2040)
        )
        # dismantling 2041 -> 100% 2040
        expected_glider_recycling_score = MASS_GLIDER * dismantling_2040
        expected_battery_recycling_score = -(MASS_BATTERY * battery_recycling_2040)

        expected_timex_score = (
            expected_glider_score
            + expected_powertrain_score
            + expected_battery_score
            + expected_electricity_score
            + expected_glider_recycling_score
            + expected_battery_recycling_score
        )

        assert self.tlca.static_score == pytest.approx(expected_timex_score, abs=0.5)
