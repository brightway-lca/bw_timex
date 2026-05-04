from datetime import datetime

import bw2calc as bc
import bw2data as bd
import pytest

from bw_timex import TimexLCA


@pytest.mark.usefixtures("explicit_process_product_db")
class TestExplicitProcessProductEndToEnd:
    def test_static_and_timex_scores_match(self, explicit_process_product_db):
        fu_product = bd.get_node(database="foreground", code="fleet_driving_product")
        demand = {fu_product.key: 1}
        method = ("GWP", "example")

        slca = bc.LCA(demand, method=method)
        slca.lci()
        slca.lcia()

        tlca = TimexLCA(
            demand=demand,
            method=method,
            database_dates=explicit_process_product_db["database_dates"],
        )
        tlca.build_timeline(starting_datetime=datetime(2030, 1, 1))
        tlca.lci()
        tlca.static_lcia()

        assert tlca.base_lca.score == pytest.approx(slca.score)
        assert len(tlca.timeline) > 1
