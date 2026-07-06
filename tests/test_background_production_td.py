from datetime import datetime

import pytest

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime(2020, 1, 1),
    "background_2030": datetime(2030, 1, 1),
    "foreground": "dynamic",
}


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_first_level_production_td_conserves(background_prod_td_db, graph_traversal):
    t = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    t.build_timeline(
        starting_datetime="2020-01-01",
        temporal_grouping="year",
        graph_traversal=graph_traversal,
        traverse_background=True,
        cutoff=1e-9,
        max_calc=2000,
    )
    t.lci()
    t.static_lcia()
    assert t.static_score == pytest.approx(t.base_lca.score, rel=1e-6)
