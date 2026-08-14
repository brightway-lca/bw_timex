"""Several exchanges between the same pair of nodes are legitimate modelling
(ecoinvent/premise do it routinely). Traversing them must not raise, and the
duplicates must be merged into one edge that carries their combined amount."""

from datetime import datetime

import numpy as np
import pytest

from bw_timex import TemporalDistribution, TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
STARTING_DATETIME = "2024-01-01"

# Linear interpolation weights of the 2024 flows between the 2020 and 2030 db
# (rounded to 3 decimals, like `linear_interpolation_weights` does).
W_2030 = round(
    (datetime(2024, 1, 1) - datetime(2020, 1, 1)).days
    / (datetime(2030, 1, 1) - datetime(2020, 1, 1)).days,
    3,
)
W_2020 = 1 - W_2030
# CO2 per unit of bg_B, per background vintage
CO2_2020, CO2_2030 = 1.0, 0.5


def _score(graph_traversal, traverse_background):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime=STARTING_DATETIME,
        graph_traversal=graph_traversal,
        traverse_background=traverse_background,
    )
    tlca.lci()
    tlca.static_lcia()
    return tlca.static_score


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_duplicate_exchanges_are_summed(duplicate_exchange_db, graph_traversal):
    """bg_A consumes bg_B twice (2 + 3 kg). Traversing the background must see
    the full 5 kg, and give the same score as not traversing it."""
    expected = 5 * (W_2020 * CO2_2020 + W_2030 * CO2_2030)
    assert _score(graph_traversal, True) == pytest.approx(expected, rel=1e-9)
    assert _score(graph_traversal, False) == pytest.approx(expected, rel=1e-9)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_self_consuming_process_from_timeline(self_loop_db, graph_traversal):
    """Without an expanded matrix there is no solve to resolve the loop: the
    traversal walks it until `cutoff` stops it, and the repeated visits carry
    the amplification. It must converge to the same 1.25."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime=STARTING_DATETIME,
        graph_traversal=graph_traversal,
        traverse_background=True,
        cutoff=1e-8,
    )
    tlca.lci(expand_technosphere=False, build_dynamic_biosphere=True)
    tlca.static_lcia()
    assert tlca.static_score == pytest.approx(1 / 0.8, rel=1e-6)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
@pytest.mark.parametrize("traverse_background", [False, True])
def test_self_consuming_process(self_loop_db, graph_traversal, traverse_background):
    """A process consuming its own product has a production and a technosphere
    exchange between the same node pair. Loops like this are everywhere in LCI
    databases: the loop must survive into the matrix, where 1 kg of net product
    takes 1/(1 - 0.2) process runs."""
    expected = 1 / 0.8  # kg CO2 per kg of net product delivered
    assert _score(graph_traversal, traverse_background) == pytest.approx(
        expected, rel=1e-6
    )


def test_differently_typed_exchanges_with_temporal_information_raise():
    """With a temporal distribution on one of them, the two exchanges would
    have to be merged in time across incompatible signs. That is ambiguous, so
    it must fail loudly rather than silently pick one."""
    from bw_timex.edge_extractor import merge_duplicate_exchanges

    with pytest.raises(ValueError, match="different types"):
        merge_duplicate_exchanges(
            [
                {"amount": 1, "type": "production"},
                {
                    "amount": 0.2,
                    "type": "technosphere",
                    "temporal_distribution": TemporalDistribution(
                        date=np.array([-2], dtype="timedelta64[Y]"),
                        amount=np.array([1.0]),
                    ),
                },
            ]
        )


def test_differently_typed_exchanges_are_netted():
    """Without temporal information the duplicates are netted: the merged
    exchange reports the type governing the net, and keeps the consumed amount
    for a traversal that follows the input edge."""
    from bw_timex.edge_extractor import merge_duplicate_exchanges

    merged = merge_duplicate_exchanges(
        [
            {"amount": 1, "type": "production"},
            {"amount": 0.2, "type": "technosphere"},
        ]
    )
    assert merged.data["type"] == "production"
    assert merged.data["amount"] == pytest.approx(0.8)
    assert merged.netted_types is True
    assert merged.consuming_amount == pytest.approx(0.2)


@pytest.mark.parametrize("graph_traversal", ["priority", "bfs"])
def test_duplicate_exchanges_merge_temporal_distributions(
    duplicate_exchange_td_db, graph_traversal
):
    """Only the amount-2 duplicate carries a TD (+10 years), so 2 kg of bg_B
    happen in 2034 (sourced from the 2030 db) and 3 kg in 2024 (interpolated)."""
    expected = 3 * (W_2020 * CO2_2020 + W_2030 * CO2_2030) + 2 * CO2_2030
    assert _score(graph_traversal, True) == pytest.approx(expected, rel=1e-9)
