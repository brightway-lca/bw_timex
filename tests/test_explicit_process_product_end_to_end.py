from datetime import datetime

import bw2calc as bc
import bw2data as bd
import numpy as np
import pytest
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


def _write_multilayer_explicit_process_product_db():
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 1, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_final_output = TemporalDistribution(
        date=np.array([0, 2], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    td_component_output = TemporalDistribution(
        date=np.array([0, 1], dtype="timedelta64[Y]"),
        amount=np.array([0.25, 0.75]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "component_product"): {
                "name": "component product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
            ("foreground", "component_process"): {
                "name": "component process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "component_product"),
                        "amount": 1,
                        "type": "production",
                        "temporal_distribution": td_component_output,
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 4,
                        "type": "technosphere",
                    },
                ],
            },
            ("foreground", "final_product"): {
                "name": "final product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
            ("foreground", "final_process"): {
                "name": "final process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "final_product"),
                        "amount": 1,
                        "type": "production",
                        "temporal_distribution": td_final_output,
                    },
                    {
                        "input": ("foreground", "component_product"),
                        "amount": 2,
                        "type": "technosphere",
                    },
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()


def _run_multilayer_explicit_process_product_lca():
    final_product = bd.get_node(database="foreground", code="final_product")
    tlca = TimexLCA(
        demand={final_product.key: 1},
        method=("GWP", "example"),
        database_dates={
            "background": datetime(2030, 1, 1),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(starting_datetime=datetime(2030, 1, 1))
    tlca.lci()
    tlca.static_lcia()
    return tlca


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

        production_rows = tlca.timeline[
            tlca.timeline["producer_name"] == "fleet driving process"
        ]
        assert set(production_rows["date_producer"].dt.year) == {2030, 2031}
        assert sorted(production_rows["amount"].tolist()) == pytest.approx([0.4, 0.6])

        electricity_rows = tlca.timeline[tlca.timeline["producer_name"] == "electricity"]
        assert set(electricity_rows["date_consumer"].dt.year) == {2030, 2031}
        assert tlca.static_score == pytest.approx(slca.score)


def test_explicit_product_output_td_convolves_with_input_td():
    bd.projects.set_current("__test_explicit_product_output_input_td__")
    bd.databases.clear()
    bd.methods.clear()

    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 0.5, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_output = TemporalDistribution(
        date=np.array([0, 2], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )
    td_input = TemporalDistribution(
        date=np.array([0, 1], dtype="timedelta64[Y]"),
        amount=np.array([0.25, 0.75]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "service_process"): {
                "name": "service process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "service_product"),
                        "amount": 1,
                        "type": "production",
                        "temporal_distribution": td_output,
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 8,
                        "type": "technosphere",
                        "temporal_distribution": td_input,
                    },
                ],
            },
            ("foreground", "service_product"): {
                "name": "service product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()

    product = bd.get_node(database="foreground", code="service_product")
    method = ("GWP", "example")
    demand = {product.key: 1}
    database_dates = {
        "background": datetime(2030, 1, 1),
        "foreground": "dynamic",
    }

    slca = bc.LCA(demand, method=method)
    slca.lci()
    slca.lcia()

    tlca = TimexLCA(demand=demand, method=method, database_dates=database_dates)
    tlca.build_timeline(starting_datetime=datetime(2030, 1, 1))
    tlca.lci()
    tlca.static_lcia()

    production_rows = tlca.timeline[
        tlca.timeline["producer_name"] == "service process"
    ].sort_values("date_producer")
    assert production_rows["date_producer"].dt.year.tolist() == [2030, 2032]
    assert production_rows["date_consumer"].dt.year.tolist() == [2030, 2032]
    assert production_rows["amount"].tolist() == pytest.approx([0.6, 0.4])

    input_rows = tlca.timeline[tlca.timeline["producer_name"] == "input production"]
    observed = sorted(
        (
            row.date_consumer.year,
            row.date_producer.year,
            float(row.amount),
        )
        for row in input_rows.itertuples()
    )
    assert observed == pytest.approx(
        [
            (2030, 2030, 2.0),
            (2030, 2031, 6.0),
            (2032, 2032, 2.0),
            (2032, 2033, 6.0),
        ]
    )

    assert tlca.base_lca.score == pytest.approx(slca.score)
    assert tlca.static_score == pytest.approx(4.0)
    assert tlca.static_score == pytest.approx(slca.score)

    service_process = bd.get_node(database="foreground", code="service_process")
    input_edge = next(edge for edge in service_process.technosphere())
    input_edge["temporal_evolution_factors"] = {
        datetime(2030, 1, 1): 1.0,
        datetime(2032, 1, 1): 0.5,
    }
    input_edge["temporal_evolution_reference"] = "consumer"
    input_edge.save()
    bd.Database("foreground").process()

    tlca_with_process_version_evolution = TimexLCA(
        demand=demand,
        method=method,
        database_dates=database_dates,
    )
    tlca_with_process_version_evolution.build_timeline(
        starting_datetime=datetime(2030, 1, 1)
    )
    tlca_with_process_version_evolution.lci()
    tlca_with_process_version_evolution.static_lcia()

    evolved_rows = tlca_with_process_version_evolution.timeline[
        tlca_with_process_version_evolution.timeline["producer_name"] == "input production"
    ]
    assert set(evolved_rows["date_consumer"].dt.year) == {2030, 2032}
    assert set(evolved_rows["temporal_evolution_reference"]) == {"consumer"}
    # Cohort 2030 (weight 0.6, cop 1.0) → 0.6 * 8 * 1.0 * 0.5 = 2.4
    # Cohort 2032 (weight 0.4, cop 0.5) → 0.4 * 8 * 0.5 * 0.5 = 0.8
    assert tlca_with_process_version_evolution.static_score == pytest.approx(3.2)


def test_multilayer_explicit_process_product_chain_with_evolution():
    bd.projects.set_current("__test_multilayer_explicit_process_product_chain__")
    bd.databases.clear()
    bd.methods.clear()
    _write_multilayer_explicit_process_product_db()

    tlca = _run_multilayer_explicit_process_product_lca()

    final_rows = tlca.timeline[
        tlca.timeline["producer_name"] == "final process"
    ].sort_values("date_producer")
    assert final_rows["date_producer"].dt.year.tolist() == [2030, 2032]
    assert final_rows["date_consumer"].dt.year.tolist() == [2030, 2032]
    assert final_rows["amount"].tolist() == pytest.approx([0.6, 0.4])

    component_rows = tlca.timeline[
        tlca.timeline["producer_name"] == "component process"
    ]
    observed_component_timeline = sorted(
        (
            row.date_consumer.year,
            row.date_producer.year,
            float(row.amount),
        )
        for row in component_rows.itertuples()
    )
    assert observed_component_timeline == pytest.approx(
        [
            (2030, 2030, 0.5),
            (2030, 2031, 1.5),
            (2032, 2032, 0.5),
            (2032, 2033, 1.5),
        ]
    )

    background_rows = tlca.timeline[
        tlca.timeline["producer_name"] == "input production"
    ]
    observed_background_timeline = sorted(
        (
            row.date_consumer.year,
            row.date_producer.year,
            float(row.amount),
        )
        for row in background_rows.itertuples()
    )
    assert observed_background_timeline == pytest.approx(
        [
            (2030, 2030, 4.0),
            (2031, 2031, 4.0),
            (2032, 2032, 4.0),
            (2033, 2033, 4.0),
        ]
    )
    assert tlca.static_score == pytest.approx(8.0)

    final_process = bd.get_node(database="foreground", code="final_process")
    component_edge = next(
        edge
        for edge in final_process.technosphere()
        if edge.input["code"] == "component_product"
    )
    component_edge["temporal_evolution_factors"] = {datetime(2030, 1, 1): 0.5}
    component_edge["temporal_evolution_reference"] = "consumer"
    component_edge.save()
    bd.Database("foreground").process()

    evolved_tlca = _run_multilayer_explicit_process_product_lca()
    evolved_component_rows = evolved_tlca.timeline[
        evolved_tlca.timeline["producer_name"] == "component process"
    ]
    assert sorted(
        (
            row.date_consumer.year,
            row.date_producer.year,
            float(row.amount),
        )
        for row in evolved_component_rows.itertuples()
    ) == pytest.approx(observed_component_timeline)
    assert set(evolved_component_rows["temporal_evolution_reference"]) == {"consumer"}
    assert evolved_tlca.static_score == pytest.approx(4.0)


def test_multi_vintage_demand_splits_across_cohorts():
    """Two install-year cohorts each draw from a different background database.

    With install-year RTD = [2025: 0.5, 2035: 0.5] and the use-phase happening
    at the install year, cohort 2025 should source electricity from
    `electricity_market_2025` and cohort 2035 from `electricity_market_2035`.
    The fleet score must therefore be the demand-weighted mix of both
    backgrounds, not just one of them.
    """
    bd.projects.set_current("__test_multi_vintage_demand_split__")
    bd.databases.clear()
    bd.methods.clear()

    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    for db_name, co2_per_kwh in [
        ("electricity_market_2025", 0.40),
        ("electricity_market_2035", 0.10),
    ]:
        bd.Database(db_name).write(
            {
                (db_name, "elec"): {
                    "name": "electricity market",
                    "reference product": "electricity",
                    "location": "DE",
                    "unit": "kWh",
                    "exchanges": [
                        {
                            "amount": 1,
                            "type": "production",
                            "input": (db_name, "elec"),
                        },
                        {
                            "amount": co2_per_kwh,
                            "type": "biosphere",
                            "input": ("bio", "co2"),
                        },
                    ],
                },
            }
        )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_install = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.5]),
    )
    td_use_at_install = TemporalDistribution(
        date=np.array([0], dtype="timedelta64[Y]"),
        amount=np.array([1.0]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "unit_product"): {
                "name": "unit product",
                "type": "product",
                "unit": "unit",
                "location": "DE",
                "exchanges": [],
            },
            ("foreground", "unit_lifecycle"): {
                "name": "unit lifecycle",
                "type": "process",
                "unit": "unit",
                "location": "DE",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "unit_product"),
                        "temporal_distribution": td_install,
                    },
                    {
                        "amount": 1.0,
                        "type": "technosphere",
                        "input": ("electricity_market_2025", "elec"),
                        "temporal_distribution": td_use_at_install,
                    },
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()

    product = bd.get_node(database="foreground", code="unit_product")
    database_dates = {
        "electricity_market_2025": datetime(2025, 1, 1),
        "electricity_market_2035": datetime(2035, 1, 1),
        "foreground": "dynamic",
    }
    tlca = TimexLCA(
        demand={product.key: 1},
        method=("GWP", "example"),
        database_dates=database_dates,
    )
    tlca.build_timeline(
        starting_datetime=datetime(2025, 1, 1),
        temporal_grouping="year",
    )
    tlca.lci()
    tlca.static_lcia()

    fu_rows = tlca.timeline[tlca.timeline["consumer"] == -1].sort_values(
        "date_producer"
    )
    assert fu_rows["date_producer"].dt.year.tolist() == [2025, 2035]
    assert fu_rows["amount"].tolist() == pytest.approx([0.5, 0.5])

    elec_rows = tlca.timeline[
        tlca.timeline["producer_name"] == "electricity market"
    ].sort_values("date_consumer")
    assert elec_rows["date_consumer"].dt.year.tolist() == [2025, 2035]
    assert [
        row.temporal_market_shares for row in elec_rows.itertuples()
    ] == [
        {"electricity_market_2025": 1.0},
        {"electricity_market_2035": 1.0},
    ]

    assert tlca.static_score == pytest.approx(0.5 * 0.40 + 0.5 * 0.10)


def test_explicit_output_coefficient_alpha_scales_inputs():
    """Production output coefficient alpha != 1 must scale the supply chain by 1/alpha.

    A process produces *2* units of its product per invocation and consumes 4
    units of background input. Demanding 1 unit of the product therefore runs
    the process at scale 1/2, so the input is consumed at 0.5 * 4 = 2.0 and the
    score is 2.0 (background = 1 kg CO2 per input unit, GWP = 1).

    Every existing explicit fixture uses production amount = 1, so the 1/alpha
    division is otherwise never exercised: a bug there would be invisible.
    """
    bd.projects.set_current("__test_explicit_alpha_scaling__")
    bd.databases.clear()
    bd.methods.clear()

    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "input production",
                "reference product": "input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 1, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_output = TemporalDistribution(
        date=np.array([0, 1], dtype="timedelta64[Y]"),
        amount=np.array([0.6, 0.4]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "product"): {
                "name": "product",
                "type": "product",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [],
            },
            ("foreground", "process"): {
                "name": "process",
                "type": "process",
                "unit": "unit",
                "location": "GLO",
                "exchanges": [
                    {
                        "input": ("foreground", "product"),
                        "amount": 2,  # alpha != 1
                        "type": "production",
                        "temporal_distribution": td_output,
                    },
                    {
                        "input": ("background", "input"),
                        "amount": 4,
                        "type": "technosphere",
                    },
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()

    product = bd.get_node(database="foreground", code="product")
    method = ("GWP", "example")
    demand = {product.key: 1}
    database_dates = {"background": datetime(2030, 1, 1), "foreground": "dynamic"}

    slca = bc.LCA(demand, method=method)
    slca.lci()
    slca.lcia()
    # Sanity: bw2calc itself applies 1/alpha -> 0.5 * 4 * 1 = 2.0
    assert slca.score == pytest.approx(2.0)

    tlca = TimexLCA(demand=demand, method=method, database_dates=database_dates)
    tlca.build_timeline(starting_datetime=datetime(2030, 1, 1))
    tlca.lci()
    tlca.static_lcia()

    assert tlca.base_lca.score == pytest.approx(2.0)
    assert tlca.static_score == pytest.approx(2.0)

    # The per-cohort-instance input intensity carries the 1/alpha scaling:
    # edge amount 4 / alpha 2 = 2.0 per cohort row (the cohort TD weights 0.6/0.4
    # are applied to the process column in the matrix, summing to 1, so the total
    # input is 0.6*2 + 0.4*2 = 2.0 and the score is 2.0). If 1/alpha were dropped
    # the row amount would be 4.0; if mis-applied, 8.0.
    input_rows = tlca.timeline[tlca.timeline["producer_name"] == "input production"]
    observed = sorted(
        (row.date_consumer.year, row.date_producer.year, float(row.amount))
        for row in input_rows.itertuples()
    )
    assert observed == pytest.approx([(2030, 2030, 2.0), (2031, 2031, 2.0)])


def _write_explicit_two_background_cohort_db():
    """Explicit process/product with a cohort production TD whose two cohorts
    resolve to two different background databases. If the production-edge TD is
    dropped, both cohorts collapse onto one background and the score changes -
    so this fixture is sensitive to correct production-edge handling.
    """
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    for db_name, co2_per_kwh in [
        ("electricity_market_2025", 0.40),
        ("electricity_market_2035", 0.10),
    ]:
        bd.Database(db_name).write(
            {
                (db_name, "elec"): {
                    "name": "electricity market",
                    "reference product": "electricity",
                    "location": "DE",
                    "unit": "kWh",
                    "exchanges": [
                        {"amount": 1, "type": "production", "input": (db_name, "elec")},
                        {"amount": co2_per_kwh, "type": "biosphere", "input": ("bio", "co2")},
                    ],
                },
            }
        )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    td_install = TemporalDistribution(
        date=np.array([0, 10], dtype="timedelta64[Y]"),
        amount=np.array([0.5, 0.5]),
    )
    td_use_at_install = TemporalDistribution(
        date=np.array([0], dtype="timedelta64[Y]"),
        amount=np.array([1.0]),
    )

    bd.Database("foreground").write(
        {
            ("foreground", "unit_product"): {
                "name": "unit product",
                "type": "product",
                "unit": "unit",
                "location": "DE",
                "exchanges": [],
            },
            ("foreground", "unit_lifecycle"): {
                "name": "unit lifecycle",
                "type": "process",
                "unit": "unit",
                "location": "DE",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "unit_product"),
                        "temporal_distribution": td_install,
                    },
                    {
                        "amount": 1.0,
                        "type": "technosphere",
                        "input": ("electricity_market_2025", "elec"),
                        "temporal_distribution": td_use_at_install,
                    },
                ],
            },
        }
    )
    for db in bd.databases:
        bd.Database(db).process()


def test_explicit_bfs_traversal_matches_priority():
    """Explicit graphs must traverse correctly under graph_traversal='bfs' too.

    Both EdgeExtractor variants (priority + BFS) need explicit-node support, and
    BFS has regressed on temporal data before (see fix-bfs-with-evo-factors).
    Two background databases make the score sensitive to the cohort split: if
    the production-edge TD is dropped, both cohorts collapse onto the 2025
    background (score 0.40) instead of the 0.5/0.5 mix (score 0.25).
    """
    bd.projects.set_current("__test_explicit_bfs_traversal__")
    bd.databases.clear()
    bd.methods.clear()
    _write_explicit_two_background_cohort_db()

    product = bd.get_node(database="foreground", code="unit_product")
    method = ("GWP", "example")
    demand = {product.key: 1}
    database_dates = {
        "electricity_market_2025": datetime(2025, 1, 1),
        "electricity_market_2035": datetime(2035, 1, 1),
        "foreground": "dynamic",
    }

    def score(graph_traversal):
        tlca = TimexLCA(demand=demand, method=method, database_dates=database_dates)
        tlca.build_timeline(
            starting_datetime=datetime(2025, 1, 1),
            temporal_grouping="year",
            graph_traversal=graph_traversal,
        )
        tlca.lci()
        tlca.static_lcia()
        return tlca.static_score

    priority_score = score("priority")
    assert priority_score == pytest.approx(0.5 * 0.40 + 0.5 * 0.10)
    assert score("bfs") == pytest.approx(priority_score)
