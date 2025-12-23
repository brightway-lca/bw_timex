import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


@pytest.fixture
@bw2test
def process_at_base_database_time_db():
    biosphere = bd.Database("biosphere")
    biosphere.write(
        {
            ("biosphere", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
            },
        }
    )

    foreground = bd.Database("foreground")
    foreground.register()

    background_2020 = bd.Database("background_2020")
    background_2020.register()

    background_2030 = bd.Database("background_2030")
    background_2030.register()

    node_co2 = biosphere.get("CO2")

    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu.save()

    first_level_input = foreground.new_node(
        "first_level_input", name="first_level_input", unit="unit"
    )
    first_level_input["reference product"] = "first_level_input"
    first_level_input.save()

    background_process_2020 = background_2020.new_node(
        "background_process", name="background_process", unit="background_process"
    )
    background_process_2020["reference product"] = "background_process"
    background_process_2020.save()

    background_process_2030 = background_2030.new_node(
        "background_process", name="background_process", unit="background_process"
    )
    background_process_2030["reference product"] = "background_process"
    background_process_2030.save()

    fu.new_edge(input=fu, amount=1, type="production").save()
    first_level_input_to_fu = fu.new_edge(
        input=first_level_input, amount=1, type="technosphere"
    )

    first_level_input.new_edge(
        input=first_level_input, amount=1, type="production"
    ).save()
    background_process_2020_to_first_level_input = first_level_input.new_edge(
        input=background_process_2020, amount=840, type="technosphere"
    )

    background_process_2020.new_edge(
        input=background_process_2020, amount=1, type="production"
    ).save()
    background_process_2020.new_edge(input=node_co2, amount=1, type="biosphere").save()

    background_process_2030.new_edge(
        input=background_process_2030, amount=1, type="production"
    ).save()
    background_process_2030.new_edge(input=node_co2, amount=1, type="biosphere").save()

    td_first_level_input_to_fu = TemporalDistribution(
        date=np.array([-2, -1], dtype="timedelta64[Y]"), amount=np.array([0.2, 0.8])
    )

    td_background_process_2020_to_first_level_input = TemporalDistribution(
        date=np.array([-3, -2], dtype="timedelta64[Y]"), amount=np.array([0.4, 0.6])
    )

    first_level_input_to_fu["temporal_distribution"] = td_first_level_input_to_fu
    first_level_input_to_fu.save()

    background_process_2020_to_first_level_input["temporal_distribution"] = (
        td_background_process_2020_to_first_level_input
    )
    background_process_2020_to_first_level_input.save()

    bd.Method(("GWP", "example")).write(
        [
            (("biosphere", "CO2"), 1),
        ]
    )

    for db in bd.databases:
        bd.Database(db).process()
