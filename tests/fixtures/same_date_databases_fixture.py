import bw2data as bd
import pytest
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def same_date_db():
    """Four static background databases on two dates.

    `background_2020` / `background_2030` hold an untouched `electricity`
    process. `modified_2020` / `modified_2030` hold a copy of `steel` with its
    end-of-life removed, named `steel, without EOL`; they carry the *same*
    dates as the two `background_*` databases. The foreground consumes one of
    each, so both must become temporal markets that interpolate within their
    own family of databases.

    CO2 amounts differ per vintage so the interpolation is visible in the score:
    electricity 10 (2020) / 5 (2030), steel 20 (2020) / 10 (2030).
    """
    biosphere = bd.Database("biosphere")
    biosphere.write({("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}})
    node_co2 = biosphere.get("CO2")

    amounts = {
        "background_2020": {"electricity": 10, "steel": 20},
        "background_2030": {"electricity": 5, "steel": 10},
    }

    for year in ("2020", "2030"):
        background = bd.Database(f"background_{year}")
        background.register()
        modified = bd.Database(f"modified_{year}")
        modified.register()

        electricity = background.new_node("electricity", name="electricity", unit="kWh")
        electricity["reference product"] = "electricity"
        electricity["location"] = "GLO"
        electricity.save()
        electricity.new_edge(input=electricity, amount=1, type="production").save()
        electricity.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["electricity"],
            type="biosphere",
        ).save()

        steel = background.new_node("steel", name="steel", unit="kg")
        steel["reference product"] = "steel"
        steel["location"] = "GLO"
        steel.save()
        steel.new_edge(input=steel, amount=1, type="production").save()
        steel.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["steel"],
            type="biosphere",
        ).save()

        # The study's own copy of `steel`, without EOL, in its own database.
        steel_copy = steel.copy(code="steel_without_eol", database=f"modified_{year}")
        steel_copy["name"] = "steel, without EOL"
        steel_copy["reference product"] = "steel, without EOL"
        steel_copy.save()

    foreground = bd.Database("foreground")
    foreground.register()
    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu["location"] = "GLO"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()
    fu.new_edge(
        input=bd.Database("background_2020").get("electricity"), amount=1, type="technosphere"
    ).save()
    fu.new_edge(
        input=bd.Database("modified_2020").get("steel_without_eol"),
        amount=1,
        type="technosphere",
    ).save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for dbname in bd.databases:
        bd.Database(dbname).process()
