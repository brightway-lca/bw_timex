import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_timex import TemporalDistribution


def _write_same_date_databases(
    with_background_chain: bool = False, years: tuple[str, ...] = ("2020", "2030")
):
    """Write static background databases on the given dates (two by default).

    `background_2020` / `background_2030` hold an untouched `electricity`
    process. `modified_2020` / `modified_2030` hold a copy of `steel` with its
    end-of-life removed, named `steel, without EOL`; they carry the *same*
    dates as the two `background_*` databases. The foreground consumes one of
    each, so both must become temporal markets that interpolate within their
    own family of databases.

    CO2 amounts differ per vintage so the interpolation is visible in the score:
    electricity 10 (2020) / 5 (2030) / 2 (2040), steel 20 (2020) / 10 (2030) /
    5 (2040). Passing a third year via `years` (e.g. `("2020", "2030",
    "2040")`) writes a third `background_*` / `modified_*` pair without
    touching the amounts used by the two-year fixtures, whose scores other
    tests assert.

    With `with_background_chain=True`, each modified database also holds a
    `smelting` process that the copy consumes through a temporal distribution,
    and `smelting` in turn consumes a `coke` process via a plain technosphere
    edge. Both exist only in the modified family and are reached only when the
    background is traversed. `smelting` having its own technosphere input
    (`coke`) matters: `_emit_variant_split` only splits a producer that itself
    has further technosphere inputs, so without `coke`, `smelting` would be a
    technosphere dead end and never reach the per-node routing code at all —
    it would fall through as an ordinary background leaf, resolved entirely by
    `TimelineBuilder`'s (already-correct) temporal-market logic instead.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write({("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}})
    node_co2 = biosphere.get("CO2")

    amounts = {
        "2020": {"electricity": 10, "steel": 20},
        "2030": {"electricity": 5, "steel": 10},
        "2040": {"electricity": 2, "steel": 5},
    }
    coke_co2_amounts = {"2020": 30, "2030": 15, "2040": 8}

    for year in years:
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
            amount=amounts[year]["electricity"],
            type="biosphere",
        ).save()

        steel = background.new_node("steel", name="steel", unit="kg")
        steel["reference product"] = "steel"
        steel["location"] = "GLO"
        steel.save()
        steel.new_edge(input=steel, amount=1, type="production").save()
        steel.new_edge(
            input=node_co2,
            amount=amounts[year]["steel"],
            type="biosphere",
        ).save()

        # The study's own copy of `steel`, without EOL, in its own database.
        steel_copy = steel.copy(code="steel_without_eol", database=f"modified_{year}")
        steel_copy["name"] = "steel, without EOL"
        steel_copy["reference product"] = "steel, without EOL"
        steel_copy.save()

        if with_background_chain:
            # Reached only by descending into the background. The 10-year offset
            # pushes it towards the 2030 vintage of the modified family.
            smelting = modified.new_node("smelting", name="smelting", unit="kg")
            smelting["reference product"] = "smelting"
            smelting["location"] = "GLO"
            smelting.save()
            smelting.new_edge(input=smelting, amount=1, type="production").save()
            smelting.new_edge(
                input=node_co2,
                amount=amounts[year]["steel"],
                type="biosphere",
            ).save()

            copy_to_smelting = steel_copy.new_edge(
                input=smelting, amount=1, type="technosphere"
            )
            copy_to_smelting["temporal_distribution"] = TemporalDistribution(
                date=np.array([10], dtype="timedelta64[Y]"),
                amount=np.array([1.0]),
            )
            copy_to_smelting.save()

            # `smelting`'s own technosphere input, so the split guard
            # (`producer has technosphere inputs`) is satisfied and `smelting`
            # is resolved to its candidate databases via
            # `_candidate_databases_for_node` rather than falling through as a
            # technosphere-dead-end leaf.
            coke = modified.new_node("coke", name="coke", unit="kg")
            coke["reference product"] = "coke"
            coke["location"] = "GLO"
            coke.save()
            coke.new_edge(input=coke, amount=1, type="production").save()
            coke.new_edge(
                input=node_co2,
                amount=coke_co2_amounts[year],
                type="biosphere",
            ).save()
            smelting.new_edge(input=coke, amount=1, type="technosphere").save()

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


@pytest.fixture
@bw2test
def same_date_db():
    """Four static background databases on two dates, no background chain."""
    _write_same_date_databases()


@pytest.fixture
@bw2test
def same_date_deep_db():
    """Same as `same_date_db`, plus a `smelting` -> `coke` chain in the
    modified family."""
    _write_same_date_databases(with_background_chain=True)


@pytest.fixture
@bw2test
def same_date_db_three_dates():
    """Same as `same_date_db`, but with a third `2040` vintage in each
    family, so partial-coverage interpolation across more than two
    candidate dates can be exercised."""
    _write_same_date_databases(years=("2020", "2030", "2040"))
