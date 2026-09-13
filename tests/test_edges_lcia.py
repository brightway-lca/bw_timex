"""Tests for the `edges` integration (TimexLCA.edges_lcia)."""

import importlib
import sys
from datetime import datetime

import bw2data as bd
import pytest
from pydantic import ValidationError


def test_missing_edges_gives_actionable_import_error(monkeypatch):
    """Without `edges` installed, importing the adapter names the extra and the Python ceiling."""
    monkeypatch.setitem(sys.modules, "edges", None)
    monkeypatch.delitem(sys.modules, "bw_timex.edges_lcia", raising=False)

    with pytest.raises(ImportError) as exc_info:
        importlib.import_module("bw_timex.edges_lcia")

    message = str(exc_info.value)
    assert "bw_timex[edges]" in message
    assert "3.13" in message


def test_edges_lcia_inputs_defaults():
    from bw_timex.validation import EdgesLCIAInputs

    inputs = EdgesLCIAInputs(method=("some", "method"))

    assert inputs.weight == "population"
    assert inputs.regionalized is True
    assert inputs.use_disaggregated_lci is False
    assert inputs.parameters is None


def test_edges_lcia_inputs_rejects_empty_method():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method=())


def test_edges_lcia_inputs_rejects_dict_without_exchanges():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method={"name": "no exchanges here"})


def test_fixture_emits_in_two_calendar_years(edges_td_db):
    """The biosphere TD must land emissions in two years, otherwise later tests prove nothing."""
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci()

    emissions = timex_lca.dynamic_inventory_df
    years = sorted(emissions.date.dt.year.unique())

    # (a) the dynamic inventory spans at least two distinct calendar years.
    assert years == [2024, 2025]

    # (b) the foreground process's own CO2 emission (as opposed to the background
    # electricity's CO2, which the timeline places at the process's 2024 time) splits
    # 4.0 kg / 6.0 kg across the two consecutive years 2024/2025. `activity_time_mapping`
    # maps every time-mapped activity id back to its (database, code) pair; the foreground
    # "heat" process keeps that code whether it is looked up as the original "foreground"
    # node or as a "temporalized" copy the timeline creates for it.
    foreground_activity_ids = {
        time_mapped_id
        for time_mapped_id, ((_, code), _) in timex_lca.activity_time_mapping.reversed.items()
        if code == "heat"
    }
    foreground_emissions = emissions[emissions.activity.isin(foreground_activity_ids)]
    per_year = foreground_emissions.groupby(foreground_emissions.date.dt.year).amount.sum()

    assert per_year.loc[2024] == pytest.approx(4.0)
    assert per_year.loc[2025] == pytest.approx(6.0)


@pytest.fixture
def timex_lca_with_lci(edges_td_db):
    """A TimexLCA with a finished time-explicit LCI, shared by the edges tests."""
    pytest.importorskip("edges")

    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci()
    return timex_lca


def method_path(name):
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "edges_methods" / f"{name}.json"


def test_translation_gives_each_position_its_node_metadata(timex_lca_with_lci):
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca_with_lci,
        method=("test", "constant"),
        filepath=str(method_path("constant_cf")),
    )
    adapter.lci()

    names = {flow["name"] for flow in adapter.technosphere_flows}
    assert "heat production" in names
    assert "electricity production" in names
    assert all(flow["location"] == "CH" for flow in adapter.technosphere_flows)
    assert all("position" in flow for flow in adapter.technosphere_flows)

    # one entry per matrix column, including several vintages of the same node
    assert len(adapter.technosphere_flows) == len(timex_lca_with_lci.lca.dicts.activity)
    assert set(adapter.position_to_timestamp) == set(
        timex_lca_with_lci.lca.dicts.activity.values()
    )


def test_translation_does_not_resolve_time_mapped_ids_through_bw2data(timex_lca_with_lci):
    """Time-mapped ids do not exist in bw2data; edges' own lci() would raise a KeyError."""
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca_with_lci,
        method=("test", "constant"),
        filepath=str(method_path("constant_cf")),
    )
    adapter.lci()

    assert adapter.biosphere_edges, "biosphere edges should be populated"
    assert adapter.biosphere_flows, "biosphere flows should be populated"


def _adapter(timex_lca, name, **kwargs):
    from bw_timex.edges_lcia import TimexEdgeLCIA

    adapter = TimexEdgeLCIA(
        timex_lca,
        method=("test", name),
        filepath=str(method_path(name)),
        **kwargs,
    )
    adapter.lci()
    adapter.run_mapping(regionalized=False)
    return adapter


def test_constant_cf_reproduces_the_static_score(timex_lca_with_lci):
    """A CF of 1.0 per kg CO2 must give the same total as the static bw2calc score."""
    timex_lca_with_lci.static_lcia()

    adapter = _adapter(timex_lca_with_lci, "constant_cf")
    adapter.characterize_time_explicit()

    assert adapter.score == pytest.approx(timex_lca_with_lci.static_score, rel=1e-9)


def test_cf_is_evaluated_at_the_emission_year_not_the_process_year(timex_lca_with_lci):
    """
    The foreground emits 4 kg in 2024 and 6 kg in 2025, although the process itself runs in 2024.
    With CF 1 in 2024 and CF 2 in 2025, the foreground contribution is 4*1 + 6*2 = 16.
    Characterising at the process year would instead give (4 + 6) * 1 = 10.
    """
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    adapter = _adapter(
        timex_lca_with_lci,
        "year_dependent_cf",
        parameters=parameters,
        scenario="test",
    )
    table = adapter.characterize_time_explicit()

    foreground = table[table["consumer"] == "heat production"]
    assert foreground["impact"].sum() == pytest.approx(16.0)
    assert sorted(foreground["year"].unique()) == [2024, 2025]
    assert adapter.score == pytest.approx(table["impact"].sum())
    # The background electricity's 6 kg is characterized at the consuming process's year, 2024,
    # where the CF is still 1.0. The overall score is therefore the foreground's 16.0 plus 6.0.
    assert adapter.score == pytest.approx(22.0)


def test_cf_table_carries_dates_and_time_mapped_activity(timex_lca_with_lci):
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    adapter = _adapter(
        timex_lca_with_lci,
        "year_dependent_cf",
        parameters=parameters,
        scenario="test",
    )
    table = adapter.characterize_time_explicit()

    for column in ("supplier", "consumer", "date", "year", "amount", "CF", "impact", "activity"):
        assert column in table.columns
    assert sorted(table["year"].unique()) == [2024, 2025]
    assert (table["impact"] == table["amount"] * table["CF"]).all()
    # `year` is the rounded year the CF was evaluated at, `date` the exact emission date;
    # dates from July 1st on round up, so the two only coincide for January dates like these.
    assert table["date"].dt.year.equals(table["year"])


def test_static_characterization_entry_points_are_blocked(timex_lca_with_lci):
    """
    `lcia()` and `generate_cf_table()` would characterize the *static* inventory against whichever
    year was evaluated last, which is exactly the silent error this integration exists to prevent.
    """
    adapter = _adapter(
        timex_lca_with_lci,
        "year_dependent_cf",
        parameters={"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}},
        scenario="test",
    )
    adapter.characterize_time_explicit()

    with pytest.raises(NotImplementedError, match="characterize_time_explicit"):
        adapter.lcia()

    with pytest.raises(NotImplementedError, match="characterize_time_explicit"):
        adapter.generate_cf_table()


def test_cf_uncertainty_is_rejected(timex_lca_with_lci):
    """CF uncertainty gives 3-dimensional CF matrices, which per-year characterization can't use."""
    from bw_timex.edges_lcia import TimexEdgeLCIA

    with pytest.raises(NotImplementedError, match="use_distributions"):
        TimexEdgeLCIA(
            timex_lca_with_lci,
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
            use_distributions=True,
        )


def test_empty_cf_table_has_the_dtypes_of_a_populated_one(timex_lca_with_lci, monkeypatch):
    """An empty result must still support `table["date"].dt.year` and friends."""
    adapter = _adapter(timex_lca_with_lci, "constant_cf")
    populated = adapter.characterize_time_explicit()

    monkeypatch.setattr(adapter, "_biosphere_entries", lambda use_disaggregated_lci: None)
    empty = adapter.characterize_time_explicit()

    assert empty.empty
    assert adapter.score == 0.0
    assert list(empty.columns) == list(populated.columns)
    assert empty.dtypes.equals(populated.dtypes)
    assert empty["date"].dt.year.empty


def test_edges_lcia_end_to_end(timex_lca_with_lci):
    parameters = {"test": {"cf_co2": {"2024": 1.0, "2025": 2.0}}}

    table = timex_lca_with_lci.edges_lcia(
        method=("test", "year_dependent_cf"),
        filepath=str(method_path("year_dependent_cf")),
        parameters=parameters,
        scenario="test",
        regionalized=False,
    )

    assert timex_lca_with_lci.edges_score == pytest.approx(table["impact"].sum())
    assert timex_lca_with_lci.edges_characterized_inventory is table
    assert timex_lca_with_lci.edges_lcia_object.score == pytest.approx(
        timex_lca_with_lci.edges_score
    )


def test_edges_score_before_calculation_raises(timex_lca_with_lci):
    with pytest.raises(AttributeError, match="edges_lcia"):
        timex_lca_with_lci.edges_score


def test_edges_lcia_without_lci_raises(edges_td_db):
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))

    with pytest.raises(AttributeError, match="TimexLCA.lci"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )


def test_edges_lcia_requires_the_expanded_matrix(edges_td_db):
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci(expand_technosphere=False)

    with pytest.raises(NotImplementedError, match="expand_technosphere"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )


def test_edges_lcia_requires_the_dynamic_biosphere(edges_td_db):
    """Without the dynamic inventory, biosphere temporal distributions would be lost silently."""
    from bw_timex import TimexLCA

    node = bd.get_node(database="foreground", code="heat")
    timex_lca = TimexLCA(
        demand={node: 1},
        method=("GWP", "example"),
        database_dates={
            "db_2020": datetime.strptime("2020", "%Y"),
            "foreground": "dynamic",
        },
    )
    timex_lca.build_timeline(starting_datetime=datetime(2024, 1, 1))
    timex_lca.lci(build_dynamic_biosphere=False)

    with pytest.raises(ValueError, match="build_dynamic_biosphere"):
        timex_lca.edges_lcia(
            method=("test", "constant_cf"),
            filepath=str(method_path("constant_cf")),
        )


def test_technosphere_cfs_are_characterized_at_the_process_vintage(timex_lca_with_lci):
    """
    The heat process consumes 3 kWh of electricity and runs in 2024, so with a CF of 5 per kWh in
    2024 the technosphere impact is 15. Technosphere exchanges carry no emission dates, so the
    vintage of the consuming process is the year used.

    bw_timex inserts a temporal market node between "heat production" and the background
    "electricity production" it draws from (even though there is only one candidate background
    database here), so the expanded technosphere matrix carries two edges along this path, not
    one: `electricity production (db_2020, 2020) -> market (2024)`, amount 3, and
    `market (2024) -> heat production (2024)`, amount 3. Because the market column's metadata is
    resolved from the same node code as the real "electricity production" process, both edges
    match the CF's supplier pattern, and characterizing both would double-count this single
    physical flow of 3 kWh. `_technosphere_entries()` drops edges whose consumer is a temporal
    market, keeping only `market -> heat production`, so exactly one row is characterized, dated
    at heat production's own vintage (2024).
    """
    parameters = {"test": {"cf_electricity": {"2024": 5.0, "2025": 50.0}}}

    table = timex_lca_with_lci.edges_lcia(
        method=("test", "technosphere_cf"),
        filepath=str(method_path("technosphere_cf")),
        parameters=parameters,
        scenario="test",
        regionalized=False,
    )

    assert set(table["direction"]) == {"technosphere-technosphere"}
    assert len(table) == 1
    assert table["consumer"].iloc[0] == "heat production"
    assert table["impact"].sum() == pytest.approx(15.0)
    assert sorted(table["year"].unique()) == [2024]
