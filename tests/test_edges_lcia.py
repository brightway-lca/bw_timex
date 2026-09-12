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
