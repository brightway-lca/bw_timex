"""Tests for finding or building the background vintages a scenario names.

premise is never installed, never called and never imported here: the two
functions that touch it are monkeypatched with fakes that register small
Brightway databases carrying premise-style metadata.
"""

import sys
from datetime import datetime

import bw2data as bd
import pytest

from bw_timex import set_database_metadata
from bw_timex.scenario_builder import ensure_scenario_databases, find_existing_vintages

SCENARIO = {
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "system_model": "cutoff",
    "ecoinvent_version": "3.10.1",
}


def write_minimal_database(name):
    """A one-process database, enough to be registered and carry metadata."""
    bd.Database(name).write(
        {
            (name, "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "C",
                "exchanges": [
                    {"amount": 1, "type": "production", "input": (name, "C")},
                ],
            },
        },
    )


def write_vintage(name, year, **extra):
    """A database that looks like a premise export for `year`."""
    write_minimal_database(name)
    set_database_metadata(
        name,
        representative_time=datetime(year, 1, 1),
        premise_version="2.4.9.2",
        **{**SCENARIO, **extra},
    )


@pytest.fixture
def fake_premise(monkeypatch):
    """Records calls to the premise seam and writes what premise would write."""
    calls = []

    def fake_run_premise(**kwargs):
        calls.append(kwargs)
        years = [scenario["year"] for scenario in kwargs["scenarios"]]
        for name, year in zip(kwargs["names"], years):
            write_vintage(name, year)

    monkeypatch.setattr(
        "bw_timex.scenario_builder._run_premise", fake_run_premise, raising=True
    )
    return calls


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestFindExistingVintages:

    def test_matching_vintage_is_found_by_year(self):
        write_vintage("ei_2030", 2030)
        assert find_existing_vintages(SCENARIO) == {2030: "ei_2030"}

    def test_other_scenario_is_not_found(self):
        write_vintage("ei_2030", 2030, pathway="SSP2-Base")
        assert find_existing_vintages(SCENARIO) == {}

    def test_database_without_scenario_metadata_satisfies_its_year(self):
        write_minimal_database("hand_built_2020")
        set_database_metadata("hand_built_2020", representative_time=datetime(2020, 1, 1))
        assert find_existing_vintages(SCENARIO) == {2020: "hand_built_2020"}

    def test_dynamic_databases_are_ignored(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert find_existing_vintages(SCENARIO) == {}

    def test_multi_scenario_databases_are_ignored(self):
        write_vintage("superstructure", 2030)
        bd.databases["superstructure"]["scenarios"] = [{"year": 2030}, {"year": 2040}]
        bd.databases.flush()
        assert find_existing_vintages(SCENARIO) == {}


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestNothingToBuild:

    def test_all_years_present_builds_nothing(self, fake_premise):
        write_vintage("ei_2030", 2030)
        write_vintage("ei_2040", 2040)
        ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise == []

    def test_all_years_present_returns_the_mapping(self, fake_premise):
        write_vintage("ei_2030", 2030)
        result = ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert result == {"ei_2030": datetime(2030, 1, 1)}

    def test_premise_is_not_imported(self, fake_premise, monkeypatch):
        monkeypatch.delitem(sys.modules, "premise", raising=False)
        write_vintage("ei_2030", 2030)
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert "premise" not in sys.modules
