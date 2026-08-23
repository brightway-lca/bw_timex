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


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestValidation:

    def test_missing_years_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases(SCENARIO)

    def test_empty_years_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases({**SCENARIO, "years": []})

    def test_non_integer_year_raises(self, fake_premise):
        with pytest.raises(ValueError, match="years"):
            ensure_scenario_databases({**SCENARIO, "years": ["2030"]})

    def test_missing_filter_keys_are_named(self, fake_premise):
        with pytest.raises(ValueError, match="ecoinvent_version"):
            ensure_scenario_databases(
                {"iam_model": "remind", "pathway": "SSP2-PkBudg500", "system_model": "cutoff",
                 "years": [2030]}
            )

    def test_no_scenario_raises(self, fake_premise):
        with pytest.raises(ValueError, match="scenario"):
            ensure_scenario_databases(None)


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestCredentials:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self, temporal_grouping_db_monthly):
        # Without a source database the run would stop at the ecoinvent
        # credentials before ever reaching the premise key. Depends
        # explicitly on temporal_grouping_db_monthly (rather than relying on
        # class-level usefixtures) so it runs after that fixture builds the
        # project: autouse fixtures otherwise run before fixtures requested
        # via usefixtures, and temporal_grouping_db_monthly's @bw2test starts
        # a brand-new empty project that would silently erase these writes.
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_premise_key_argument_wins_over_environment(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "from-environment")
        ensure_scenario_databases({**SCENARIO, "years": [2030]}, premise_key="explicit")
        assert fake_premise[0]["key"] == "explicit"

    def test_premise_key_falls_back_to_environment(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "from-environment")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["key"] == "from-environment"

    def test_missing_premise_key_names_the_variable(self, fake_premise, monkeypatch):
        monkeypatch.delenv("PREMISE_KEY", raising=False)
        with pytest.raises(ValueError, match="PREMISE_KEY"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})


@pytest.fixture
def fake_ecoinvent_import(monkeypatch):
    """Records ecoinvent imports and registers what bw2io would register."""
    calls = []

    def fake_import(version, system_model, credentials):
        calls.append((version, system_model, credentials))
        name = f"ecoinvent-{version}-{system_model}"
        write_minimal_database(name)
        write_minimal_database(f"ecoinvent-{version}-biosphere")
        return name

    monkeypatch.setattr(
        "bw_timex.scenario_builder._import_ecoinvent", fake_import, raising=True
    )
    return calls


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestVintageName:

    def test_name_is_deterministic(self):
        from bw_timex.scenario_builder import vintage_name

        assert (
            vintage_name(SCENARIO, 2030)
            == "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        )


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSourceDatabase:

    def test_existing_ecoinvent_is_used(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_ecoinvent_import == []
        assert fake_premise[0]["source_database"] == "ecoinvent-3.10.1-cutoff"
        assert fake_premise[0]["biosphere"] == "ecoinvent-3.10.1-biosphere"

    def test_missing_ecoinvent_is_imported(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        monkeypatch.setenv("ECOINVENT_USERNAME", "user")
        monkeypatch.setenv("ECOINVENT_PASSWORD", "secret")
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_ecoinvent_import == [("3.10.1", "cutoff", ("user", "secret"))]

    def test_missing_ecoinvent_without_credentials_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        monkeypatch.delenv("ECOINVENT_USERNAME", raising=False)
        monkeypatch.delenv("ECOINVENT_PASSWORD", raising=False)
        with pytest.raises(ValueError, match="ECOINVENT_USERNAME"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

    def test_explicit_source_database_is_used(self, fake_premise, fake_ecoinvent_import, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("my_own_ecoinvent")
        write_minimal_database("biosphere3")
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "source_database": "my_own_ecoinvent"}
        )
        assert fake_ecoinvent_import == []
        assert fake_premise[0]["source_database"] == "my_own_ecoinvent"
        assert fake_premise[0]["biosphere"] == "biosphere3"

    def test_unregistered_explicit_source_database_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        with pytest.raises(ValueError, match="no_such_db"):
            ensure_scenario_databases(
                {**SCENARIO, "years": [2030], "source_database": "no_such_db"}
            )


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestOverwriteGuard:

    def test_foreign_database_under_target_name_raises(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError, match="already exists"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

    def test_nothing_is_built_when_a_name_collides(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError):
            ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise == []


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestBuilding:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self, temporal_grouping_db_monthly, monkeypatch):
        # See TestCredentials._ecoinvent_present: this must depend explicitly
        # on temporal_grouping_db_monthly, or the autouse fixture runs first
        # and temporal_grouping_db_monthly's @bw2test then wipes it out by
        # switching to a fresh temp project.
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_only_missing_years_are_built(self, fake_premise):
        write_vintage("ei_2030", 2030)
        ensure_scenario_databases({**SCENARIO, "years": [2020, 2030, 2040]})
        assert len(fake_premise) == 1
        assert [s["year"] for s in fake_premise[0]["scenarios"]] == [2020, 2040]

    def test_premise_scenarios_carry_model_and_pathway(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["scenarios"] == [
            {"model": "remind", "pathway": "SSP2-PkBudg500", "year": 2030}
        ]

    def test_source_version_and_system_model_are_passed(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["source_version"] == "3.10.1"
        assert fake_premise[0]["system_model"] == "cutoff"

    def test_names_line_up_with_scenarios(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise[0]["names"] == [
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030",
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2040",
        ]

    def test_all_sectors_by_default(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert fake_premise[0]["sectors"] is None

    def test_narrowed_sectors_are_passed_through(self, fake_premise):
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "sectors": ["electricity", "steel"]}
        )
        assert fake_premise[0]["sectors"] == ["electricity", "steel"]

    def test_narrowed_sectors_are_recorded_in_metadata(self, fake_premise):
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        )
        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        assert bd.databases[name]["sectors"] == ["electricity"]

    def test_no_sectors_metadata_when_all_sectors(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2030]})
        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        assert "sectors" not in bd.databases[name]

    def test_built_and_existing_vintages_are_returned(self, fake_premise):
        write_vintage("ei_2030", 2030)
        result = ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert result == {
            "ei_2030": datetime(2030, 1, 1),
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2040": datetime(2040, 1, 1),
        }

    def test_missing_representative_time_after_write_raises(self, monkeypatch):
        def fake_run_premise(**kwargs):
            for name in kwargs["names"]:
                write_minimal_database(name)  # no metadata: an old premise

        monkeypatch.setattr(
            "bw_timex.scenario_builder._run_premise", fake_run_premise, raising=True
        )
        with pytest.raises(RuntimeError, match="2.4.9.2"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestPremiseNotInstalled:

    def test_import_error_names_the_extra(self, monkeypatch):
        from bw_timex import scenario_builder

        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")
        monkeypatch.setitem(sys.modules, "premise", None)  # forces ImportError
        with pytest.raises(ImportError, match=r'bw_timex\[premise\]'):
            scenario_builder.ensure_scenario_databases({**SCENARIO, "years": [2030]})


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAIntegration:

    @pytest.fixture(autouse=True)
    def _ecoinvent_present(self, temporal_grouping_db_monthly, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

    def test_public_export(self):
        import bw_timex

        assert bw_timex.ensure_scenario_databases is ensure_scenario_databases

    def test_create_missing_builds_and_resolves(self, fake_premise):
        from bw_timex import TimexLCA

        tlca = TimexLCA(
            demand={("foreground", "A"): 1},
            method=("GWP", "example"),
            scenario={**SCENARIO, "years": [2030]},
            create_missing=True,
        )
        assert (
            tlca.database_dates["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"]
            == datetime(2030, 1, 1)
        )
        assert tlca.database_dates["foreground"] == "dynamic"

    def test_default_does_not_build(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                scenario={**SCENARIO, "years": [2030]},
            )
        assert fake_premise == []

    def test_error_without_create_missing_mentions_it(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="create_missing"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                scenario={**SCENARIO, "years": [2030]},
            )

    def test_create_missing_with_database_dates_raises(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="database_dates"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                database_dates={"foreground": "dynamic"},
                create_missing=True,
            )
        assert fake_premise == []

    def test_create_missing_without_scenario_raises(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="scenario"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                create_missing=True,
            )
        assert fake_premise == []
