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
        assert find_existing_vintages(SCENARIO) == {
            2030: ("ei_2030", datetime(2030, 1, 1))
        }

    def test_other_scenario_is_not_found(self):
        write_vintage("ei_2030", 2030, pathway="SSP2-Base")
        assert find_existing_vintages(SCENARIO) == {}

    def test_database_without_scenario_metadata_satisfies_its_year(self):
        write_minimal_database("hand_built_2020")
        set_database_metadata("hand_built_2020", representative_time=datetime(2020, 1, 1))
        assert find_existing_vintages(SCENARIO) == {
            2020: ("hand_built_2020", datetime(2020, 1, 1))
        }

    def test_the_real_representative_time_is_returned(self):
        write_minimal_database("hand_built_mid_2030")
        set_database_metadata(
            "hand_built_mid_2030", representative_time=datetime(2030, 6, 15)
        )
        assert find_existing_vintages(SCENARIO) == {
            2030: ("hand_built_mid_2030", datetime(2030, 6, 15))
        }

    def test_dynamic_databases_are_ignored(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert find_existing_vintages(SCENARIO) == {}

    def test_multi_scenario_databases_are_ignored(self):
        write_vintage("superstructure", 2030)
        bd.databases["superstructure"]["scenarios"] = [{"year": 2030}, {"year": 2040}]
        bd.databases.flush()
        assert find_existing_vintages(SCENARIO) == {}


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSectorMatching:
    """A vintage only satisfies a year if it covers the sectors that were asked for.

    premise does not record which sectors it updated, so `sectors` metadata is
    written by `ensure_scenario_databases` itself; a database without it was
    built with all sectors (or by hand, which the scenario filter treats the
    same way).
    """

    def test_same_sectors_are_reused(self):
        write_vintage("ei_2030", 2030, sectors=["electricity"])
        assert find_existing_vintages(SCENARIO, sectors=["electricity"]) == {
            2030: ("ei_2030", datetime(2030, 1, 1))
        }

    def test_sector_order_does_not_matter(self):
        write_vintage("ei_2030", 2030, sectors=["electricity", "steel"])
        assert find_existing_vintages(SCENARIO, sectors=["steel", "electricity"]) == {
            2030: ("ei_2030", datetime(2030, 1, 1))
        }

    def test_other_sectors_are_not_reused(self):
        write_vintage("ei_2030", 2030, sectors=["electricity"])
        assert find_existing_vintages(SCENARIO, sectors=["steel"]) == {}

    def test_narrowed_vintage_does_not_satisfy_an_all_sector_request(self):
        write_vintage("ei_2030", 2030, sectors=["electricity"])
        assert find_existing_vintages(SCENARIO) == {}

    def test_all_sector_vintage_does_not_satisfy_a_narrowed_request(self):
        write_vintage("ei_2030", 2030)
        assert find_existing_vintages(SCENARIO, sectors=["electricity"]) == {}

    def test_a_differently_narrowed_vintage_is_rebuilt(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")
        write_vintage("ei_electricity_2040", 2040, sectors=["electricity"])
        ensure_scenario_databases(
            {**SCENARIO, "years": [2040], "sectors": ["steel"]}
        )
        assert len(fake_premise) == 1
        assert fake_premise[0]["sectors"] == ["steel"]


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

    def test_found_database_keeps_its_real_representative_time(self, fake_premise):
        # A hand-built vintage need not sit on 1 January, and the returned
        # mapping documents what the database represents, not what was asked for.
        write_minimal_database("hand_built_mid_2030")
        set_database_metadata(
            "hand_built_mid_2030", representative_time=datetime(2030, 6, 15)
        )
        result = ensure_scenario_databases({**SCENARIO, "years": [2030]})
        assert result == {"hand_built_mid_2030": datetime(2030, 6, 15)}

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

    def test_unknown_scenario_key_is_rejected_before_building(self, fake_premise):
        # Caught here or hours later by `resolve_database_dates_from_metadata`,
        # after premise has written gigabytes.
        with pytest.raises(ValueError) as error:
            ensure_scenario_databases({**SCENARIO, "region": "EU", "years": [2030]})
        message = str(error.value)
        assert "region" in message
        assert "iam_model" in message
        assert fake_premise == []

    def test_malformed_ecoinvent_credentials_are_rejected(self, fake_premise):
        with pytest.raises(ValueError, match="username, password"):
            ensure_scenario_databases(
                {**SCENARIO, "years": [2030]}, ecoinvent_credentials=("user",)
            )
        assert fake_premise == []


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
        write_vintage(
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030", 2030, pathway="SSP2-Base"
        )
        with pytest.raises(ValueError, match="already exists"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

    def test_unfinished_earlier_build_is_recognised(self, fake_premise, monkeypatch):
        # What a run that stopped at the metadata check leaves behind: premise
        # wrote the database, but it carries no `representative_time`. Telling
        # the user to delete it throws away hours of correct premise output.
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError) as error:
            ensure_scenario_databases({**SCENARIO, "years": [2030]})
        message = str(error.value)
        assert "did not finish" in message
        assert "set_database_metadata" in message
        assert "representative_time" in message

    def test_the_documented_recovery_lets_the_build_be_resumed(
        self, fake_premise, monkeypatch
    ):
        # The whole point of the message: adding the metadata by hand makes the
        # next run reuse the stranded database instead of rebuilding it.
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ecoinvent-3.10.1-cutoff")
        write_minimal_database("ecoinvent-3.10.1-biosphere")

        def premise_without_metadata(**kwargs):
            for name in kwargs["names"]:
                write_minimal_database(name)

        monkeypatch.setattr(
            "bw_timex.scenario_builder._run_premise",
            premise_without_metadata,
            raising=True,
        )
        with pytest.raises(RuntimeError, match="set_database_metadata"):
            ensure_scenario_databases({**SCENARIO, "years": [2030]})

        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        set_database_metadata(name, representative_time=datetime(2030, 1, 1))
        # Reuses the stranded database: the fake premise above is still in
        # place, so a rebuild would raise the same RuntimeError again.
        assert ensure_scenario_databases({**SCENARIO, "years": [2030]}) == {
            name: datetime(2030, 1, 1)
        }

    def test_nothing_is_built_when_a_name_collides(self, fake_premise, monkeypatch):
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        with pytest.raises(ValueError):
            ensure_scenario_databases({**SCENARIO, "years": [2030, 2040]})
        assert fake_premise == []

    def test_unfinished_earlier_build_with_sectors_can_be_resumed(
        self, fake_premise, monkeypatch
    ):
        # `sectors` metadata is only written after the representative_time
        # check passes, so a stranded narrowed build has neither key. The
        # advice must restore both, or re-running the same request lands
        # right back in this same collision.
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_minimal_database("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030")
        request = {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        with pytest.raises(ValueError) as error:
            ensure_scenario_databases(request)
        message = str(error.value)
        assert "did not finish" in message
        assert "set_database_metadata" in message
        assert "sectors" in message
        assert "electricity" in message

        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        set_database_metadata(
            name, representative_time=datetime(2030, 1, 1), sectors=["electricity"]
        )
        # Following the advice literally must resume, not collide again.
        assert ensure_scenario_databases(request) == {name: datetime(2030, 1, 1)}

    def test_all_sector_vintage_colliding_with_a_narrowed_build_is_not_foreign(
        self, fake_premise, monkeypatch
    ):
        # An all-sectors vintage satisfies every filter key of a narrowed
        # request; only `sectors` differs. That is not a foreign database and
        # must not be offered "delete" as the fix.
        monkeypatch.setenv("PREMISE_KEY", "key")
        write_vintage("ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030", 2030)
        with pytest.raises(ValueError) as error:
            ensure_scenario_databases(
                {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
            )
        message = str(error.value)
        assert "delete" not in message.lower()
        assert "sectors" in message
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

    def test_a_vintage_built_with_sectors_is_reused_on_the_same_request(
        self, fake_premise
    ):
        # The `sectors` metadata written after the build must be readable by
        # the next run, or every narrowed build is repeated forever.
        request = {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        ensure_scenario_databases(request)
        ensure_scenario_databases(request)
        assert len(fake_premise) == 1

    def test_a_vintage_built_with_sectors_is_not_reused_for_other_sectors(
        self, fake_premise
    ):
        ensure_scenario_databases(
            {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        )
        with pytest.raises(ValueError, match="already exists"):
            ensure_scenario_databases(
                {**SCENARIO, "years": [2030], "sectors": ["steel"]}
            )
        assert len(fake_premise) == 1

    def test_duplicate_years_are_built_once(self, fake_premise):
        ensure_scenario_databases({**SCENARIO, "years": [2060, 2060]})
        assert fake_premise[0]["names"] == [
            "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2060"
        ]
        assert [s["year"] for s in fake_premise[0]["scenarios"]] == [2060]

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

    def test_empty_sectors_list_is_idempotent(self, fake_premise):
        # `sectors=[]` must behave exactly like omitting `sectors`, or the
        # second identical call collides with the database the first just built.
        request = {**SCENARIO, "years": [2030], "sectors": []}
        ensure_scenario_databases(request)
        ensure_scenario_databases(request)
        assert len(fake_premise) == 1

    def test_missing_representative_time_with_sectors_can_be_resumed(
        self, fake_premise, monkeypatch
    ):
        # Same trap as the collision-guard message: `sectors` is written
        # after this check, so the recovery snippet must include it too, or
        # following it literally re-triggers the sector-mismatch collision.
        def premise_without_metadata(**kwargs):
            for name in kwargs["names"]:
                write_minimal_database(name)

        monkeypatch.setattr(
            "bw_timex.scenario_builder._run_premise",
            premise_without_metadata,
            raising=True,
        )
        request = {**SCENARIO, "years": [2030], "sectors": ["electricity"]}
        with pytest.raises(RuntimeError) as error:
            ensure_scenario_databases(request)
        message = str(error.value)
        assert "set_database_metadata" in message
        assert "sectors" in message
        assert "electricity" in message

        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        set_database_metadata(
            name, representative_time=datetime(2030, 1, 1), sectors=["electricity"]
        )
        assert ensure_scenario_databases(request) == {name: datetime(2030, 1, 1)}


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

    def test_package_level_shorthand_builds_without_timex_lca(self, fake_premise):
        import bw_timex

        result = bw_timex.ensure_scenario_databases({**SCENARIO, "years": [2030]})

        name = "ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2030"
        assert result == {name: datetime(2030, 1, 1)}
        assert name in bd.databases
        assert len(fake_premise) == 1

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

    def test_unknown_scenario_key_raises_before_any_build(self, fake_premise):
        from bw_timex import TimexLCA

        with pytest.raises(ValueError, match="region"):
            TimexLCA(
                demand={("foreground", "A"): 1},
                method=("GWP", "example"),
                scenario={**SCENARIO, "region": "EU", "years": [2030]},
                create_missing=True,
            )
        assert fake_premise == []

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
