"""Tests for reading and writing what a Brightway database represents."""

from datetime import datetime

import bw2data as bd
import pytest
from loguru import logger

from bw_timex import TimexLCA, set_database_metadata
from bw_timex.database_metadata import resolve_database_dates_from_metadata
from bw_timex.validation import TimexLCAInputs

# ─── Tests for set_database_metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestSetDatabaseMetadata:

    def test_datetime_is_stored_as_iso_string(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_iso_string_is_stored_as_given(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01"

    def test_dynamic_is_allowed(self):
        set_database_metadata("foreground", representative_time="dynamic")
        assert bd.databases["foreground"]["representative_time"] == "dynamic"

    def test_scenario_fields_are_stored(self):
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            iam_model="remind",
            pathway="SSP2-PkBudg500",
        )
        assert bd.databases["db_2022"]["iam_model"] == "remind"
        assert bd.databases["db_2022"]["pathway"] == "SSP2-PkBudg500"

    def test_database_object_is_accepted(self):
        set_database_metadata(
            bd.Database("db_2022"), representative_time=datetime(2022, 1, 1)
        )
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_existing_metadata_is_kept(self):
        before = bd.databases["db_2022"]["backend"]
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        assert bd.databases["db_2022"]["backend"] == before

    def test_survives_flush_and_reload(self):
        set_database_metadata("db_2022", representative_time=datetime(2022, 1, 1))
        bd.databases.__init__()  # re-read from disk
        assert bd.databases["db_2022"]["representative_time"] == "2022-01-01T00:00:00"

    def test_unregistered_database_raises(self):
        with pytest.raises(ValueError, match="not registered"):
            set_database_metadata("no_such_db", representative_time=datetime(2022, 1, 1))

    def test_unparseable_representative_time_raises(self):
        with pytest.raises(ValueError, match="representative_time"):
            set_database_metadata("db_2022", representative_time="whenever")

    def test_non_serializable_value_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            set_database_metadata("db_2022", pathway=object())

    def test_no_metadata_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            set_database_metadata("db_2022")


# ─── Tests for resolving database dates from metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestResolveFromMetadata:

    def test_empty_project_metadata_resolves_to_nothing(self):
        assert resolve_database_dates_from_metadata() == {}

    def test_iso_strings_resolve_to_datetimes(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        assert resolve_database_dates_from_metadata() == {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
        }

    def test_dynamic_metadata_resolves_to_dynamic(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("foreground", representative_time="dynamic")
        resolved = resolve_database_dates_from_metadata()
        assert resolved["foreground"] == "dynamic"
        assert resolved["db_2022"] == datetime(2022, 1, 1)

    def test_databases_without_metadata_are_ignored(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        assert set(resolve_database_dates_from_metadata()) == {"db_2022"}

    def test_multi_scenario_database_is_skipped(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            scenarios=[
                {"pathway": "SSP2-Base", "representative_time": "2024-01-01"},
                {"pathway": "SSP2-PkBudg500", "representative_time": "2024-01-01"},
            ],
        )
        assert set(resolve_database_dates_from_metadata()) == {"db_2022"}

    def test_invalid_metadata_value_raises_naming_the_database(self):
        bd.databases["db_2022"]["representative_time"] = "whenever"
        bd.databases.flush()
        with pytest.raises(ValueError, match="db_2022"):
            resolve_database_dates_from_metadata()

    def test_multi_scenario_database_is_named_in_the_log(self):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            scenarios=[
                {"pathway": "SSP2-Base", "representative_time": "2024-01-01"},
                {"pathway": "SSP2-PkBudg500", "representative_time": "2024-01-01"},
            ],
        )
        messages = []
        sink_id = logger.add(messages.append, level="INFO")
        try:
            resolve_database_dates_from_metadata()
        finally:
            logger.remove(sink_id)
        assert any("db_2024" in message for message in messages)


# ─── Tests for order-insensitive `external_scenarios` comparison ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestExternalScenariosOrderInsensitive:

    def test_ambiguity_signature_is_order_insensitive(self):
        """Same `external_scenarios`, listed in a different order, must not
        look like two different scenarios."""
        set_database_metadata(
            "db_2022",
            representative_time="2022-01-01",
            external_scenarios=["scenario_a", "scenario_b"],
        )
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            external_scenarios=["scenario_b", "scenario_a"],
        )
        # Would raise "Several background scenarios found" if the signature
        # depended on list order.
        resolved = resolve_database_dates_from_metadata()
        assert set(resolved) == {"db_2022", "db_2024"}

    def test_filter_value_is_order_insensitive(self):
        set_database_metadata(
            "db_2022",
            representative_time="2022-01-01",
            external_scenarios=["scenario_a", "scenario_b"],
        )
        resolved = resolve_database_dates_from_metadata(
            scenario={"external_scenarios": ["scenario_b", "scenario_a"]}
        )
        assert resolved == {"db_2022": datetime(2022, 1, 1)}


# ─── Tests for scenario selection ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestScenarioSelection:

    @pytest.fixture(autouse=True)
    def two_scenarios(self, temporal_grouping_db_monthly):
        """db_2022 and db_2024 hold the same year in two different pathways."""
        set_database_metadata(
            "db_2022",
            representative_time="2022-01-01",
            iam_model="remind",
            pathway="SSP2-PkBudg500",
            premise_version="2.4.9.1",
        )
        set_database_metadata(
            "db_2024",
            representative_time="2024-01-01",
            iam_model="remind",
            pathway="SSP2-Base",
            premise_version="2.4.9.1",
        )

    def test_two_scenario_sets_without_selection_raises(self):
        with pytest.raises(ValueError, match="Several background scenarios"):
            resolve_database_dates_from_metadata()

    def test_error_names_the_differing_key_and_values(self):
        with pytest.raises(ValueError) as excinfo:
            resolve_database_dates_from_metadata()
        message = str(excinfo.value)
        assert "pathway" in message
        assert "SSP2-PkBudg500" in message
        assert "SSP2-Base" in message
        # iam_model is identical in both sets, so it isn't part of the report
        assert "iam_model" not in message

    def test_scenario_selects_one_set(self):
        resolved = resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-Base"}
        )
        assert resolved == {"db_2024": datetime(2024, 1, 1)}

    def test_databases_without_scenario_metadata_survive_the_filter(self):
        set_database_metadata("foreground", representative_time="dynamic")
        resolved = resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-Base"}
        )
        assert resolved == {
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_several_filter_keys_are_combined(self):
        resolved = resolve_database_dates_from_metadata(
            scenario={"iam_model": "remind", "pathway": "SSP2-Base"}
        )
        assert set(resolved) == {"db_2024"}

    def test_filter_matching_nothing_resolves_to_nothing(self):
        assert resolve_database_dates_from_metadata(
            scenario={"pathway": "SSP2-PkBudg1150"}
        ) == {}

    def test_unknown_filter_key_raises_listing_available_keys(self):
        with pytest.raises(ValueError) as excinfo:
            resolve_database_dates_from_metadata(scenario={"pathwya": "SSP2-Base"})
        message = str(excinfo.value)
        assert "pathwya" in message
        assert "pathway" in message

    def test_same_scenario_from_two_premise_versions_is_not_ambiguous(self):
        set_database_metadata("db_2024", pathway="SSP2-PkBudg500")
        set_database_metadata("db_2024", premise_version="2.4.9.2")
        assert set(resolve_database_dates_from_metadata()) == {"db_2022", "db_2024"}


# ─── Tests for TimexLCA using database metadata ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestTimexLCAFromMetadata:

    @pytest.fixture
    def fu(self):
        return bd.get_node(database="foreground", code="A")

    def test_no_arguments_uses_metadata(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates == {
            "db_2022": datetime(2022, 1, 1),
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_demand_database_metadata_is_respected(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("foreground", representative_time="dynamic")
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates["foreground"] == "dynamic"

    def test_demand_database_mapped_to_a_fixed_date_raises(self, fu):
        """Regression: without an explicit `database_dates`, the demand's
        database is resolved from its own `representative_time` metadata. If
        that metadata maps it to a fixed date rather than "dynamic", this
        must still raise - the same check that fires when `database_dates`
        is given explicitly (see `test_demand_not_in_dynamic_db_raises` in
        test_timex_lca.py) must also fire along the metadata-resolution path.
        """
        set_database_metadata("foreground", representative_time="2022-01-01")
        with pytest.raises(ValueError, match="mapped to a date rather than 'dynamic'"):
            TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))

    def test_scenario_is_forwarded(self, fu):
        set_database_metadata(
            "db_2022", representative_time="2022-01-01", pathway="SSP2-Base"
        )
        set_database_metadata(
            "db_2024", representative_time="2024-01-01", pathway="SSP2-PkBudg500"
        )
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            scenario={"pathway": "SSP2-Base"},
        )
        assert tlca.database_dates == {
            "db_2022": datetime(2022, 1, 1),
            "foreground": "dynamic",
        }

    def test_typo_in_scenario_value_raises(self, fu):
        set_database_metadata(
            "db_2022", representative_time="2022-01-01", pathway="SSP2-Base"
        )
        set_database_metadata(
            "db_2024", representative_time="2024-01-01", pathway="SSP2-PkBudg500"
        )
        with pytest.raises(ValueError, match="SSP2-Basee") as excinfo:
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                scenario={"pathway": "SSP2-Basee"},
            )
        message = str(excinfo.value)
        # The filter that matched nothing, and what's actually declared for
        # that key, must both be in the error so a typo is obvious.
        assert "SSP2-Base" in message
        assert "SSP2-PkBudg500" in message

    def test_typo_in_scenario_value_raises_even_with_a_non_scenario_survivor(self, fu):
        """A database that declares no scenario keys survives every filter,
        so the resolved mapping is non-empty even though the filter matched
        none of the scenario databases it was meant to select among. The
        error must still fire - checking whether `resolved` is empty is not
        enough.
        """
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata(
            "db_2024", representative_time="2024-01-01", pathway="SSP2-Base"
        )
        with pytest.raises(ValueError, match="SSP2-Basee") as excinfo:
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                scenario={"pathway": "SSP2-Basee"},
            )
        message = str(excinfo.value)
        assert "SSP2-Base" in message

    def test_database_dates_is_exclusive(self, fu):
        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        tlca = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates={
                "db_2024": datetime(2024, 1, 1),
                "foreground": "dynamic",
            },
        )
        assert tlca.database_dates == {
            "db_2024": datetime(2024, 1, 1),
            "foreground": "dynamic",
        }

    def test_database_dates_with_scenario_raises(self, fu):
        with pytest.raises(ValueError, match="only applies when"):
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                database_dates={"foreground": "dynamic"},
                scenario={"pathway": "SSP2-Base"},
            )

    def test_empty_database_dates_dict_raises(self, fu):
        """An empty dict is falsy but not `None`: it must still be treated as
        an explicit (if invalid) `database_dates`, not fall through to
        metadata resolution.
        """
        set_database_metadata("db_2022", representative_time="2022-01-01")
        with pytest.raises(ValueError, match="non-empty dictionary"):
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                database_dates={},
            )

    def test_no_metadata_anywhere_falls_back_to_dynamic_demand(self, fu):
        tlca = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        assert tlca.database_dates == {"foreground": "dynamic"}

    def test_metadata_and_database_dates_give_the_same_score(self, fu):
        explicit = TimexLCA(
            demand={fu.key: 1},
            method=("GWP", "example"),
            database_dates={
                "db_2022": datetime(2022, 1, 1),
                "db_2024": datetime(2024, 1, 1),
                "foreground": "dynamic",
            },
        )
        explicit.build_timeline(starting_datetime=datetime(2024, 1, 2))
        explicit.lci()
        explicit.static_lcia()

        set_database_metadata("db_2022", representative_time="2022-01-01")
        set_database_metadata("db_2024", representative_time="2024-01-01")
        from_metadata = TimexLCA(demand={fu.key: 1}, method=("GWP", "example"))
        from_metadata.build_timeline(starting_datetime=datetime(2024, 1, 2))
        from_metadata.lci()
        from_metadata.static_lcia()

        assert from_metadata.static_score == pytest.approx(explicit.static_score)


# ─── Tests for TimexLCAInputs.validate_scenario ───


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestValidateScenario:

    @pytest.fixture
    def fu(self):
        return bd.get_node(database="foreground", code="A")

    def test_non_string_key_raises(self, fu):
        with pytest.raises(ValueError, match="scenario keys must be strings"):
            TimexLCAInputs(
                demand={fu.key: 1},
                method=("GWP", "example"),
                scenario={123: "SSP2-Base"},
            )

    def test_non_scalar_value_raises(self, fu):
        with pytest.raises(ValueError, match="scenario values must be scalars"):
            TimexLCAInputs(
                demand={fu.key: 1},
                method=("GWP", "example"),
                scenario={"pathway": {"nested": "dict"}},
            )


# ─── Tests for the scenario key vocabulary ───


class TestSplitScenario:

    def test_build_keys_are_separated(self):
        from bw_timex.database_metadata import split_scenario

        filters, build = split_scenario(
            {
                "iam_model": "remind",
                "pathway": "SSP2-PkBudg500",
                "years": [2020, 2030],
                "sectors": ["electricity"],
                "source_database": "my_ecoinvent",
            }
        )
        assert filters == {"iam_model": "remind", "pathway": "SSP2-PkBudg500"}
        assert build == {
            "years": [2020, 2030],
            "sectors": ["electricity"],
            "source_database": "my_ecoinvent",
        }

    def test_unknown_keys_are_treated_as_filters(self):
        from bw_timex.database_metadata import split_scenario

        filters, build = split_scenario({"my_own_key": "value"})
        assert filters == {"my_own_key": "value"}
        assert build == {}

    def test_none_scenario_gives_empty_dicts(self):
        from bw_timex.database_metadata import split_scenario

        assert split_scenario(None) == ({}, {})


class TestDatabaseMatchesScenario:

    def test_declared_and_equal_matches(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"pathway": "SSP2-PkBudg500"}, {"pathway": "SSP2-PkBudg500"})

    def test_declared_and_different_does_not_match(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert not database_matches_scenario({"pathway": "SSP2-Base"}, {"pathway": "SSP2-PkBudg500"})

    def test_undeclared_key_still_matches(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"representative_time": "2020-01-01"}, {"pathway": "SSP2-PkBudg500"})

    def test_list_values_compare_order_insensitively(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario(
            {"external_scenarios": ["b", "a"]}, {"external_scenarios": ["a", "b"]}
        )

    def test_empty_scenario_matches_everything(self):
        from bw_timex.database_metadata import database_matches_scenario

        assert database_matches_scenario({"pathway": "SSP2-Base"}, None)


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestResolverIgnoresBuildKeys:

    def test_build_keys_do_not_reach_the_filter(self):
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            pathway="SSP2-PkBudg500",
        )
        resolved = resolve_database_dates_from_metadata(
            {"pathway": "SSP2-PkBudg500", "years": [2022], "sectors": ["electricity"]}
        )
        assert resolved == {"db_2022": datetime(2022, 1, 1)}

    def test_build_keys_are_not_reported_as_undeclared(self):
        # A scenario that keeps its `years` but is used without
        # `create_missing` must not be told that `years` is missing metadata:
        # no database ever declares it.
        set_database_metadata("foreground", representative_time="dynamic")
        set_database_metadata(
            "db_2022",
            representative_time=datetime(2022, 1, 1),
            pathway="SSP2-Base",
        )
        fu = bd.get_node(database="foreground", code="A")
        with pytest.raises(ValueError) as excinfo:
            TimexLCA(
                demand={fu.key: 1},
                method=("GWP", "example"),
                scenario={"pathway": "SSP2-PkBudg500", "years": [2022]},
            )
        message = str(excinfo.value)
        assert "pathway" in message
        assert "SSP2-Base" in message
        # `years` is a build key, never a metadata key: it must not appear as
        # one of the filter keys that matched nothing. (The message may still
        # suggest `create_missing=True` with a `years` list.)
        assert "'years'" not in message
