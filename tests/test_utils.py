"""Tests for utility functions in bw_timex.utils."""

from datetime import datetime

import bw2data as bd
import numpy as np
import pandas as pd
import pytest
from bw2data.errors import MultipleResults, UnknownObject
from bw_temporalis import TemporalDistribution

from bw_timex.utils import (
    add_flows_to_characterization_functions,
    add_temporal_distribution_to_exchange,
    add_temporal_evolution_to_exchange,
    convert_date_string_to_datetime,
    extract_date_as_integer,
    extract_date_as_string,
    get_exchange,
    get_temporal_evolution_factor,
    resolve_temporalized_node_name,
    round_datetime,
)

# --- extract_date_as_integer ---


class TestExtractDateAsInteger:
    def test_year(self):
        assert extract_date_as_integer(datetime(2023, 3, 15), "year") == 2023

    def test_month(self):
        assert extract_date_as_integer(datetime(2023, 3, 15), "month") == 202303

    def test_day(self):
        assert extract_date_as_integer(datetime(2023, 3, 15), "day") == 20230315

    def test_hour(self):
        assert extract_date_as_integer(datetime(2023, 3, 15, 14), "hour") == 2023031514

    def test_default_is_year(self):
        assert extract_date_as_integer(datetime(2023, 7, 1)) == 2023

    def test_invalid_resolution(self):
        with pytest.raises(ValueError, match="Invalid time_res"):
            extract_date_as_integer(datetime(2023, 1, 1), "minute")


# --- extract_date_as_string ---


class TestExtractDateAsString:
    def test_year(self):
        assert extract_date_as_string(datetime(2023, 3, 15), "year") == "2023"

    def test_month(self):
        assert extract_date_as_string(datetime(2023, 3, 15), "month") == "202303"

    def test_day(self):
        assert extract_date_as_string(datetime(2023, 3, 15), "day") == "20230315"

    def test_hour(self):
        assert extract_date_as_string(datetime(2023, 3, 15, 14), "hour") == "2023031514"

    def test_invalid_grouping(self):
        with pytest.raises(ValueError, match="not a valid option"):
            extract_date_as_string(datetime(2023, 1, 1), "minute")


# --- convert_date_string_to_datetime ---


class TestConvertDateStringToDatetime:
    def test_year(self):
        assert convert_date_string_to_datetime("year", "2023") == datetime(2023, 1, 1)

    def test_month(self):
        assert convert_date_string_to_datetime("month", "202303") == datetime(
            2023, 3, 1
        )

    def test_day(self):
        assert convert_date_string_to_datetime("day", "20230315") == datetime(
            2023, 3, 15
        )

    def test_hour(self):
        assert convert_date_string_to_datetime("hour", "2023031514") == datetime(
            2023, 3, 15, 14
        )

    def test_invalid_grouping(self):
        with pytest.raises(ValueError, match="not a valid option"):
            convert_date_string_to_datetime("minute", "2023")


# --- round_datetime ---


class TestRoundDatetime:
    # Year rounding
    def test_round_year_down(self):
        result = round_datetime(datetime(2023, 3, 15), "year")
        assert result == pd.Timestamp("2023-01-01")

    def test_round_year_up(self):
        result = round_datetime(datetime(2023, 9, 15), "year")
        assert result == pd.Timestamp("2024-01-01")

    def test_round_year_boundary(self):
        result = round_datetime(datetime(2023, 7, 1), "year")
        assert result == pd.Timestamp("2024-01-01")

    # Month rounding
    def test_round_month_down(self):
        result = round_datetime(datetime(2023, 3, 5), "month")
        assert result == pd.Timestamp("2023-03-01")

    def test_round_month_up(self):
        result = round_datetime(datetime(2023, 3, 25), "month")
        assert result == pd.Timestamp("2023-04-01")

    # Day rounding
    def test_round_day_down(self):
        result = round_datetime(datetime(2023, 3, 15, 8), "day")
        assert result == datetime(2023, 3, 15)

    def test_round_day_up(self):
        result = round_datetime(datetime(2023, 3, 15, 18), "day")
        assert result == datetime(2023, 3, 16)

    # Hour rounding
    def test_round_hour_down(self):
        result = round_datetime(datetime(2023, 3, 15, 14, 10), "hour")
        assert result == datetime(2023, 3, 15, 14)

    def test_round_hour_up(self):
        result = round_datetime(datetime(2023, 3, 15, 14, 45), "hour")
        assert result == datetime(2023, 3, 15, 15)

    # Invalid
    def test_invalid_resolution(self):
        with pytest.raises(ValueError, match="Resolution must be one of"):
            round_datetime(datetime(2023, 1, 1), "second")


# --- add_flows_to_characterization_functions ---


class TestAddFlowsToCharacterizationFunctions:
    def test_single_flow(self):
        func = lambda x: x
        result = add_flows_to_characterization_functions("CO2", func)
        assert result == {"CO2": func}

    def test_list_of_flows(self):
        func = lambda x: x
        result = add_flows_to_characterization_functions(["CO2", "CH4"], func)
        assert result == {"CO2": func, "CH4": func}

    def test_extends_existing_dict(self):
        func1 = lambda x: x
        func2 = lambda x: x * 2
        existing = {"CO2": func1}
        result = add_flows_to_characterization_functions("CH4", func2, existing)
        assert result == {"CO2": func1, "CH4": func2}
        assert result is existing  # same dict object

    def test_empty_list(self):
        func = lambda x: x
        result = add_flows_to_characterization_functions([], func)
        assert result == {}


# --- get_temporal_evolution_factor ---


class TestGetTemporalEvolutionFactor:
    def test_none_returns_one(self):
        assert get_temporal_evolution_factor(None, datetime(2023, 1, 1)) == 1.0

    def test_empty_dict_returns_one(self):
        assert get_temporal_evolution_factor({}, datetime(2023, 1, 1)) == 1.0

    def test_single_entry(self):
        te = {datetime(2023, 1, 1): 2.0}
        assert get_temporal_evolution_factor(te, datetime(2025, 6, 1)) == 2.0

    def test_clamp_below_minimum(self):
        te = {
            datetime(2023, 1, 1): 1.0,
            datetime(2025, 1, 1): 3.0,
        }
        assert get_temporal_evolution_factor(te, datetime(2020, 1, 1)) == 1.0

    def test_clamp_above_maximum(self):
        te = {
            datetime(2023, 1, 1): 1.0,
            datetime(2025, 1, 1): 3.0,
        }
        assert get_temporal_evolution_factor(te, datetime(2030, 1, 1)) == 3.0

    def test_exact_boundary_lower(self):
        te = {
            datetime(2023, 1, 1): 1.0,
            datetime(2025, 1, 1): 3.0,
        }
        assert get_temporal_evolution_factor(te, datetime(2023, 1, 1)) == 1.0

    def test_exact_boundary_upper(self):
        te = {
            datetime(2023, 1, 1): 1.0,
            datetime(2025, 1, 1): 3.0,
        }
        assert get_temporal_evolution_factor(te, datetime(2025, 1, 1)) == 3.0

    def test_linear_interpolation_midpoint(self):
        te = {
            datetime(2020, 1, 1): 1.0,
            datetime(2030, 1, 1): 3.0,
        }
        result = get_temporal_evolution_factor(te, datetime(2025, 1, 1))
        assert result == pytest.approx(2.0, rel=0.01)

    def test_interpolation_multiple_segments(self):
        te = {
            datetime(2020, 1, 1): 0.0,
            datetime(2022, 1, 1): 2.0,
            datetime(2024, 1, 1): 4.0,
        }
        # Midpoint of first segment
        result = get_temporal_evolution_factor(te, datetime(2021, 1, 1))
        assert result == pytest.approx(1.0, rel=0.01)
        # Midpoint of second segment
        result = get_temporal_evolution_factor(te, datetime(2023, 1, 1))
        assert result == pytest.approx(3.0, rel=0.01)


# --- resolve_temporalized_node_name (DB-dependent) ---


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestResolveTemporalizedNodeName:
    def test_unique_code(self):
        assert resolve_temporalized_node_name("A") == "A"

    def test_shared_code_same_name(self):
        # "C" exists in both db_2022 and db_2024 with the same name "C"
        assert resolve_temporalized_node_name("C") == "C"

    def test_unknown_code(self):
        with pytest.raises(UnknownObject):
            resolve_temporalized_node_name("nonexistent_code")


# --- get_exchange (DB-dependent) ---


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestGetExchange:
    def test_get_by_codes(self):
        exc = get_exchange(
            input_code="B",
            input_database="foreground",
            output_code="A",
            output_database="foreground",
        )
        assert exc["amount"] == 1

    def test_get_by_node_objects(self):
        node_a = bd.get_node(database="foreground", code="A")
        node_b = bd.get_node(database="foreground", code="B")
        exc = get_exchange(input_node=node_b, output_node=node_a)
        assert exc["type"] == "technosphere"

    def test_no_match_raises(self):
        with pytest.raises(UnknownObject, match="No exchange found"):
            get_exchange(
                input_code="nonexistent",
                input_database="foreground",
                output_code="A",
                output_database="foreground",
            )

    def test_multiple_matches_raises(self):
        # Querying with only output info should match multiple exchanges
        with pytest.raises(MultipleResults):
            get_exchange(
                output_code="A",
                output_database="foreground",
            )


# --- add_temporal_distribution_to_exchange (DB-dependent) ---


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestAddTemporalDistributionToExchange:
    def test_add_td(self):
        td = TemporalDistribution(
            np.array([0, 1, 2], dtype="timedelta64[Y]"),
            np.array([0.5, 0.3, 0.2]),
        )
        add_temporal_distribution_to_exchange(
            td,
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        exc = get_exchange(
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        assert exc.get("temporal_distribution") is not None
        assert len(exc["temporal_distribution"].amount) == 3


# --- add_temporal_evolution_to_exchange (DB-dependent) ---


@pytest.mark.usefixtures("temporal_grouping_db_monthly")
class TestAddTemporalEvolutionToExchange:
    def test_mutually_exclusive_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            add_temporal_evolution_to_exchange(
                temporal_evolution_factors={datetime(2020, 1, 1): 1.0},
                temporal_evolution_amounts={datetime(2020, 1, 1): 5.0},
                input_code="CO2",
                input_database="bio",
                output_code="C",
                output_database="db_2024",
            )

    def test_add_factors(self):
        factors = {datetime(2020, 1, 1): 1.0, datetime(2030, 1, 1): 2.0}
        add_temporal_evolution_to_exchange(
            temporal_evolution_factors=factors,
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        exc = get_exchange(
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        assert exc.get("temporal_evolution_factors") == factors

    def test_add_amounts(self):
        amounts = {datetime(2020, 1, 1): 5.0, datetime(2030, 1, 1): 10.0}
        add_temporal_evolution_to_exchange(
            temporal_evolution_amounts=amounts,
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        exc = get_exchange(
            input_code="CO2",
            input_database="bio",
            output_code="C",
            output_database="db_2024",
        )
        assert exc.get("temporal_evolution_amounts") == amounts
