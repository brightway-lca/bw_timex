"""Tests for what `bw_timex` exposes at the top level."""

import bw_temporalis
import pytest

import bw_timex
from bw_timex import utils

FORWARDED_FROM_TEMPORALIS = [
    "TemporalDistribution",
    "easy_datetime_distribution",
    "easy_timedelta_distribution",
]

EXPOSED_UTILS = [
    "add_flows_to_characterization_functions",
    "add_temporal_distribution_to_exchange",
    "add_temporal_evolution_to_exchange",
    "get_exchange",
    "get_temporal_evolution_factor",
    "interactive_td_widget",
    "plot_characterized_inventory_as_waterfall",
]


@pytest.mark.parametrize("name", FORWARDED_FROM_TEMPORALIS)
def test_temporalis_objects_are_forwarded(name):
    assert getattr(bw_timex, name) is getattr(bw_temporalis, name)


@pytest.mark.parametrize("name", EXPOSED_UTILS)
def test_utils_are_exposed_at_top_level(name):
    assert getattr(bw_timex, name) is getattr(utils, name)


def test_all_names_are_importable():
    for name in bw_timex.__all__:
        assert hasattr(bw_timex, name), f"{name} in __all__ but not importable"
