from datetime import datetime

import bw2data as bd
import numpy as np
from bw_temporalis import TemporalDistribution

from bw_timex import TimexLCA


def _td_final_to_middle() -> TemporalDistribution:
    return TemporalDistribution(
        date=np.array(["2027-01-01", "2029-01-01"], dtype="datetime64[s]"),
        amount=np.array([0.25, 0.75]),
    )


def _td_component_to_background() -> TemporalDistribution:
    return TemporalDistribution(
        date=np.array(["2020-01-01", "2035-01-01"], dtype="datetime64[s]"),
        amount=np.array([0.1, 0.9]),
    )


def _write_absolute_td_supply_chain_db(
    middle_to_component_td: TemporalDistribution,
    include_component_process: bool = False,
) -> None:
    bd.Database("bio").write(
        {("bio", "co2"): {"name": "carbon dioxide", "unit": "kg", "type": "emission"}}
    )
    bd.Database("background").write(
        {
            ("background", "input"): {
                "name": "background input",
                "reference product": "background input",
                "unit": "kg",
                "location": "GLO",
                "exchanges": [
                    {"input": ("background", "input"), "amount": 1, "type": "production"},
                    {"input": ("bio", "co2"), "amount": 1, "type": "biosphere"},
                ],
            }
        }
    )
    bd.Method(("GWP", "example")).write([(("bio", "co2"), 1.0)])

    foreground = {
        ("foreground", "final"): {
            "name": "final process",
            "reference product": "final product",
            "unit": "unit",
            "location": "GLO",
            "exchanges": [
                {"input": ("foreground", "final"), "amount": 1, "type": "production"},
                {
                    "input": ("foreground", "middle"),
                    "amount": 2,
                    "type": "technosphere",
                    "temporal_distribution": _td_final_to_middle(),
                },
            ],
        },
        ("foreground", "middle"): {
            "name": "middle process",
            "reference product": "middle product",
            "unit": "unit",
            "location": "GLO",
            "exchanges": [
                {"input": ("foreground", "middle"), "amount": 1, "type": "production"},
                {
                    "input": (
                        ("foreground", "component")
                        if include_component_process
                        else ("background", "input")
                    ),
                    "amount": 5,
                    "type": "technosphere",
                    "temporal_distribution": middle_to_component_td,
                },
            ],
        },
    }

    if include_component_process:
        foreground[("foreground", "component")] = {
            "name": "component process",
            "reference product": "component product",
            "unit": "unit",
            "location": "GLO",
            "exchanges": [
                {
                    "input": ("foreground", "component"),
                    "amount": 1,
                    "type": "production",
                },
                {
                    "input": ("background", "input"),
                    "amount": 7,
                    "type": "technosphere",
                    "temporal_distribution": _td_component_to_background(),
                },
            ],
        }

    bd.Database("foreground").write(foreground)
    for db in bd.databases:
        bd.Database(db).process()


def _build_absolute_td_supply_chain_timeline() -> TimexLCA:
    final = bd.get_node(database="foreground", code="final")
    tlca = TimexLCA(
        demand={final.key: 1},
        method=("GWP", "example"),
        database_dates={
            "background": datetime(2030, 1, 1),
            "foreground": "dynamic",
        },
    )
    tlca.build_timeline(starting_datetime=datetime(2030, 1, 1))
    return tlca


def _date_strings(dates: np.ndarray) -> list[str]:
    return [np.datetime_as_string(date, unit="D") for date in np.sort(dates)]


def _timeline_producer_dates(tlca: TimexLCA, producer_name: str) -> list[str]:
    rows = tlca.timeline[tlca.timeline["producer_name"] == producer_name]
    return _date_strings(rows["date_producer"].to_numpy(dtype="datetime64[s]"))


def _timeline_rows(tlca: TimexLCA, producer_name: str) -> list[tuple[str, str, float]]:
    rows = tlca.timeline[tlca.timeline["producer_name"] == producer_name]
    return sorted(
        (
            row.date_consumer.strftime("%Y-%m-%d"),
            row.date_producer.strftime("%Y-%m-%d"),
            round(float(row.amount), 12),
        )
        for row in rows.itertuples()
    )


def test_absolute_td_then_relative_td_in_supply_chain():
    bd.projects.set_current("__test_absolute_then_relative_rtd_supply_chain__")
    bd.databases.clear()
    bd.methods.clear()

    td_middle_to_background = TemporalDistribution(
        date=np.array([0, 1], dtype="timedelta64[Y]"),
        amount=np.array([0.4, 0.6]),
    )
    _write_absolute_td_supply_chain_db(
        middle_to_component_td=td_middle_to_background
    )
    tlca = _build_absolute_td_supply_chain_timeline()

    expected_background_td = _td_final_to_middle() * td_middle_to_background
    assert _timeline_producer_dates(tlca, "background input") == _date_strings(
        expected_background_td.date
    )
    assert _timeline_rows(tlca, "middle process") == [
        ("2030-01-01", "2027-01-01", 0.5),
        ("2030-01-01", "2029-01-01", 1.5),
    ]
    assert _timeline_rows(tlca, "background input") == [
        ("2027-01-01", "2027-01-01", 2.0),
        ("2027-01-01", "2028-01-01", 3.0),
        ("2029-01-01", "2029-01-01", 2.0),
        ("2029-01-01", "2030-01-01", 3.0),
    ]


def test_multiple_absolute_tds_in_sequence_broadcast_to_each_consumer_date():
    bd.projects.set_current("__test_sequential_absolute_rtd_supply_chain__")
    bd.databases.clear()
    bd.methods.clear()

    td_middle_to_component = TemporalDistribution(
        date=np.array(["2028-01-01", "2031-01-01"], dtype="datetime64[s]"),
        amount=np.array([0.2, 0.8]),
    )
    _write_absolute_td_supply_chain_db(
        middle_to_component_td=td_middle_to_component,
        include_component_process=True,
    )
    tlca = _build_absolute_td_supply_chain_timeline()

    expected_component_td = _td_final_to_middle() * td_middle_to_component
    assert sorted(set(_timeline_producer_dates(tlca, "component process"))) == (
        _date_strings(expected_component_td.date)
    )
    expected_background_td = td_middle_to_component * _td_component_to_background()
    assert sorted(set(_timeline_producer_dates(tlca, "background input"))) == (
        _date_strings(expected_background_td.date)
    )
    assert _timeline_rows(tlca, "middle process") == [
        ("2030-01-01", "2027-01-01", 0.5),
        ("2030-01-01", "2029-01-01", 1.5),
    ]
    assert _timeline_rows(tlca, "component process") == [
        ("2027-01-01", "2028-01-01", 1.0),
        ("2027-01-01", "2031-01-01", 4.0),
        ("2029-01-01", "2028-01-01", 1.0),
        ("2029-01-01", "2031-01-01", 4.0),
    ]
    assert _timeline_rows(tlca, "background input") == [
        ("2028-01-01", "2020-01-01", 0.7),
        ("2028-01-01", "2035-01-01", 6.3),
        ("2031-01-01", "2020-01-01", 0.7),
        ("2031-01-01", "2035-01-01", 6.3),
    ]
