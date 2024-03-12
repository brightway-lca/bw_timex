import bw2data as bd
import numpy as np

from bw_temporalis import (
    easy_timedelta_distribution,
    TemporalDistribution,
    easy_datetime_distribution,
)


def db_electrolysis():
    if "__test_electrolysis__" in bd.projects:
        bd.projects.delete_project("__test_electrolysis__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_electrolysis__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "electricity_mix"): {
                "name": "Electricity mix",
                "location": "somewhere",
                "reference product": "electricity mix",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_mix"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2020").write(
        {
            ("background_2020", "electricity_mix"): {
                "name": "Electricity mix",
                "location": "somewhere",
                "reference product": "electricity mix",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2020", "electricity_mix"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2020", "electricity_wind"),
                    },
                ],
            },
            ("background_2020", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2020", "electricity_wind"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "electrolysis"): {
                "name": "Hydrogen production, electrolysis",
                "location": "somewhere",
                "reference product": "hydrogen",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "electrolysis"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_mix"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-1, 0, 1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.2, 0.6, 0.2]),
                        ),
                    },
                ],
            },
            ("foreground", "electrolysis2"): {
                "name": "Hydrogen production, electrolysis2",
                "location": "somewhere",
                "reference product": "hydrogen",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "electrolysis2"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("foreground", "electrolysis"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-3], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "heat_from_hydrogen"): {
                "name": "Heat production, hydrogen",
                "location": "somewhere",
                "reference product": "heat",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "heat_from_hydrogen"),
                    },
                    {
                        "amount": 0.7,
                        "type": "technosphere",
                        "input": ("foreground", "electrolysis2"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, 0], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.9, 0.1]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_simple():
    if "__test_abc_simple__" in bd.projects:
        bd.projects.delete_project("__test_abc_simple__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_simple__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1.2,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-20, 15], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.9, 0.1]),
                        ),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_C_to_E():
    if "__test_abc_C_to_E__" in bd.projects:
        bd.projects.delete_project("__test_abc_C_to_E__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_C_to_E__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1.2,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                    {
                        "amount": 11,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([10, 5], dtype="timedelta64[Y]"),
                            amount=np.array([0.3, 0.7]),
                        ),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-20, 15], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.9, 0.1]),
                        ),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_C_and_B_to_E():
    if "__test_abc_C_and_B_to_E__" in bd.projects:
        bd.projects.delete_project("__test_abc_C_and_B_to_E__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_C_and_B_to_E__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1.2,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                    {
                        "amount": 11,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([10, 5], dtype="timedelta64[Y]"),
                            amount=np.array([0.3, 0.7]),
                        ),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-4, -2], dtype="timedelta64[Y]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-20, 15], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.9, 0.1]),
                        ),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_B_to_E():
    if "__test_abc_B_to_E__" in bd.projects:
        bd.projects.delete_project("__test_abc_B_to_E__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_B_to_E__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1.2,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-4, -2], dtype="timedelta64[Y]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 4,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-20, 15], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.9, 0.1]),
                        ),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_B_to_E_simplified():
    if "__test_abc_B_to_E_simplified__" in bd.projects:
        bd.projects.delete_project("__test_abc_B_to_E_simplified__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_B_to_E_simplified__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-4, -2], dtype="timedelta64[Y]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_B_to_E_simplified_and_E_with_TD():
    if "__test_abc_B_to_E_simplified_and_E_with_TD__" in bd.projects:
        bd.projects.delete_project("__test_abc_B_to_E_simplified_and_E_with_TD__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_B_to_E_simplified_and_E_with_TD__")

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
        }
    )

    bd.Database("background_2024").write(
        {
            ("background_2024", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2024", "electricity_wind"),
                    },
                ],
            },
            ("background_2024", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2024", "electricity_wind"),
                    },
                    {
                        "amount": 0.9,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("background_2008").write(
        {
            ("background_2008", "C"): {
                "name": "C",
                "location": "somewhere",
                "reference product": "c",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "C"),
                    },
                    {
                        "amount": 1,
                        "type": "technosphere",
                        "input": ("background_2008", "electricity_wind"),
                    },
                ],
            },
            ("background_2008", "electricity_wind"): {
                "name": "Electricity production, wind",
                "location": "somewhere",
                "reference product": "electricity, wind",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("background_2008", "electricity_wind"),
                    },
                    {
                        "amount": 1.2,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                    },
                ],
            },
        }
    )

    bd.Database("foreground").write(
        {
            ("foreground", "E"): {
                "name": "E",
                "location": "somewhere",
                "reference product": "e",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "E"),
                    },
                    {
                        "amount": 8,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-4, -2], dtype="timedelta64[Y]"),
                            amount=np.array([0.4, 0.6]),
                        ),
                    },
                ],
            },
            ("foreground", "D"): {
                "name": "D",
                "location": "somewhere",
                "reference product": "d",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "D"),
                    },
                    {
                        "amount": 5,
                        "type": "technosphere",
                        "input": ("foreground", "B"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                    {
                        "amount": 2,
                        "type": "technosphere",
                        "input": ("foreground", "E"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array(
                                [
                                    -1,
                                ],
                                dtype="timedelta64[Y]",
                            ),
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "B"): {
                "name": "B",
                "location": "somewhere",
                "reference product": "b",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "B"),
                    },
                    {
                        "amount": 13,
                        "type": "technosphere",
                        "input": ("background_2024", "C"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-5], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([1]),
                        ),
                    },
                ],
            },
            ("foreground", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("foreground", "A"),
                    },
                    {
                        "amount": 0.5,
                        "type": "technosphere",
                        "input": ("foreground", "D"),
                        "temporal_distribution": TemporalDistribution(  # e.g. because some hydrogen was stored in the meantime
                            date=np.array(
                                [-2, -1], dtype="timedelta64[Y]"
                            ),  # `M` is months
                            amount=np.array([0.7, 0.3]),
                        ),
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )


def db_abc_loopA():
    if "__test_abc_loopA__" in bd.projects:
        bd.projects.delete_project("__test_abc_loopA__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_loopA__")

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'E'): {
                'name': 'E',
                'location': 'somewhere',
                'reference product': 'e',
                'exchanges': [
                    {
                        'amount': 1,
                        'type': 'production',
                        'input': ('foreground', 'E'),
                    },
                    {
                        'amount': 11,
                        'type': 'technosphere',
                        'input': ('background_2024', 'C'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([10, 5], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.3,0.7])
                            ),     
                    },
                    {
                        'amount': 8,
                        'type': 'technosphere',
                        'input': ('foreground', 'B'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([-4,-2], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.4,0.6])
                            ),
                        
                    },
                    {
                        'amount': 0.1,
                        'type': 'technosphere',
                        'input': ('foreground', 'A'),
                        # 'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        #     TemporalDistribution(
                        #         date=np.array([-3,-1], dtype='timedelta64[Y]'),  # `M` is months
                        #         amount=np.array([0.2,0.8])
                        #     ),
                        
                    },
                ]
            },



        ('foreground', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'D'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-1,], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),
                    
                },
                {
                    'amount': 2,
                    'type': 'technosphere',
                    'input': ('foreground', 'E'),
                },
            ]
        },
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-5], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),     
                },
            ]
        },
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-20, 15], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.9, 0.1])
                        ),
                },
                {
                    'amount': 0.5,
                    'type': 'technosphere',
                    'input': ('foreground', 'D'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, -1], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.7, 0.3])
                        ),
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )
    

def db_abc_loopA_with_biosphere():
    if "__test_abc_loopA_with_biosphere__" in bd.projects:
        bd.projects.delete_project("__test_abc_loopA_with_biosphere__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_abc_loopA_with_biosphere__")

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'E'): {
                'name': 'E',
                'location': 'somewhere',
                'reference product': 'e',
                'exchanges': [
                    {
                        'amount': 1,
                        'type': 'production',
                        'input': ('foreground', 'E'),
                    },
                    {
                        'amount': 11,
                        'type': 'technosphere',
                        'input': ('background_2024', 'C'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([10, 5], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.3,0.7])
                            ),     
                    },
                    {
                        'amount': 8,
                        'type': 'technosphere',
                        'input': ('foreground', 'B'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([-4,-2], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.4,0.6])
                            ),
                        
                    },
                    {
                        'amount': 0.1,
                        'type': 'technosphere',
                        'input': ('foreground', 'A'),
                        # 'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        #     TemporalDistribution(
                        #         date=np.array([-3,-1], dtype='timedelta64[Y]'),  # `M` is months
                        #         amount=np.array([0.2,0.8])
                        #     ),
                        
                    },
                ]
            },



        ('foreground', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'D'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-1,], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),
                    
                },
                {
                    'amount': 2,
                    'type': 'technosphere',
                    'input': ('foreground', 'E'),
                },
            ]
        },
        
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-5], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),     
                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 17,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-20, 15], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.9, 0.1])
                        ),
                },
                {
                    'amount': 0.5,
                    'type': 'technosphere',
                    'input': ('foreground', 'D'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, -1], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.7, 0.3])
                        ),
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )

def db_abc_loopA_with_biosphere_tds_CO2_and_CH4():
    project_name = "db_abc_loopA_with_biosphere_tds_CO2_and_CH4"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'E'): {
                'name': 'E',
                'location': 'somewhere',
                'reference product': 'e',
                'exchanges': [
                    {
                        'amount': 1,
                        'type': 'production',
                        'input': ('foreground', 'E'),
                    },
                    {
                        'amount': 11,
                        'type': 'technosphere',
                        'input': ('background_2024', 'C'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([10, 5], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.3,0.7])
                            ),     
                    },
                    {
                        'amount': 8,
                        'type': 'technosphere',
                        'input': ('foreground', 'B'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([-4,-2], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.4,0.6])
                            ),
                        
                    },
                    {
                        'amount': 0.1,
                        'type': 'technosphere',
                        'input': ('foreground', 'A'),
                        # 'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        #     TemporalDistribution(
                        #         date=np.array([-3,-1], dtype='timedelta64[Y]'),  # `M` is months
                        #         amount=np.array([0.2,0.8])
                        #     ),
                        
                    },
                ]
            },



        ('foreground', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'D'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-1,], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),
                    
                },
                {
                    'amount': 2,
                    'type': 'technosphere',
                    'input': ('foreground', 'E'),
                },
            ]
        },
        
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-5], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),     
                },
                {
                    'amount': 6,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CH4'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, 4, 6], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.2, 0.7, 0.1])
                        ),
                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 17,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([0, 3], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.3, 0.7])
                        ),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-20, 15], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.9, 0.1])
                        ),
                },
                {
                    'amount': 0.5,
                    'type': 'technosphere',
                    'input': ('foreground', 'D'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, -1], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.7, 0.3])
                        ),
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
            (("temporalis-bio", "CH4"), 25),
        ]
    )

def db_abcd_CO2_foreground_and_in_deep_background():
    project_name = "db_abcd_CO2_foreground_and_in_deep_background"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            # no biosphere in intersecting node between background and foreground
            ]
        },
        
        # ('background_2024', 'D'): {
        #     'name': 'D',
        #     'location': 'somewhere',
        #     'reference product': 'd',
        #     'exchanges': [
        #         {
        #             'amount': 1,
        #             'type': 'production',
        #             'input': ('background_2024', 'D'),
        #         },
        #         {
        #             'amount': 3,
        #             'type': 'technosphere',
        #             'input': ('background_2024', 'electricity_wind'),
        #         },
        #     ]
        # },
                
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
             # no biosphere in intersecting node between background and foreground
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        },
                
        # ('background_2008', 'D'): {
        #     'name': 'D',
        #     'location': 'somewhere',
        #     'reference product': 'd',
        #     'exchanges': [
        #         {
        #             'amount': 1,
        #             'type': 'production',
        #             'input': ('background_2008', 'D'),
        #         },
        #         {
        #             'amount': 3,
        #             'type': 'technosphere',
        #             'input': ('background_2008', 'electricity_wind'),
        #         },
        #     ]
        # },
    })

    bd.Database('foreground').write({
    
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 3,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),     
                },
                

                {
                    'amount': 6,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CH4'),
                    # 'temporal_distribution': 
                    #     TemporalDistribution(
                    #         date=np.array([-2, 4, 6], dtype='timedelta64[Y]'), 
                    #         amount=np.array([0.2, 0.7, 0.1])
                    #     ),
                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 3,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    # 'temporal_distribution': 
                    #     TemporalDistribution(
                    #         date=np.array([0, 1], dtype='timedelta64[Y]'),  
                    #         amount=np.array([0.3, 0.7])
                    #     ),
                },
                {
                    'amount': 0.3,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    # 'temporal_distribution':
                    #     TemporalDistribution(
                    #         date=np.array([-20, 15], dtype='timedelta64[Y]'), 
                    #         amount=np.array([0.9, 0.1])
                    #     ),
                },
                
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
            (("temporalis-bio", "CH4"), 25),
        ]
    )



def db_abcd_CO2_only_in_deep_background():
    project_name = "db_abcd_CO2_only_in_deep_background"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            # no biosphere in intersecting node between background and foreground
            ]
        },
        
                
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
             # no biosphere in intersecting node between background and foreground
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        },
                
    })

    bd.Database('foreground').write({
    
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 3,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),     
                },
                
                # no biosphere in foreground
                
                # {
                #     'amount': 6,
                #     'type': 'biosphere',
                #     'input': ('temporalis-bio', 'CH4'),
                #     # 'temporal_distribution': 
                #     #     TemporalDistribution(
                #     #         date=np.array([-2, 4, 6], dtype='timedelta64[Y]'), 
                #     #         amount=np.array([0.2, 0.7, 0.1])
                #     #     ),
                # },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                
                # no biosphere in foreground
                # {
                #     'amount': 3,
                #     'type': 'biosphere',
                #     'input': ('temporalis-bio', 'CO2'),
                #     # 'temporal_distribution': 
                #     #     TemporalDistribution(
                #     #         date=np.array([0, 1], dtype='timedelta64[Y]'),  
                #     #         amount=np.array([0.3, 0.7])
                #     #     ),
                # },
                {
                    'amount': 0.3,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    # 'temporal_distribution':
                    #     TemporalDistribution(
                    #         date=np.array([-20, 15], dtype='timedelta64[Y]'), 
                    #         amount=np.array([0.9, 0.1])
                    #     ),
                },
                
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
            (("temporalis-bio", "CH4"), 25),
        ]
    )

def db_abcd_CO2_foreground_deep_background_and_two_markets():
    project_name = "db_abcd_CO2_foreground_deep_background_and_two_markets"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
    # biosphere in intersecting process (market)
                
                {
                    'amount': 5,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        },
        
                
        ('background_2024', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'D'),
                },
                {
                    'amount': 3,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1.5,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
    # biosphere in intersecting process (market)
                {
                    'amount': 5,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        },
        ('background_2008', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'D'),
                },
                {
                    'amount': 3,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                {
                    'amount': 1.5,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),     
                },
                
                {
                    'amount': 6,
                    'type': 'technosphere',
                    'input': ('background_2024', 'D'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-6], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),     
                },

            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 0,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    # 'temporal_distribution': 
                    #     TemporalDistribution(
                    #         date=np.array([0, 1], dtype='timedelta64[Y]'),  
                    #         amount=np.array([0.3, 0.7])
                    #     ),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    # 'temporal_distribution':
                    #     TemporalDistribution(
                    #         date=np.array([-20, 15], dtype='timedelta64[Y]'), 
                    #         amount=np.array([0.9, 0.1])
                    #     ),
                },
                
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
            (("temporalis-bio", "CH4"), 25),
        ]
    )


def db_abc_loopA_with_biosphere_advanced():

    project_name = "___abc_loopA_with_biosphere_advanced__"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        #bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2, 1, 2], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                 {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2, 1, 2], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'E'): {
                'name': 'E',
                'location': 'somewhere',
                'reference product': 'e',
                'exchanges': [
                    {
                        'amount': 1,
                        'type': 'production',
                        'input': ('foreground', 'E'),
                    },
                    {
                        'amount': 11,
                        'type': 'technosphere',
                        'input': ('background_2024', 'C'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([10, 5], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.3,0.7])
                            ),     
                    },
                    {
                        'amount': 8,
                        'type': 'technosphere',
                        'input': ('foreground', 'B'),
                        'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                            TemporalDistribution(
                                date=np.array([-4,-2], dtype='timedelta64[Y]'),  # `M` is months
                                amount=np.array([0.4,0.6])
                            ),
                        
                    },
                    {
                        'amount': 0.1,
                        'type': 'technosphere',
                        'input': ('foreground', 'A'),
                        # 'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        #     TemporalDistribution(
                        #         date=np.array([-3,-1], dtype='timedelta64[Y]'),  # `M` is months
                        #         amount=np.array([0.2,0.8])
                        #     ),
                        
                    },
                ]
            },



        ('foreground', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'D'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-1,], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),
                    
                },
                {
                    'amount': 2,
                    'type': 'technosphere',
                    'input': ('foreground', 'E'),
                },
            ]
        },
        
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-5], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),     
                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 17,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-20, 15], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.9, 0.1])
                        ),
                },
                {
                    'amount': 0.5,
                    'type': 'technosphere',
                    'input': ('foreground', 'D'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, -1], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.7, 0.3])
                        ),
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )



def db_abc_loopA_with_biosphere_advanced_simple():

    project_name = "___abc_loopA_with_biosphere_advanced_simple__"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        #bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },

        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2, 1, 2], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                 {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2, 1, 2], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
            ]
        }
    })

    bd.Database('foreground').write({
    
        ('foreground', 'E'): {
                'name': 'E',
                'location': 'somewhere',
                'reference product': 'e',
                'exchanges': [
                    {
                        'amount': 1,
                        'type': 'production',
                        'input': ('foreground', 'E'),
                    },
                    {
                        'amount': 11,
                        'type': 'technosphere',
                        'input': ('background_2024', 'C'),
    
                    },
                    {
                        'amount': 8,
                        'type': 'technosphere',
                        'input': ('foreground', 'B'),

                        
                    },
                    {
                        'amount': 0.1,
                        'type': 'technosphere',
                        'input': ('foreground', 'A'),
                        # 'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        #     TemporalDistribution(
                        #         date=np.array([-3,-1], dtype='timedelta64[Y]'),  # `M` is months
                        #         amount=np.array([0.2,0.8])
                        #     ),
                        
                    },
                ]
            },



        ('foreground', 'D'): {
            'name': 'D',
            'location': 'somewhere',
            'reference product': 'd',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'D'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
           
                    
                },
                {
                    'amount': 2,
                    'type': 'technosphere',
                    'input': ('foreground', 'E'),
                },
            ]
        },
        
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
         
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CH4'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2, ], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),

                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 17,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
       
                },
                {
                    'amount': 0.5,
                    'type': 'technosphere',
                    'input': ('foreground', 'D'),
         
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )



def db_dynamic_cf_test():

    project_name = "___db_dynamic_cf_test__"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        #bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },

        ('temporalis-bio', "CH4"): {
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },

        ('temporalis-bio', "CO"): {
            "type": "emission",
            "name": "carbon monoxide",
            "temporalis code": "co",
        },

        ('temporalis-bio', "N2O"): {
            "type": "emission",
            "name": "nitrous oxide",
            "temporalis code": "n2o",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_wind'),
                },
            ]
        },
        ('background_2024', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_wind'),
                },
                {
                    'amount': 12,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-20, 10, 20], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),

                },
            ]
        }
    })

    bd.Database('background_2008').write({
        ('background_2008', 'C'): {
            'name': 'C',
            'location': 'somewhere',
            'reference product': 'c',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'C'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2008', 'electricity_wind'),
                },
            ]
        },
        ('background_2008', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2008', 'electricity_wind'),
                },
                 {
                    'amount': 12,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-20, 10, 20], dtype='timedelta64[Y]'), 
                            amount=np.array([0.2, 0.7, 0.1])
                        ),

                },
                                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),

                },
            ]
        }
    })

    bd.Database('foreground').write({
        
        ('foreground', 'B'): {
            'name': 'B',
            'location': 'somewhere',
            'reference product': 'b',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'B'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('background_2024', 'C'),
         
                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CH4'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-2], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),

                },
                {
                    'amount': 1,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'N2O'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([4], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),

                },
            ]
        },
        
        ('foreground', 'A'): {
            'name': 'A',
            'location': 'somewhere',
            'reference product': 'a',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'A'),
                },
                {
                    'amount': 17,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-1], dtype='timedelta64[Y]'), 
                            amount=np.array([1])
                        ),
                },
                {
                    'amount': 4,
                    'type': 'technosphere',
                    'input': ('foreground', 'B'),
                    'temporal_distribution': 
                        TemporalDistribution(
                            date=np.array([-25, 0], dtype='timedelta64[Y]'), 
                            amount=np.array([0.5, 0.5])
                        ),
       
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CO2"), 1),
        ]
    )

