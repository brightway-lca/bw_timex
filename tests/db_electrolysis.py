import bw2data as bd
import numpy as np

from bw_temporalis import easy_timedelta_distribution, TemporalDistribution, easy_datetime_distribution

def db_electrolysis():
    if "__test_electrolysis__" in bd.projects:
        bd.projects.delete_project("__test_electrolysis__")
        bd.projects.purge_deleted_directories()

    bd.projects.set_current("__test_electrolysis__")

    bd.Database('temporalis-bio').write({
        ('temporalis-bio', "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
            "temporalis code": "co2",
        },
    })

    bd.Database('background_2024').write({
        ('background_2024', 'electricity_mix'): {
            'name': 'Electricity mix',
            'location': 'somewhere',
            'reference product': 'electricity mix',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2024', 'electricity_mix'),
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
                    'amount': 0.9,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('background_2020').write({
        ('background_2020', 'electricity_mix'): {
            'name': 'Electricity mix',
            'location': 'somewhere',
            'reference product': 'electricity mix',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2020', 'electricity_mix'),
                },
                {
                    'amount': 1,
                    'type': 'technosphere',
                    'input': ('background_2020', 'electricity_wind'),
                },
            ]
        },
        ('background_2020', 'electricity_wind'): {
            'name': 'Electricity production, wind',
            'location': 'somewhere',
            'reference product': 'electricity, wind',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('background_2020', 'electricity_wind'),
                },
                {
                    'amount': 1.2,
                    'type': 'biosphere',
                    'input': ('temporalis-bio', 'CO2'),
                },
            ]
        }
    })

    bd.Database('foreground').write({
        ('foreground', 'electrolysis'): {
            'name': 'Hydrogen production, electrolysis',
            'location': 'somewhere',
            'reference product': 'hydrogen',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'electrolysis'),
                },
                {
                    'amount': 5,
                    'type': 'technosphere',
                    'input': ('background_2024', 'electricity_mix'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-1, 0, 1], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.2, 0.6, 0.2])
                        ),
                    
                },
            ]
        },
        ('foreground', 'electrolysis2'): {
            'name': 'Hydrogen production, electrolysis2',
            'location': 'somewhere',
            'reference product': 'hydrogen',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'electrolysis2'),
                },
                {
                    'amount': 13,
                    'type': 'technosphere',
                    'input': ('foreground', 'electrolysis'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-3], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([1])
                        ),
                },
            ]
        },
        ('foreground', 'heat_from_hydrogen'): {
            'name': 'Heat production, hydrogen',
            'location': 'somewhere',
            'reference product': 'heat',
            'exchanges': [
                {
                    'amount': 1,
                    'type': 'production',
                    'input': ('foreground', 'heat_from_hydrogen'),
                },
                {
                    'amount': 0.7,
                    'type': 'technosphere',
                    'input': ('foreground', 'electrolysis2'),
                    'temporal_distribution': # e.g. because some hydrogen was stored in the meantime
                        TemporalDistribution(
                            date=np.array([-2, 0], dtype='timedelta64[Y]'),  # `M` is months
                            amount=np.array([0.9, 0.1])
                        ),
                },
            ]
        },
    })
    
    bd.Method(("GWP", "example")).write([
        (('temporalis-bio', "CO2"), 1),
    ])