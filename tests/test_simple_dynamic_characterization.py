"""
Very basic test that checks if the dynamic characterization runs. This does not (yet) fully check the correctness of the results.
"""

from datetime import datetime

import bw2data as bd
import numpy as np
import pandas as pd
import pytest

from bw_timex import TimexLCA


def characterize_something(
    series,
    period: int = 100,
    cumulative=False,
) -> pd.DataFrame:

    radiative_efficiency_kg = 2e-13  # W/m2/kg-CH4

    tau = 100  # Lifetime (years)

    date_beginning: np.datetime64 = series["date"].to_numpy()
    date_characterized: np.ndarray = date_beginning + np.arange(
        start=0, stop=period, dtype="timedelta64[Y]"
    ).astype("timedelta64[s]")

    decay_multipliers: list = np.array(
        [
            radiative_efficiency_kg * tau * (1 - np.exp(-year / tau))
            for year in range(period)
        ]
    )

    forcing = pd.Series(data=series.amount * decay_multipliers, dtype="float64")

    if not cumulative:
        forcing = forcing.diff(periods=1).fillna(0)

    return pd.DataFrame(
        {
            "date": pd.Series(data=date_characterized, dtype="datetime64[s]"),
            "amount": forcing,
            "flow": series.flow,
            "activity": series.activity,
        }
    )


def test_nonunitary_db_fixture(delayed_pulse_emission_db):
    assert len(bd.databases) == 2


@pytest.mark.usefixtures("delayed_pulse_emission_db")
class TestClassDynamicCharacterization:

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.node_a = bd.get_activity(("test", "A"))

        self.tlca = TimexLCA(
            demand={self.node_a: 1},
            method=("GWP", "example"),
        )

        self.tlca.build_timeline()
        self.tlca.lci()

        self.characterization_function_dict = {
            bd.get_node(code="CH4").id: characterize_something,
        }

    def test_basic_dynamic_characterization_radiative_forcing(self):
        time_horizon = 97
        self.tlca.dynamic_lcia(
            metric="radiative_forcing",
            fixed_time_horizon=False,
            characterization_function_dict=self.characterization_function_dict,
            time_horizon=time_horizon,
        )
        assert len(self.tlca.characterized_inventory) == time_horizon - 1
