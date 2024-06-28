"""
Testing error messages and warnings that can occur during dynamic characterization, depending on the chosen time horizon and the timing of emissions.
"""

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


def test_emission_out_of_fixed_th_only(delayed_pulse_emission_db):
    node_a = bd.get_activity(("test", "A"))
    tlca = TimexLCA(
        demand={node_a: 1},
        method=("GWP", "example"),
    )
    tlca.build_timeline()
    tlca.lci()
    characterization_function_dict = {
        bd.get_node(code="CH4").id: characterize_something,
    }
    time_horizon = 9
    with pytest.raises(
        ValueError,
        match="There are no flows to characterize. Please make sure your time horizon matches the timing of emissions and make sure there are characterization functions for the flows in the dynamic inventories.",
    ):
        tlca.dynamic_lcia(
            metric="radiative_forcing",
            fixed_time_horizon=True,
            characterization_function_dict=characterization_function_dict,
            time_horizon=time_horizon,
        )


def test_emission_out_of_fixed_th(early_and_delayed_pulse_emission_db):
    node_a = bd.get_activity(("test", "A"))
    tlca = TimexLCA(
        demand={node_a: 1},
        method=("GWP", "example"),
    )
    tlca.build_timeline()
    tlca.lci()
    characterization_function_dict = {
        bd.get_node(code="CH4").id: characterize_something,
    }
    time_horizon = 9
    with pytest.warns(
        UserWarning,
        match="An emission occurs outside of the specified time horizon and will not be characterized. Please make sure this is intended.",
    ):
        tlca.dynamic_lcia(
            metric="radiative_forcing",
            fixed_time_horizon=True,
            characterization_function_dict=characterization_function_dict,
            time_horizon=time_horizon,
        )
