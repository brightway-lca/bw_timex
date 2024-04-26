from typing import Union, Tuple, Optional, Callable, List
import pandas as pd
import bw2data as bd
import numpy as np
import os
import warnings
import pickle

# from bw_temporalis.lcia.climate import characterize_methane, characterize_co2
from datetime import datetime, timedelta


class DynamicCharacterization:
    """
    This class calculates dynamic characterization of life cycle inventories.

    Currently, dynamic characterization functions is available for the following emissions:
    - CO2
    - CH4
    from bw_temporalis, which refers to these publications https://doi.org/10.5194/acp-13-2793-2013, http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118

    - CO
    - N2O
    from Levasseur et al 2010 (https://ciraig.org/index.php/project/dynco2-dynamic-carbon-footprinter/)


    First, the dynamic inventory is formatted as a DataFrame, then the characterization functions are applied in the method characterize_dynamic_inventory

    """

    def __init__(
        self,
        # method: str,
        # **kwargs,
        dynamic_inventory_df: pd.DataFrame,
        activity_dict: dict,
        biosphere_dict: dict,
        activity_time_mapping_dict_reversed: dict,
        biosphere_time_mapping_dict_reversed: dict,
        demand_timing_dict: dict,
        temporal_grouping: dict,
        method: tuple,
        characterization_functions: dict = None,
    ):
        """
        Initialize the DynamicCharacterization object. and add dynamic characterization functions for CO2, CH4, CO, N2O based on IPCC AR6 decay curves in acse users doesn't provide own dynamic characterization functions
        """
        self.dynamic_inventory_df = dynamic_inventory_df
        self.activity_dict = activity_dict
        self.biosphere_dict = biosphere_dict
        self.activity_time_mapping_dict_reversed = activity_time_mapping_dict_reversed
        self.biosphere_time_mapping_dict_reversed = biosphere_time_mapping_dict_reversed
        self.demand_timing_dict = demand_timing_dict
        self.temporal_grouping = temporal_grouping
        self.method = method

        if not characterization_functions:
            warnings.warn(
                f"No custom dynamic characterization functions provided. Using default dynamic characterization functions based on IPCC AR6 meant to work with biosphere3 flows. The flows that are characterized are based on the selection of the initially chosen impact category: {self.method}. You can look up the mapping in the dict timex_lca.dynamic_characterizer.characterization_functions."
            )
            self.add_default_characterization_functions()

        else:
            self.characterization_functions = (
                characterization_functions  # user-provided characterization functions
            )

    def characterize_dynamic_inventory(
        self,
        metric: (
            str | None
        ) = "radiative_forcing",  # available metrics are "radiative_forcing" and "GWP"
        fixed_TH: (
            bool | None
        ) = False,  # True: Levasseur approach TH for all emissions is calculated from FU, false: TH is calculated from t emission
        TH: int | None = 100,
        flow: set[int] | None = None,
        activity: set[int] | None = None,
        cumsum: bool | None = True,
    ) -> Tuple[pd.DataFrame, str, bool, int]:
        """
        Dynamic inventory, formatted as a Dataframe, is characterized using given dynamic characterization functions. If none are provided,
        default dynamic characterization functions for CO2, CH4, CO, N2O were created when initializing the DynamicCharacterization object.

        Available impact assessment types are Radiative forcing [W/m2] and GWP [kg CO2eq].
        The `characterization_function` is applied to each row of the input DataFrame of a timeline for a flexible `TH`, defaulting to 100 years.

        There is the option to use a fixed TH, which is calculated from the time of the functional unit (FU) instead of the time of emission.
        This means that earlier emissions are characterized for a longer time period than later emissions.

        param: cumsum: bool, default True: adds a new column to the characterized inventory that contains the cumulative forcing/GWP over time per flow
        param: type_of_method: str, default "radiative_forcing". Available options are "radiative_forcing" and "GWP".
        param: fixed_TH: bool, default False. If True, TH is calculated from the time of the functional unit (FU) instead of the time of emission.
        param: TH: int, default 100. Time horizon of assessment.
        param: flow: set[int], default None. Subset of flows to be characterized.
        param: activity: set[int], default None. Subset of activities to be characterized.

        return: Tuple[pd.DataFrame, str, bool, int] (characterized dynamic inventory, type of method, fixed TH, TH). The latter three are stored as attributes of TimexLCA to be called for plotting.

        """
        if metric not in {"radiative_forcing", "GWP"}:
            raise ValueError(
                f"impact assessment type must be either 'radiative_forcing' or 'GWP', not {metric}"
            )

        flow_mapping = {}  # not sure if we have to keep this
        for flow in self.characterization_functions.keys():
            try:
                flow_mapping[bd.get_activity(flow).id] = flow
            except:
                raise ValueError(
                    f"Flow '{flow}' could not be mapped to a biosphere flow in your database."
                )

        time_res_dict = {
            "year": "%Y",
            "month": "%Y%m",
            "day": "%Y%m%d",
            "hour": "%Y%m%d%M",
        }

        self.characterized_inventory = pd.DataFrame()
        for _, row in self.dynamic_inventory_df.iterrows():
            if row.flow not in self.characterization_functions.keys():
                continue

            if metric == "radiative_forcing":  # radiative forcing in W/m2

                if (
                    not fixed_TH
                ):  # fixed_TH = False: conventional approach, emission is calculated from t emission for the length of TH
                    self.characterized_inventory = pd.concat(
                        [
                            self.characterized_inventory,
                            self.characterization_functions[row.flow][0](
                                row,
                                period=TH,
                                negative_sign=self.characterization_functions[row.flow][
                                    1
                                ],
                            ),
                        ]
                    )

                else:  # fixed TH = True: Levasseur approach: TH for all emissions extended or shortened based on timing of FU + TH
                    # e.g. an emission occuring n years before FU is characterized for TH+n years
                    timing_FU = [value for value in self.demand_timing_dict.values()]
                    end_TH_FU_list = [x + TH for x in timing_FU]

                    if len(end_TH_FU_list) > 1:
                        warnings.warn(
                            f"There are multiple functional units with different timings. The first one ({str(end_TH_FU_list[0])}) will be used as a basis for the fixed time horizon in dynamic characterization."
                        )

                    end_TH_FU = datetime.strptime(
                        str(end_TH_FU_list[0]), time_res_dict[self.temporal_grouping]
                    )

                    timing_emission = (
                        row.date.to_pydatetime()
                    )  # convert'pandas._libs.tslibs.timestamps.Timestamp' to datetime object
                    new_TH = round(
                        (end_TH_FU - timing_emission).days / 365.25
                    )  # time difference in integer years between emission timing and end of TH of FU
                    self.characterized_inventory = pd.concat(
                        [
                            self.characterized_inventory,
                            self.characterization_functions[row.flow][0](
                                row,
                                period=new_TH,
                                negative_sign=self.characterization_functions[row.flow][
                                    1
                                ],
                            ),
                        ]
                    )

            if metric == "GWP":  # scale radiative forcing to GWP [kg CO2 equivalent]
                if (
                    not fixed_TH
                ):  # fixed_TH = False: conventional approach, emission is calculated from t emission for the length of TH
                    radiative_forcing_ghg = self.characterization_functions[row.flow][
                        0
                    ](
                        row,
                        period=TH,
                        negative_sign=self.characterization_functions[row.flow][1],
                    )

                    row["amount"] = 1  # convert 1 kg CO2 equ.
                    radiative_forcing_co2 = characterize_co2(row, period=TH)
                    warnings.warn(
                        "Using timex' default co2 characterization function for GWP reference."
                    )

                    ghg_integral = radiative_forcing_ghg["amount"].sum()
                    co2_integral = radiative_forcing_co2["amount"].sum()
                    co2_equiv = ghg_integral / co2_integral

                    row_data = {
                        "date": radiative_forcing_ghg.loc[
                            0, "date"
                        ],  # start date of emission
                        "amount": co2_equiv,  # ghg emission in kg co2 equiv
                        "flow": radiative_forcing_ghg.loc[0, "flow"],
                        "activity": radiative_forcing_ghg.loc[0, "activity"],
                    }
                    row_df = pd.DataFrame([row_data])
                    self.characterized_inventory = pd.concat(
                        [self.characterized_inventory, row_df], ignore_index=True
                    )

                else:  # fixed TH = True: Levasseur approach: TH for all emissions extended or shortened based on timing of FU + TH
                    # e.g. an emission occuring n years before FU is characterized for TH+n years
                    timing_FU = [
                        value for value in self.demand_timing_dict.values()
                    ]  # FIXME what if there are multiple FU occuring at different times?
                    end_TH_FU_list = [x + TH for x in timing_FU]

                    if len(end_TH_FU_list) > 1:
                        warnings.warn(
                            f"There are multiple functional units with different timings. The first one ({str(end_TH_FU_list[0])}) will be used as a basis for the fixed time horizon in dynamic characterization."
                        )

                    end_TH_FU = datetime.strptime(
                        str(end_TH_FU_list[0]), time_res_dict[self.temporal_grouping]
                    )

                    timing_emission = (
                        row.date.to_pydatetime()
                    )  # convert'pandas._libs.tslibs.timestamps.Timestamp' to datetime object
                    new_TH = round(
                        (end_TH_FU - timing_emission).days / 365.25
                    )  # time difference in integer years between emission timing and end of TH of FU

                    radiative_forcing_ghg = self.characterization_functions[row.flow][
                        0
                    ](
                        row,
                        period=new_TH,
                        negative_sign=self.characterization_functions[row.flow][1],
                    )  # indidvidual emissions are calculated for t_emission until t_FU + TH

                    row["amount"] = 1  # convert 1 kg CO2 equ.
                    radiative_forcing_co2 = characterize_co2(
                        row, period=TH
                    )  # reference substance CO2 is calculated for TH!

                    ghg_integral = radiative_forcing_ghg["amount"].sum()
                    co2_integral = radiative_forcing_co2["amount"].sum()
                    co2_equiv = ghg_integral / co2_integral

                    row_data = {
                        "date": radiative_forcing_ghg.loc[
                            0, "date"
                        ],  # start date of emission
                        "amount": co2_equiv,  # ghg emission in co2 equiv
                        "flow": radiative_forcing_ghg.loc[0, "flow"],
                        "activity": radiative_forcing_ghg.loc[0, "activity"],
                    }
                    row_df = pd.DataFrame([row_data])
                    self.characterized_inventory = pd.concat(
                        [self.characterized_inventory, row_df], ignore_index=True
                    )

            # sort by date
            if "date" in self.characterized_inventory:
                self.characterized_inventory.sort_values(
                    by="date", ascending=True, inplace=True
                )
                self.characterized_inventory.reset_index(drop=True, inplace=True)

            if cumsum and "amount" in self.characterized_inventory:
                self.characterized_inventory["amount_sum"] = (
                    self.characterized_inventory["amount"].cumsum()
                )  # TODO: there is also an option for cumulative results in the characterization functions themselves. Rethink where this is handled best and to avoid double cumsum

            # all_characterized_inventory = pd.concat([all_characterized_inventory, characterized_inventory])

        # remove rows with zero amounts to make it more readable
        self.characterized_inventory = self.characterized_inventory[self.characterized_inventory["amount"] != 0]
        
        # add meta data and reorder
        self.characterized_inventory["activity_name"] = self.characterized_inventory[
            "activity"
        ].map(lambda x: self.activity_time_mapping_dict_reversed.get(x)[0])
        self.characterized_inventory["flow_name"] = self.characterized_inventory[
            "flow"
        ].apply(lambda x: bd.get_node(id=x)["name"])
        self.characterized_inventory = self.characterized_inventory[
            [
                "date",
                "amount",
                "flow",
                "flow_name",
                "activity",
                "activity_name",
                "amount_sum",
            ]
        ]
        self.characterized_inventory.reset_index(drop=True, inplace=True)
        self.characterized_inventory.sort_values(
            by="date", ascending=True, inplace=True
        )

        return self.characterized_inventory, metric, fixed_TH, TH

    def add_default_characterization_functions(self):
        """
        Add default dynamic characterization functions for CO2, CH4, N2O and other GHG, based on IPCC AR6 decay curves. 
        
        Please note: Currently, only CO2, CH4 and N2O include climate-carbon feedbacks. This has not yet been added for other GHGs. Refer to https://esd.copernicus.org/articles/8/235/2017/esd-8-235-2017.html
        """
        self.characterization_functions = dict()
        
        bioflows_in_lcia_method = bd.Method(self.method).load()
        
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'data', 'decay_multipliers.pkl'
        )
        with open(filepath, 'rb') as f:
            decay_multipliers = pickle.load(f)

        for flow in bioflows_in_lcia_method:
            node = bd.get_node(database=flow[0][0], code=flow[0][1])

            if (
                "carbon dioxide" in node["name"].lower()
            ):
                if "soil" in node.get("categories", []):
                    self.characterization_functions[node.id] = (characterize_co2, True) # negative emission because uptake by soil
                else:
                    self.characterization_functions[node.id] = (characterize_co2, False)

            elif (
                "methane, fossil" in node["name"].lower()
                or "methane, from soil or biomass stock" in node["name"].lower()
            ):
                # TODO Check why "methane, non-fossil" has a CF of 27 instead of 29.8, currently excluded
                self.characterization_functions[node.id] = (characterize_ch4, False)
                
            elif "dinitrogen monoxide" in node["name"].lower():
                self.characterization_functions[node.id] = (characterize_n2o, False)  
            
            elif "carbon monoxide" in node["name"].lower():
                self.characterization_functions[node.id] = (characterize_co, False)
                            
            else:  
                cas_number = node.get("CAS number")
                if cas_number:
                    decay_series = decay_multipliers.get(cas_number)
                    if decay_series is not None:
                        self.characterization_functions[node.id] = (create_generic_characterization_function(self, decay_series), False)


def IRF_co2(year) -> callable:
    """
    Impulse Resonse Function of CO2
    """
    alpha_0, alpha_1, alpha_2, alpha_3 = 0.2173, 0.2240, 0.2824, 0.2763
    tau_1, tau_2, tau_3 = 394.4, 36.54, 4.304
    exponentials = lambda year, alpha, tau: alpha * tau * (1 - np.exp(-year / tau))
    return (
        alpha_0 * year
        + exponentials(year, alpha_1, tau_1)
        + exponentials(year, alpha_2, tau_2)
        + exponentials(year, alpha_3, tau_3)
    )


def characterize_co2(
    series,
    period: int | None = 100,
    cumulative: bool | None = False,
    negative_sign: (
        bool | None
    ) = False,  # flip the sign of the characterization function if it's an uptake and not release of CO2
) -> pd.DataFrame:
    """
    Based on characterize_co2 from bw_temporalis, but updated numerical values from IPCC AR6 Ch7 & SM.

    Calculate the cumulative or marginal radiative forcing (CRF) from CO2 for each year in a given period.

    If `cumulative` is True, the cumulative CRF is calculated. If `cumulative` is False, the marginal CRF is calculated.
    Takes a single row of the TimeSeries Pandas DataFrame (corresponding to a set of (`date`/`amount`/`flow`/`activity`).
    For each year in the given period, the CRF is calculated.
    Units are watts/square meter/kilogram of CO2.

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    Notes
    -----
    See also the relevant scientific publication on CRF: https://doi.org/10.5194/acp-13-2793-2013
    See also the relevant scientific publication on the numerical calculation of CRF: http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118
    
    Numerical values from IPCC AR6 Chapter 7

    See Also
    --------
    characterize_methane: The same function for CH4
    """

    # functional variables and units (from publications listed in docstring)
    radiative_efficiency_ppb = (
        1.33e-5  # W/m2/ppb; 2019 background co2 concentration; IPCC AR6 Table 7.15
    )

    # for conversion from ppb to kg-CO2
    M_co2 = 44.01  # g/mol
    M_air = 28.97  # g/mol, dry air
    m_atmosphere = 5.135e18  # kg [Trenberth and Smith, 2005]

    radiative_efficiency_kg = (
        radiative_efficiency_ppb * M_air / M_co2 * 1e9 / m_atmosphere
    )  # W/m2/kg-CO2

    date_beginning: np.datetime64 = series["date"].to_numpy()
    date_characterized: np.ndarray = date_beginning + np.arange(
        start=0, stop=period, dtype="timedelta64[Y]"
    ).astype("timedelta64[s]")

    decay_multipliers: np.ndarray = np.array(
        [radiative_efficiency_kg * IRF_co2(year) for year in range(period)]
    )

    forcing = pd.Series(data=series.amount * decay_multipliers, dtype="float64")

    if (
        negative_sign
    ):  # flip the sign of the characterization function if it's an uptake and not release of CO2
        forcing = -forcing

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


def characterize_co(
    series,
    period: int | None = 100,
    cumulative: bool | None = False,
    negative_sign: (
        bool | None
    ) = False,  # flip the sign of the characterization function if it's an uptake and not release of CO2
) -> pd.DataFrame:
    """
    This is exactly the same function as for CO2, it's just scaled by the ratio of molar masses of CO and CO2. This is because CO is very short-lived (lifetime ~2 months) and we assume that it completely reacts to CO2 within the first year.
    
    Based on characterize_co2 from bw_temporalis, but updated numerical values from IPCC AR6 Ch7 & SM.

    Calculate the cumulative or marginal radiative forcing (CRF) from CO2 for each year in a given period.

    If `cumulative` is True, the cumulative CRF is calculated. If `cumulative` is False, the marginal CRF is calculated.
    Takes a single row of the TimeSeries Pandas DataFrame (corresponding to a set of (`date`/`amount`/`flow`/`activity`).
    For each year in the given period, the CRF is calculated.
    Units are watts/square meter/kilogram of CO2.

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    Notes
    -----
    See also the relevant scientific publication on CRF: https://doi.org/10.5194/acp-13-2793-2013
    See also the relevant scientific publication on the numerical calculation of CRF: http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118
    
    Numerical values from IPCC AR6 Chapter 7

    See Also
    --------
    characterize_methane: The same function for CH4
    """

    # functional variables and units (from publications listed in docstring)
    radiative_efficiency_ppb = (
        1.33e-5  # W/m2/ppb; 2019 background co2 concentration; IPCC AR6 Table 7.15
    )

    # for conversion from ppb to kg-CO2
    M_co2 = 44.01  # g/mol
    M_co = 28.01
    M_air = 28.97  # g/mol, dry air
    m_atmosphere = 5.135e18  # kg [Trenberth and Smith, 2005]

    radiative_efficiency_kg = (
        radiative_efficiency_ppb * M_air / M_co2 * 1e9 / m_atmosphere
    )  # W/m2/kg-CO2

    date_beginning: np.datetime64 = series["date"].to_numpy()
    date_characterized: np.ndarray = date_beginning + np.arange(
        start=0, stop=period, dtype="timedelta64[Y]"
    ).astype("timedelta64[s]")

    decay_multipliers: np.ndarray = np.array(
        [M_co2 / M_co * radiative_efficiency_kg * IRF_co2(year) for year in range(period)] # <-- Scaling from co2 to co is done here
    )

    forcing = pd.Series(data=series.amount * decay_multipliers, dtype="float64")

    if (
        negative_sign
    ):  # flip the sign of the characterization function if it's an uptake and not release of CO2
        forcing = -forcing

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


def characterize_ch4(
    series,
    period: int = 100,
    cumulative=False,
    negative_sign: (
        bool | None
    ) = False,  # simply passed to avoid error message, never used since all CH4 flows are emissions
) -> pd.DataFrame:
    """
    Based on characterize_methane from bw_temporalis, but updated numerical values from IPCC AR6 Ch7 & SM.

    Calculate the cumulative or marginal radiative forcing (CRF) from CH4 for each year in a given period. This DOES include include indirect effects of CH4 on ozone and water vapor, but DOES NOT include the decay to CO2. For more info on that, see the deprecated version of temporalis.

    If `cumulative` is True, the cumulative CRF is calculated. If `cumulative` is False, the marginal CRF is calculated.
    Takes a single row of the TimeSeries Pandas DataFrame (corresponding to a set of (`date`/`amount`/`flow`/`activity`).
    For earch year in the given period, the CRF is calculated.
    Units are watts/square meter/kilogram of CH4.

    Parameters
    ----------
    series : array-like
        A single row of the TimeSeries dataframe.
    period : int, optional
        Time period for calculation (number of years), by default 100
    cumulative : bool, optional
        Should the RF amounts be summed over time?

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    Notes
    -----
    See also the relevant scientific publication on CRF: https://doi.org/10.5194/acp-13-2793-2013
    See also the relevant scientific publication on the numerical calculation of CRF: http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118

    See Also
    --------
    characterize_co2: The same function for CO2
    """

    # functional variables and units (from publications listed in docstring)
    radiative_efficiency_ppb = 5.7e-4  # # W/m2/ppb; 2019 background cch4 concentration; IPCC AR6 Table 7.15. This number includes indirect effects.

    # for conversion from ppb to kg-CH4
    M_ch4 = 16.04  # g/mol
    M_air = 28.97  # g/mol, dry air
    m_atmosphere = 5.135e18  # kg [Trenberth and Smith, 2005]

    radiative_efficiency_kg = (
        radiative_efficiency_ppb * M_air / M_ch4 * 1e9 / m_atmosphere
    )  # W/m2/kg-CH4;

    tau = 11.8  # Lifetime (years)

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


def characterize_n2o(
    series,
    period: int = 100,
    cumulative=False,
    negative_sign: (
        bool | None
    ) = False,  # simply passed to avoid error message, never used since all n2o flows are emissions
) -> pd.DataFrame:
    """
    Based on characterize_methane from bw_temporalis, but updated numerical values from IPCC AR6 Ch7 & SM.

    Calculate the cumulative or marginal radiative forcing (CRF) from N2O for each year in a given period.

    If `cumulative` is True, the cumulative CRF is calculated. If `cumulative` is False, the marginal CRF is calculated.
    Takes a single row of the TimeSeries Pandas DataFrame (corresponding to a set of (`date`/`amount`/`flow`/`activity`).
    For earch year in the given period, the CRF is calculated.
    Units are watts/square meter/kilogram of N2O.

    Parameters
    ----------
    series : array-like
        A single row of the TimeSeries dataframe.
    period : int, optional
        Time period for calculation (number of years), by default 100
    cumulative : bool, optional
        Should the RF amounts be summed over time?

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    Notes
    -----
    See also the relevant scientific publication on CRF: https://doi.org/10.5194/acp-13-2793-2013
    See also the relevant scientific publication on the numerical calculation of CRF: http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118

    See Also
    --------
    characterize_co2: The same function for CO2
    """

    # functional variables and units (from publications listed in docstring)
    radiative_efficiency_ppb = 2.8e-3  # # W/m2/ppb; 2019 background cch4 concentration; IPCC AR6 Table 7.15. This number includes indirect effects.

    # for conversion from ppb to kg-CH4
    M_n2o = 44.01  # g/mol
    M_air = 28.97  # g/mol, dry air
    m_atmosphere = 5.135e18  # kg [Trenberth and Smith, 2005]

    radiative_efficiency_kg = (
        radiative_efficiency_ppb * M_air / M_n2o * 1e9 / m_atmosphere
    )  # W/m2/kg-CH4;

    tau = 109  # Lifetime (years)

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


def create_generic_characterization_function(self, decay_series) -> pd.DataFrame:
    """
    Creates a characterization function for a generic GHG based on a decay series.
    """

    def characterize_generic(
        series,
        period: int = 100,
        cumulative=False,
        negative_sign: (
            bool | None
        ) = False,  # flip the sign of the characterization function if it's an uptake and not release of CO2)
    ) -> pd.DataFrame:
        """
        Uses lookup generated in /dev/calculate_metrics.ipynb
        Data originates from https://doi.org/10.1029/2019RG000691

        Parameters
        ----------
        series : array-like
            A single row of the dynamic inventory dataframe.
        period : int, optional
            Time period for calculation (number of years), by default 100
        cumulative : bool,
            cumulative impact

        Returns
        -------
        A TimeSeries dataframe with the following columns:
        - date: datetime64[s]
        - amount: float
        - flow: str
        - activity: str

        """

        date_beginning: np.datetime64 = series["date"].to_numpy()
        dates_characterized: np.ndarray = date_beginning + np.arange(
            start=0, stop=period, dtype="timedelta64[Y]"
        ).astype("timedelta64[s]")

        decay_multipliers = decay_series[:period]

        forcing = pd.Series(data=series.amount * decay_multipliers, dtype="float64")
        if not cumulative:
            forcing = forcing.diff(periods=1).fillna(0)

        return pd.DataFrame(
            {
                "date": pd.Series(data=dates_characterized, dtype="datetime64[s]"),
                "amount": forcing,
                "flow": series.flow,
                "activity": series.activity,
            }
        )

    return characterize_generic

def import_levasseur_dcfs():
    """
    Extracts the yearly radiative forcing values of various GHG based on Levasseur 2010 for 0 - 2000 years (https://ciraig.org/index.php/project/dynco2-dynamic-carbon-footprinter/)
    Unit of forcing is W/m2/kg of GHG emitted.

    Because of the deviation between the functions in bw_temporalis and Levasseur for CO2, we use the bw_temporalis for CO2 and the Levasseur values for the other GHGs.

    param: None
    return: dict of dicts with the following structure: {ghg: {year: forcing}}
    """
    # Levasseur

    # read in excel data
    subfolder_name = "data"
    file = "Dynamic_LCAcalculatorv.2.0.xlsm"
    file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), subfolder_name, file
    )
    sheet_name = "FC"
    data = pd.read_excel(file_path, sheet_name=sheet_name)

    # clean data
    data = data.iloc[2:]  # Remove the first three empty rows
    data.columns = data.iloc[0]  # Set the first row as the header
    data = data.iloc[3:, :]  # remove row 0-2 (contain units)
    data = data.rename(
        columns={data.columns[0]: "year"}
    )  # replace the first column label (NaN) with 'Year'
    data = data.sort_values("year")  # sort by year

    # rename of elemental flows
    biopshere_flow_mapping = {
        "CO2": "carbon dioxide",
        "CH4": "methane",
        "N2O": "nitrous oxide",
        "CO": "carbon monoxide",
    }  # TODO think about how to map this to ecoinvent biosphere
    data = data.rename(columns=biopshere_flow_mapping)

    # Convert DataFrame to dictionary
    levasseur_radiative_forcing = {}
    for column in data.columns[1:]:  # skipping first column (year)
        levasseur_radiative_forcing[column] = dict(
            zip(data.iloc[:, 0], data[column])
        )  # {year: forcing}
    return levasseur_radiative_forcing


def characterize_co_levasseur(
    self, series, period: int = 100, cumulative=False
) -> pd.DataFrame:
    """
    Radiative forcing function of CO from Levasseur et al 2010 (self.levasseur_dcfs["carbon monoxide"])
    Units are watts/square meter/kilogram of ghg emissions

    Parameters
    ----------
    series : array-like
        A single row of the dynamic inventory dataframe.
    period : int, optional
        Time period for calculation (number of years), by default 100
    cumulative : bool,
        cumulative impact

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    """
    levasseur_dcfs = (
        import_levasseur_dcfs()
    )  # This is super inefficient, but as Im not sure we'll keep this i didnt bother to change it

    co_decay_array = levasseur_dcfs["carbon monoxide"]

    date_beginning: np.datetime64 = series["date"].to_numpy()
    dates_characterized: np.ndarray = date_beginning + np.arange(
        start=0, stop=period, dtype="timedelta64[Y]"
    ).astype("timedelta64[s]")

    decay_multiplier_y0 = [0]  # no forcing in y0
    decay_multipliers_y1to100 = [
        co_decay_array[key] for key in range(1, period) if key in co_decay_array
    ]
    decay_multipliers = decay_multiplier_y0 + decay_multipliers_y1to100
    decay_multipliers = np.cumsum(decay_multipliers)

    forcing = pd.Series(data=series.amount * decay_multipliers, dtype="float64")
    if not cumulative:
        forcing = forcing.diff(periods=1).fillna(0)

    return pd.DataFrame(
        {
            "date": pd.Series(data=dates_characterized, dtype="datetime64[s]"),
            "amount": forcing,
            "flow": series.flow,
            "activity": series.activity,
        }
    )


def characterize_n2o_levasseur(
    self, series, period: int = 100, cumulative=False
) -> pd.DataFrame:
    """

    Radiative forcing functions of N2O are taken from Levasseur et al 2010 (self.levasseur_dcfs["carbon monoxide"])
    Units are watts/square meter/kilogram of ghg emissions

    Parameters
    ----------
    n20_decay_array:
    series : array-like
        A single row of the dynamic inventory dataframe.
    period : int, optional
        Time period for calculation (number of years), by default 100
    cumulative : bool, optional
        cumulative impact

    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str

    """
    levasseur_dcfs = (
        import_levasseur_dcfs()
    )  # This is super inefficient, but as Im not sure we'll keep this i didnt bother to change it

    n2o_decay_array = self.levasseur_dcfs["nitrous oxide"]
    date_beginning: np.datetime64 = series["date"].to_numpy()
    date_characterized: np.ndarray = date_beginning + np.arange(
        start=0, stop=period, dtype="timedelta64[Y]"
    ).astype("timedelta64[s]")

    decay_multiplier_y0 = [0]  # no forcing in y0
    decay_multipliers_y1to100 = [
        n2o_decay_array[key] for key in range(1, period) if key in n2o_decay_array
    ]
    decay_multipliers = decay_multiplier_y0 + decay_multipliers_y1to100
    decay_multipliers = np.cumsum(decay_multipliers)

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
