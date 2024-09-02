import warnings
from datetime import datetime
from typing import Callable, List, Optional, Union

import bw2data as bd
import matplotlib.pyplot as plt
import pandas as pd
from bw2data.backends.schema import ExchangeDataset
from bw2data.backends.proxies import Exchange
from bw2data.errors import MultipleResults, UnknownObject


def extract_date_as_integer(dt_obj: datetime, time_res: Optional[str] = "year") -> int:
    """
    Converts a datetime object to an integer for a given temporal resolution `time_res`

    Parameters
    ----------

    dt_obj : Datetime object.
        Datetime object to be converted to an integer.

    time_res : str, optional
        time resolution to be returned: year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH

    Returns
    -------
    date_as_integer : int
        Datetime objectconverted to an integer in the format of time_res

    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
    }

    if time_res not in time_res_dict.keys():
        warnings.warn(
            'time_res: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                time_res, time_res_dict.keys()
            ),
            category=Warning,
        )
    formatted_date = dt_obj.strftime(time_res_dict[time_res])
    date_as_integer = int(formatted_date)

    return date_as_integer


def extract_date_as_string(temporal_grouping: str, timestamp: datetime) -> str:
    """
    Extracts the grouping date as a string from a datetime object, based on the chosen temporal grouping.
    e.g. for `temporal_grouping` = 'month', and `timestamp` = 2023-03-29T01:00:00, it extracts the string '202303'.


    Parameters
    ----------
    temporal_grouping : str
        Temporal grouping for the date string. Options are: 'year', 'month', 'day', 'hour'
    timestamp : datetime
        Datetime object to be converted to a string.

    Returns
    -------
    date_as_string
        Date as a string in the format of the chosen temporal grouping.
    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
    }

    if temporal_grouping not in time_res_dict.keys():
        warnings.warn(
            'temporal_grouping: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                temporal_grouping, time_res_dict.keys()
            ),
            category=Warning,
        )
    return timestamp.strftime(time_res_dict[temporal_grouping])


def convert_date_string_to_datetime(temporal_grouping, datestring) -> datetime:
    """
    Converts the string of a date to datetime object.
    e.g. for `temporal_grouping` = 'month', and `datestring` = '202303', it extracts 2023-03-01

    Parameters
    ----------
    temporal_grouping : str
        Temporal grouping for the date string. Options are: 'year', 'month', 'day', 'hour'
    datestring : str
        Date as a string

    Returns
    -------
    datetime
        Datetime object of the date string at the chosen temporal resolution.
    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
    }

    if temporal_grouping not in time_res_dict.keys():
        warnings.warn(
            'temporal grouping: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                temporal_grouping, time_res_dict.keys()
            ),
            category=Warning,
        )
    return datetime.strptime(datestring, time_res_dict[temporal_grouping])


def add_flows_to_characterization_function_dict(
    flows: Union[str, List[str]],
    func: Callable,
    characterization_function_dict: Optional[dict] = dict(),
) -> dict:
    """
    Add a new flow or a list of flows to the available characterization functions.

    Parameters
    ----------
    flows : Union[str, List[str]]
        Flow or list of flows to be added to the characterization function dictionary.
    func : Callable
        Dynamic characterization function for flow.
    characterization_function_dict : dict, optional
        Dictionary of flows and their corresponding characterization functions. Default is an empty dictionary.

    Returns
    -------
    dict
        Updated characterization function dictionary with the new flow(s) and function(s).
    """

    # Check if the input is a single flow (str) or a list of flows (List[str])
    if isinstance(flows, str):
        # It's a single flow, add it directly
        characterization_function_dict[flows] = func
    elif isinstance(flows, list):
        # It's a list of flows, iterate and add each one
        for flow in flows:
            characterization_function_dict[flow] = func

    return characterization_function_dict


def plot_characterized_inventory_as_waterfall(
    lca_obj,
    static_scores=None,
    prospective_scores=None,
    order_stacked_activities=None,
):
    """
    Plot a stacked waterfall chart of characterized inventory data. As comparison, static and prospective scores can be added.
    Only works for metric GWP at the moment.

    Parameters
    ----------
    lca_obj : TimexLCA
        LCA object with characterized inventory data.
    static_scores : dict, optional
        Dictionary of static scores. Default is None.
    prospective_scores : dict, optional
        Dictionary of prospective scores. Default is None.
    order_stacked_activities : list, optional
        List of activities to order the stacked bars in the waterfall plot. Default is None.

    Returns
    -------
    None but plots the waterfall chart.

    """
    if not hasattr(lca_obj, "characterized_inventory"):
        raise ValueError("LCA object does not have characterized inventory data.")

    if not hasattr(lca_obj, "activity_time_mapping_dict_reversed"):
        raise ValueError("Make sure to pass an instance of a TimexLCA.")

    # Grouping and summing data
    plot_data = lca_obj.characterized_inventory.groupby(
        ["date", "activity"], as_index=False
    ).sum()
    plot_data["year"] = plot_data[
        "date"
    ].dt.year  # TODO make temporal resolution flexible

    # Optimized activity label fetching
    unique_activities = plot_data["activity"].unique()
    activity_labels = {
        idx: bd.get_activity(lca_obj.activity_time_mapping_dict_reversed[idx][0])[
            "name"
        ]
        for idx in unique_activities
    }
    plot_data["activity_label"] = plot_data["activity"].map(activity_labels)
    # Pivoting data for plotting
    pivoted_data = plot_data.pivot(
        index="year", columns="activity_label", values="amount"
    )

    combined_data = []
    # Adding exchange_scores as a static column
    if static_scores:
        static_data = pd.DataFrame(
            static_scores.items(), columns=["activity_label", "amount"]
        )
        static_data["year"] = "static"
        pivoted_static_data = static_data.pivot(
            index="year", columns="activity_label", values="amount"
        )
        combined_data.append(pivoted_static_data)

    combined_data.append(pivoted_data)  # making sure the order is correct

    # Adding exchange_scores as a prospective column
    if prospective_scores:
        prospective_data = pd.DataFrame(
            prospective_scores.items(), columns=["activity_label", "amount"]
        )
        prospective_data["year"] = "prospective"
        pivoted_prospective_data = prospective_data.pivot(
            index="year", columns="activity_label", values="amount"
        )
        combined_data.append(pivoted_prospective_data)

    combined_df = pd.concat(combined_data, axis=0)

    if order_stacked_activities:
        combined_df = combined_df[
            order_stacked_activities
        ]  # change order of activities in the stacked bars of the waterfall

    # Calculate the bottom for only the dynamic data
    dynamic_bottom = pivoted_data.sum(axis=1).cumsum().shift(1).fillna(0)

    if static_scores and prospective_scores:
        bottom = pd.concat([pd.Series([0]), dynamic_bottom, pd.Series([0])])
    elif static_scores:
        bottom = pd.concat([pd.Series([0]), dynamic_bottom])
    elif prospective_scores:
        bottom = pd.concat([dynamic_bottom, pd.Series([0])])
    else:
        bottom = dynamic_bottom

    # Plotting
    ax = combined_df.plot(
        kind="bar",
        stacked=True,
        bottom=bottom,
        figsize=(14, 6),
        edgecolor="black",
        linewidth=0.5,
    )
    ax.set_ylabel("GWP [kg CO2-eq]")
    ax.set_xlabel("")
    plt.xticks(rotation=45, ha="right")

    if static_scores:
        ax.axvline(x=0.5, color="black", linestyle="--", lw=1)
    if prospective_scores:
        ax.axvline(x=len(combined_df) - 1.5, color="black", linestyle="--", lw=1)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1])  # Reversing the order for the legend
    ax.set_axisbelow(True)
    plt.grid(True)
    plt.show()

def get_exchange(**kwargs) -> Exchange:
    """
    Get an exchange from the database.
    """
    mapping = {
        "input_code": ExchangeDataset.input_code,
        "input_database": ExchangeDataset.input_database,
        "output_code": ExchangeDataset.output_code,
        "output_database": ExchangeDataset.output_database,
    }
    qs = ExchangeDataset.select()
    for key, value in kwargs.items():
        try:
            qs = qs.where(mapping[key] == value)
        except KeyError:
            continue
    
    candidates = [Exchange(obj) for obj in qs]
    if len(candidates) > 1:
        raise MultipleResults(
            "Found {} results for the given search. Please be more specific or double-check your system model for duplicates.".format(len(candidates))
        )
    elif not candidates:
        raise UnknownObject
    return candidates[0]