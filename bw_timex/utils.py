import warnings
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Union

import matplotlib.pyplot as plt
import pandas as pd
import bw2data as bd
from bw2data.backends import ActivityDataset as AD
from bw2data.backends.proxies import Exchange
from bw2data.backends.schema import ExchangeDataset
from bw2data.errors import MultipleResults, UnknownObject
from bw_temporalis import TemporalDistribution

time_res_to_int_dict = {
    "year": "%Y",
    "month": "%Y%m",
    "day": "%Y%m%d",
    "hour": "%Y%m%d%H",
}


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
        Datetime object converted to an integer in the format of time_res

    """
    if time_res not in time_res_to_int_dict.keys():
        warnings.warn(
            f'time_res: {time_res} is not a valid option. Please choose from: {list(time_res_to_int_dict.keys())} defaulting to "year"',
            category=Warning,
        )
    formatted_date = dt_obj.strftime(time_res_to_int_dict[time_res])
    date_as_integer = int(formatted_date)

    return date_as_integer


def extract_date_as_string(timestamp: datetime, temporal_grouping: str) -> str:
    """
    Extracts the grouping date as a string from a datetime object, based on the chosen temporal grouping.
    e.g. for `temporal_grouping` = 'month', and `timestamp` = 2023-03-29T01:00:00, it extracts the string '202303'.


    Parameters
    ----------
    timestamp : Datetime object
        Datetime object to be converted to a string.
    temporal_grouping : str
        Temporal grouping for the date string. Options are: 'year', 'month', 'day', 'hour'


    Returns
    -------
    date_as_string: str
        Date as a string in the format of the chosen temporal grouping.
    """

    if temporal_grouping not in time_res_to_int_dict.keys():
        warnings.warn(
            f'temporal_grouping: {temporal_grouping} is not a valid option. Please choose from: {list(time_res_to_int_dict.keys())} defaulting to "year"',
            category=Warning,
        )
    return timestamp.strftime(time_res_to_int_dict[temporal_grouping])


def convert_date_string_to_datetime(temporal_grouping, date_string) -> datetime:
    """
    Converts the string of a date to datetime object.
    e.g. for `temporal_grouping` = 'month', and `date_string` = '202303', it extracts 2023-03-01

    Parameters
    ----------
    temporal_grouping : str
        Temporal grouping for the date string. Options are: 'year', 'month', 'day', 'hour'
    date_string : str
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
            f'temporal grouping: {temporal_grouping} is not a valid option. Please choose from: {list(time_res_dict.keys())} defaulting to "year"',
            category=Warning,
        )
    return datetime.strptime(date_string, time_res_dict[temporal_grouping])


def round_datetime(date: datetime, resolution: str) -> datetime:
    """
    Round a datetime object based on a given resolution

    Parameters
    ----------
    date : datetime
        datetime object to be rounded
    resolution: str
        Temporal resolution to round the datetime object to. Options are: 'year', 'month', 'day' and
        'hour'.

    Returns
    -------
    datetime
        rounded datetime object
    """
    if resolution == "year":
        mid_year = pd.Timestamp(f"{date.year}-07-01")
        return (
            pd.Timestamp(f"{date.year+1}-01-01")
            if date >= mid_year
            else pd.Timestamp(f"{date.year}-01-01")
        )

    if resolution == "month":
        start_of_month = pd.Timestamp(f"{date.year}-{date.month}-01")
        next_month = start_of_month + pd.DateOffset(months=1)
        mid_month = start_of_month + (next_month - start_of_month) / 2
        return next_month if date >= mid_month else start_of_month

    if resolution == "day":
        start_of_day = datetime(date.year, date.month, date.day)
        mid_day = start_of_day + timedelta(hours=12)
        return start_of_day + timedelta(days=1) if date >= mid_day else start_of_day

    if resolution == "hour":
        start_of_hour = datetime(date.year, date.month, date.day, date.hour)
        mid_hour = start_of_hour + timedelta(minutes=30)
        return start_of_hour + timedelta(hours=1) if date >= mid_hour else start_of_hour

    raise ValueError("Resolution must be one of 'year', 'month', 'day', or 'hour'.")


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
        Dictionary of flows and their corresponding characterization functions. Default is an empty
        dictionary.

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


def resolve_temporalized_node_name(code: str) -> str:
    """
    Getting the name of a node based on the code only.
    Works for non-unique codes if the name is the same across all databases.

    Parameters
    ----------
    code: str
        Code of the node to resolve.

    Returns
    -------
    str
        Name of the node.
    """
    qs = AD.select().where(AD.code == code)
    names = set([obj.name for obj in qs])
    if len(qs) > 1:
        if len(names) > 1:
            raise ValueError(
                "Found multiple names for the given code: {}".format(names)
            )
    elif not qs:
        raise UnknownObject
    return names.pop()


def find_matching_node(original_node, other_background_db):
    """
    Find a node in another background database that matches the original node based on name,
    reference product, and location.

    Parameters
    ----------
    original_node : dict
        Original node to find a match for.
    other_background_db : str
        Name of the other background database to search in.

    Returns
    -------
    dict
        Matching node in the other background database.
    """
    other_node = bd.get_node(
        **{
            "database": other_background_db,
            "name": original_node["name"],
            "product": original_node["reference product"],
            "location": original_node["location"],
        }
    )
    return other_node


def plot_characterized_inventory_as_waterfall(
    lca_obj,
    static_scores=None,
    prospective_scores=None,
    order_stacked_activities=None,
):
    """
    Plot a stacked waterfall chart of characterized inventory data. As comparison,
    static and prospective scores can be added. Only works for metric GWP at the moment.

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
    None
        plots the waterfall chart.

    """
    if not hasattr(lca_obj, "characterized_inventory"):
        raise ValueError("LCA object does not have characterized inventory data.")

    if not hasattr(lca_obj, "activity_time_mapping_dict_reversed"):
        raise ValueError("Make sure to pass an instance of a TimexLCA.")

    time_res_dict = {
        "year": "%Y",
        "month": "%Y-%m",
        "day": "%Y-%m-%d",
        "hour": "%Y-%m-%d %H",
    }

    plot_data = lca_obj.characterized_inventory.copy()
    
    plot_data["year"] = plot_data["date"].dt.strftime(
        time_res_dict[lca_obj.temporal_grouping]
    )  # TODO make temporal resolution flexible

    # Optimized activity label fetching
    unique_activities = plot_data["activity"].unique()
    activity_labels = {
        idx: resolve_temporalized_node_name(
            lca_obj.activity_time_mapping_dict_reversed[idx][0][1]
        )
        for idx in unique_activities
    }
    plot_data["activity_label"] = plot_data["activity"].map(activity_labels)

    plot_data = plot_data.groupby(["year", "activity_label"], as_index=False)["amount"].sum()
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
    ax.legend(
        handles[::-1], labels[::-1], loc="upper center", fontsize="small"
    )  # Reversing the order for the legend
    ax.set_axisbelow(True)
    plt.grid(True)
    plt.show()


def get_exchange(**kwargs) -> Exchange:
    """
    Get an exchange from the database.

    Parameters
    ----------
    **kwargs :
        Arguments to specify an exchange.
            - input_node: Input node object
            - input_code: Input node code
            - input_database: Input node database
            - output_node: Output node object
            - output_code: Output node code
            - output_database: Output node database

    Returns
    -------
    Exchange
        The exchange object matching the criteria.

    Raises
    ------
    MultipleResults
        If multiple exchanges match the criteria.
    UnknownObject
        If no exchange matches the criteria.
    """

    # Process input_node if present
    input_node = kwargs.pop("input_node", None)
    if input_node:
        kwargs["input_code"] = input_node["code"]
        kwargs["input_database"] = input_node["database"]

    # Process output_node if present
    output_node = kwargs.pop("output_node", None)
    if output_node:
        kwargs["output_code"] = output_node["code"]
        kwargs["output_database"] = output_node["database"]

    # Map kwargs to database fields
    mapping = {
        "input_code": ExchangeDataset.input_code,
        "input_database": ExchangeDataset.input_database,
        "output_code": ExchangeDataset.output_code,
        "output_database": ExchangeDataset.output_database,
    }

    # Build query filters
    filters = []
    for key, value in kwargs.items():
        field = mapping.get(key)
        if field is not None:
            filters.append(field == value)

    # Execute query with filters
    qs = ExchangeDataset.select().where(*filters)
    candidates = [Exchange(obj) for obj in qs]
    num_candidates = len(candidates)

    if num_candidates > 1:
        raise MultipleResults(
            f"Found {num_candidates} results for the given search. "
            "Please be more specific or double-check your system model for duplicates."
        )
    elif num_candidates == 0:
        raise UnknownObject("No exchange found matching the criteria.")

    return candidates[0]


def add_temporal_distribution_to_exchange(
    temporal_distribution: TemporalDistribution, **kwargs
):
    """
    Adds a temporal distribution to an exchange specified by kwargs.

    Parameters
    ----------
    temporal_distribution : TemporalDistribution
        TemporalDistribution to be added to the exchange.
    **kwargs :
        Arguments to specify an exchange.
            - input_node: Input node object
            - input_id: Input node database ID
            - input_code: Input node code
            - input_database: Input node database
            - output_node: Output node object
            - output_id: Output node database ID
            - output_code: Output node code
            - output_database: Output node database

    Returns
    -------
    None
        The exchange is saved with the temporal distribution.
    """
    exchange = get_exchange(**kwargs)
    exchange["temporal_distribution"] = temporal_distribution
    exchange.save()
