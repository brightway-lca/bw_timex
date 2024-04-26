import warnings
import matplotlib.pyplot as plt
import pandas as pd
import bw2data as bd

from datetime import datetime
from typing import Callable, Union, List, Optional


def extract_date_as_integer(dt_obj: datetime, time_res: Optional[str] = "year") -> int:
    """
    Converts a datetime object to an integer for a given temporal resolution (time_res)

    :param dt_obj: Datetime object.
    :time_res: time resolution to be returned: year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH
    :return: integer in the format of time_res

    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%M",
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


def extract_grouping_date_as_string(temporal_grouping: str, timestamp: datetime):
    """
    Extracts the grouping date as a string from a datetime object, based on the chosen temporal grouping.
    e.g. for temporal grouping = 'year', and timestamp = 2023-03-29T01:00:00, it extracts the string '2023'.
    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%M",
    }

    if temporal_grouping not in time_res_dict.keys():
        warnings.warn(
            'temporal_grouping: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                temporal_grouping, time_res_dict.keys()
            ),
            category=Warning,
        )
    return timestamp.strftime(time_res_dict[temporal_grouping])


def convert_grouping_date_string_to_datetime(temporal_grouping, datestring):
    """
    Converts the string of a date used for grouping back to datetime object.
    e.g. for temporal grouping = 'year', and datestring = '2023', it extracts 2023-01-01
    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%M",
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
    negative_sign: Optional[
        bool
    ] = False,  # flip the sign of the characterization function if it's an uptake and not release of emission
) -> dict:
    """
    Add a new flow or a list of flows to the available characterization functions.
    """

    # Check if the input is a single flow (str) or a list of flows (List[str])
    if isinstance(flows, str):
        # It's a single flow, add it directly
        characterization_function_dict[flows] = (func, negative_sign)
    elif isinstance(flows, list):
        # It's a list of flows, iterate and add each one
        for flow in flows:
            characterization_function_dict[flow] = (func, negative_sign)

    return characterization_function_dict


def plot_characterized_inventory_as_waterfall(
    characterized_inventory,
    metric,
    static_scores=None,
    prospective_scores=None,
    order_stacked_activities=None,
):
    """
    Plot a stacked waterfall chart of characterized inventory data. As comparison, static and prospective scores can be added.
    Only works for metric GWP at the moment.

    """
    # Check for necessary columns
    if not {"date", "activity_name", "amount"}.issubset(
        characterized_inventory.columns
    ):
        raise ValueError(
            "DataFrame must contain 'date', 'activity_name', and 'amount' columns."
        )
    if metric != "GWP":
        raise NotImplementedError(
            f"Only GWP metric is supported at the moment, not {metric}."
        )

    # Grouping and summing data
    plot_data = characterized_inventory.groupby(
        ["date", "activity_name"], as_index=False
    ).sum()

    plot_data["year"] = plot_data[
        "date"
    ].dt.year  # TODO make temporal resolution flexible

    # Optimized activity label fetching
    unique_activities = plot_data["activity_name"].unique()
    activity_labels = {
        activity: bd.get_activity(activity)["name"] for activity in unique_activities
    }
    plot_data["activity_label"] = plot_data["activity_name"].map(activity_labels)
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
