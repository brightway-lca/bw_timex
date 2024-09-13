import warnings
from datetime import datetime, timedelta
from typing import Callable, KeysView, Union

import bw2data as bd
import numpy as np
import pandas as pd
from bw2calc import LCA
from bw2data.configuration import labels

from .edge_extractor import Edge, EdgeExtractor
from .utils import (
    convert_date_string_to_datetime,
    extract_date_as_integer,
    extract_date_as_string,
)


class TimelineBuilder:
    """
    This class is responsible for building a timeline of processes based on the temporal relationship of the priority-first graph traversal.

    First, the `EdgeExtractor` is called and it extracts a timeline of exchanges (edge_timeline) with temporal information.
    Identical edges within temporal grouping (e.g. year, month, day, hour) are then grouped together and the amount of exchanges is summed up.
    """

    def __init__(
        self,
        slca: LCA,
        edge_filter_function: Callable,
        database_date_dict: dict,
        database_date_dict_static_only: dict,
        activity_time_mapping_dict: dict,
        node_id_collection_dict: dict,
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
        cutoff: float = 1e-9,
        max_calc: int = 2000,
        *args,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        slca: LCA
            A static LCA object.
        edge_filter_function: Callable
            A callable that filters edges. If not provided, a function that always returns False is used.
        database_date_dict: dict
            A dictionary mapping databases to dates.
        activity_time_mapping_dict: dict
          A dictionary to map processes to specific times.
        temporal_grouping: str, optional
            The temporal grouping to be used. Default is "year".
        interpolation_type: str, optional
            The type of interpolation to be used to select the background databases. Default is "linear".
        cutoff:
            The cutoff value for the graph traversal. Default is 1e-9.
        max_calc:
            The maximum number of calculations to be performed by the graph traversal. Default is 1e4.
        args:   Variable length argument list
            Keyword arguments passed to the EdgeExtractor which inherits from TemporalisLCA. Here, things like the further settings for graph traversal can be set. For details, see bw_temporalis.TemporalisLCA.
        kwargs: Arbitrary keyword arguments
            Keyword arguments passed to the EdgeExtractor which inherits from TemporalisLCA.
        """
        self.slca = slca
        self.edge_filter_function = edge_filter_function
        self.database_date_dict = database_date_dict
        self.database_date_dict_static_only = database_date_dict_static_only
        self.activity_time_mapping_dict = activity_time_mapping_dict
        self.node_id_collection_dict = node_id_collection_dict
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type
        self.cutoff = cutoff
        self.max_calc = max_calc

        # Finding indices of activities from background databases that are known to be static, i.e. have no temporal distributions connecting to them.
        # These will be be skipped in the graph traversal.
        static_activity_db_indices = [
            node_id
            for node_id in self.node_id_collection_dict[
                "demand_dependent_background_node_ids"
            ]
            if node_id
            not in self.node_id_collection_dict[
                "first_level_background_node_ids_static"
            ]
        ]

        self.edge_extractor = EdgeExtractor(
            slca,
            *args,
            edge_filter_function=edge_filter_function,
            cutoff=self.cutoff,
            max_calc=self.max_calc,
            static_activity_indices=set(static_activity_db_indices),
            **kwargs,
        )
        self.edge_timeline = self.edge_extractor.build_edge_timeline()

    def build_timeline(self) -> pd.DataFrame:
        """
        Create a dataframe with grouped, time-explicit edges and, for each grouped edge, interpolate to the database with the closest time of representativeness.

        Edges from same producer to same consumer that occur at different times within the same time window (temporal_grouping) are grouped together. Possible temporal groupings are "year", "month", "day" and "hour".

        For edges between forground and background system, the column "interpolation weights" assigns the ratio [0-1] of the edge's amount to be taken from the database with the closest time of representativeness. If a process is in the foreground system only, the interpolation weight is set to None.
        Available interpolation types are:
            - "linear": linear interpolation between the two closest databases, based on temporal distance.
            - "closest": closest database is assigned 1

        Parameters
        ----------
        None
            (all are already passed during instantiation)

        Returns
        -------
        pd.DataFrame
            A timeline with grouped, time-explicit edges and interpolation weights to background databases.
        """

        # check if database names match with databases in BW project
        self.check_database_names()

        # Check if temporal_grouping is a valid value
        valid_temporal_groupings = ["year", "month", "day", "hour"]
        if self.temporal_grouping not in valid_temporal_groupings:
            raise ValueError(
                f"Invalid value for 'temporal_grouping'. Allowed values are {valid_temporal_groupings}."
            )

        # Extract edge data into a list of dictionaries
        edges_data = [self.extract_edge_data(edge) for edge in self.edge_timeline]

        # Convert list of dictionaries to dataframe
        edges_df = pd.DataFrame(edges_data)

        # adjust the sign for substitution exchanges:
        edges_df["amount"] = edges_df["amount"] * edges_df["edge_type"].apply(
            lambda x: self.adjust_sign_of_amount_based_on_edge_type(x)
        )

        # Explode datetime and amount columns: each row with multiple dates and amounts is exploded into multiple rows with one date and one amount
        edges_df = edges_df.explode(["consumer_date", "producer_date", "amount"])
        edges_df.drop_duplicates(inplace=True)
        edges_df = edges_df[edges_df["amount"] != 0]

        # For the Functional Unit: set consumer date = producer date as it occurs at the same time
        edges_df.loc[edges_df["consumer"] == -1, "consumer_date"] = edges_df.loc[
            edges_df["consumer"] == -1, "producer_date"
        ]

        # extract grouping time of consumer and producer: processes occuring at different times withing in the same time window of grouping get the same grouping time
        edges_df["consumer_grouping_time"] = edges_df["consumer_date"].apply(
            lambda x: extract_date_as_string(self.temporal_grouping, x)
        )
        edges_df["producer_grouping_time"] = edges_df["producer_date"].apply(
            lambda x: extract_date_as_string(self.temporal_grouping, x)
        )

        # group unique pair of consumer and producer with the same grouping times
        grouped_edges = (
            edges_df.groupby(
                [
                    "producer_grouping_time",
                    "consumer_grouping_time",
                    "producer",
                    "consumer",
                ]
            )
            .agg({"amount": "sum"})
            .reset_index()
        )

        # convert grouping times, which was only used as intermediate variable, back to datetime
        grouped_edges["date_producer"] = grouped_edges["producer_grouping_time"].apply(
            lambda x: convert_date_string_to_datetime(self.temporal_grouping, x)
        )
        grouped_edges["date_consumer"] = grouped_edges["consumer_grouping_time"].apply(
            lambda x: convert_date_string_to_datetime(self.temporal_grouping, x)
        )

        # add dates as integers as hashes to the dataframe
        grouped_edges["hash_producer"] = grouped_edges["date_producer"].apply(
            lambda x: extract_date_as_integer(x, time_res=self.temporal_grouping)
        )

        grouped_edges["hash_consumer"] = grouped_edges["date_consumer"].apply(
            lambda x: extract_date_as_integer(x, time_res=self.temporal_grouping)
        )

        # add new processes to time mapping dict
        for row in grouped_edges.itertuples():
            self.activity_time_mapping_dict.add(
                (
                    ("temporalized", bd.get_node(id=row.producer)["code"]),
                    row.hash_producer,
                )
            )

        def _get_time_mapping_key(node_id: int, node_hash: int) -> tuple:
            try:
                return self.activity_time_mapping_dict[
                    (("temporalized", bd.get_node(id=node_id)["code"]), node_hash)
                ]
            except:
                return self.activity_time_mapping_dict[
                    ((bd.get_node(id=node_id).key), node_hash)
                ]

        # store the ids from the time_mapping_dict in dataframe
        grouped_edges["time_mapped_producer"] = grouped_edges.apply(
            lambda row: _get_time_mapping_key(row.producer, row.hash_producer),
            axis=1,
        )

        grouped_edges["time_mapped_consumer"] = grouped_edges.apply(
            lambda row: (
                _get_time_mapping_key(row.consumer, row.hash_consumer)
                if row.consumer != -1
                else -1
            ),
            axis=1,
        )

        # Add interpolation weights to background databases to the dataframe
        grouped_edges = self.add_column_interpolation_weights_to_timeline(
            grouped_edges,
            interpolation_type=self.interpolation_type,
        )

        # Retrieve producer and consumer names
        grouped_edges["producer_name"] = grouped_edges.producer.apply(
            lambda x: bd.get_node(id=x)["name"]
        )
        grouped_edges["consumer_name"] = grouped_edges.consumer.apply(
            self.get_consumer_name
        )

        # Reorder columns
        grouped_edges = grouped_edges[
            [
                "hash_producer",
                "time_mapped_producer",
                "date_producer",
                "producer",
                "producer_name",
                "hash_consumer",
                "time_mapped_consumer",
                "date_consumer",
                "consumer",
                "consumer_name",
                "amount",
                "interpolation_weights",
            ]
        ]

        return grouped_edges

    ###################################################
    # underlying functions called by build_timeline() #
    ###################################################

    def check_database_names(self) -> None:
        """
        Check that the strings of the databases exist in the databases of the brightway project.

        """
        for db in self.database_date_dict_static_only.keys():
            assert (
                db in bd.databases
            ), f"{db} is not in your brightway project databases."

    def extract_edge_data(self, edge: Edge) -> dict:
        """
        Stores the attributes of an Edge instance in a dictionary.

        Parameters
        ----------
        edge: Edge
            Edge instance

        Returns
        -------
        dict
            Dictionary with the attributes of the edge instance.
        """
        try:
            consumer_date = edge.abs_td_consumer.date
            consumer_date = np.array(
                [consumer_date for i in range(len(edge.td_producer))]
            ).T.flatten()
        except AttributeError:
            consumer_date = None

        return {
            "producer": edge.producer,
            "consumer": edge.consumer,
            "consumer_date": consumer_date,
            "producer_date": edge.abs_td_producer.date,
            "amount": edge.abs_td_producer.amount,
            "edge_type": edge.edge_type,
        }

    def adjust_sign_of_amount_based_on_edge_type(self, edge_type):
        """
        It checks if the an exchange is of type substitution or a technosphere exchange, based on bw2data labelling convention, and adjusts the amount accordingly.
        Flips the sign of the amount value in the timeline for substitution (positive technosphere) exchanges.

        Parameters
        ----------
        edge_type: str
            Type of the edge, as defined in the exchange data.

        Returns
        -------
        int
            Multiplier for the amount value, 1 for technosphere exchanges, -1 for substitution exchanges.

        """

        if (
            edge_type in labels.technosphere_negative_edge_types
        ):  # variants of technosphere lables
            multiplicator = 1
        elif (
            edge_type in labels.technosphere_positive_edge_types
        ):  # variants of production AND substitution labels
            multiplicator = 1
            if (
                edge_type in labels.substitution_edge_types
            ):  # overwrite variants of substitution labels
                multiplicator = -1
        else:
            # Raise a warning for unrecognized edge type
            warnings.warn(f"Unrecognized type in this edge: {edge_type}", UserWarning)
            raise ValueError("Unrecognized edge type")
        return multiplicator

    def add_column_interpolation_weights_to_timeline(
        self,
        tl_df: pd.DataFrame,
        interpolation_type: str = "linear",
    ) -> pd.DataFrame:
        """
        Add a column to a timeline with the weights for an interpolation between the two nearest dates, from the list of dates of the available databases.

        Parameters
        ----------
        tl_df: pd.DataFrame
            Timeline as a dataframe.
        interpolation_type: str, optional
            Type of interpolation between the nearest lower and higher dates. Available options: "linear" and "nearest", defaulting to "linear".

        Returns
        -------
        pd.DataFrame
            Timeline as a dataframe with a column 'interpolation_weights' added, this column looks like {database_name: weight, database_name: weight}.
        """
        if not self.database_date_dict_static_only:
            tl_df["interpolation_weights"] = None
            warnings.warn(
                "No time-explicit databases are provided. Mapping to time-explicit databases is not possible.",
                category=Warning,
            )
            return tl_df

        dates_list = [
            date
            for date in self.database_date_dict_static_only.values()
            if isinstance(date, datetime)
        ]
        if "date_producer" not in list(tl_df.columns):
            raise ValueError("The timeline does not contain dates.")

        # create reversed dict {date: database} with only static "background" db's
        self.reversed_database_date_dict = {
            v: k
            for k, v in self.database_date_dict_static_only.items()
            if isinstance(v, datetime)
        }

        if self.interpolation_type == "nearest":
            tl_df["interpolation_weights"] = tl_df["date_producer"].apply(
                lambda x: self.find_closest_date(x, dates_list)
            )

        if self.interpolation_type == "linear":
            tl_df["interpolation_weights"] = tl_df["date_producer"].apply(
                lambda x: self.get_weights_for_interpolation_between_nearest_years(
                    x, dates_list, interpolation_type
                )
            )

        else:
            raise ValueError(
                f"Sorry, but {interpolation_type} interpolation is not available yet."
            )

        tl_df["interpolation_weights"] = tl_df.apply(
            self.add_interpolation_weights_at_intersection_to_background, axis=1
        )  # add the weights to the timeline for processes at intersection

        return tl_df

    def find_closest_date(self, target: datetime, dates: KeysView[datetime]) -> dict:
        """
        Find the closest date to the target in the dates list.

        Parameters
        ----------
        target : datetime.datetime
            Target datetime object.
        dates : KeysView[datetime]
            List of datetime.datetime objects.

        Returns
        -------
        dict
            Dictionary with the key as the closest datetime.datetime object from the list and a value of 1.
        """

        # If the list is empty, return None
        if not dates:
            return None

        # Sort the dates
        dates = sorted(dates)

        # Use min function with a key based on the absolute difference between the target and each date
        closest = min(dates, key=lambda date: abs(target - date))

        return {closest: 1}

    def get_weights_for_interpolation_between_nearest_years(
        self,
        reference_date: datetime,
        dates_list: KeysView[datetime],
        interpolation_type: str = "linear",
    ) -> dict:
        """
        Find the nearest dates (lower and higher) for a given date from a list of dates and calculate the interpolation weights based on temporal proximity.

        Parameters
        ----------
        reference_date : datetime
            Target date.
        dates_list : KeysView[datetime]
            List of datetime objects representing the temporal representativeness of the available databases.
        interpolation_type : str, optional
            Type of interpolation between the nearest lower and higher dates. For now, only "linear" is available.

        Returns
        -------
        dict
            Dictionary with datetimes of the available closest databases as keys and the weights for interpolation as values.
        """
        dates_list = sorted(dates_list)

        diff_dates_list = [reference_date - x for x in dates_list]

        if timedelta(0) in diff_dates_list:  # date of process == date of database
            exact_match = dates_list[diff_dates_list.index(timedelta(0))]
            return {exact_match: 1}

        closest_lower = None
        closest_higher = None

        # select the closest lower and higher dates of the database in regards to the date of process
        for date in dates_list:
            if date < reference_date:
                if (
                    closest_lower is None
                    or reference_date - date < reference_date - closest_lower
                ):
                    closest_lower = date
            elif date > reference_date:
                if (
                    closest_higher is None
                    or date - reference_date < closest_higher - reference_date
                ):
                    closest_higher = date

        if closest_lower is None:
            warnings.warn(
                f"Reference date {reference_date} is lower than all provided dates. Data will be taken from the closest higher year.",
                category=Warning,
            )
            return {closest_higher: 1}

        if closest_higher is None:
            warnings.warn(
                f"Reference date {reference_date} is higher than all provided dates. Data will be taken from the closest lower year.",
                category=Warning,
            )
            return {closest_lower: 1}

        if self.interpolation_type == "linear":
            weight = int((reference_date - closest_lower).total_seconds()) / int(
                (closest_higher - closest_lower).total_seconds()
            )
        else:
            raise ValueError(
                f"Sorry, but {interpolation_type} interpolation is not available yet."
            )
        return {closest_lower: 1 - weight, closest_higher: weight}

    def add_interpolation_weights_at_intersection_to_background(
        self, row
    ) -> Union[dict, None]:
        """
        returns the interpolation weights to background databases only for those exchanges, where the producing process
        actually comes from a background database (temporal markets).

        Only these processes are receiving inputs from the background databases.
        All other process in the timeline are not directly linked to the background, so the interpolation weight info is not needed and set to None

        Parameters
        ----------
        row : pd.Series
            Row of the timeline dataframe
        Returns
        -------
        dict
            Dictionary with the name of databases and interpolation weights.
        """

        if (
            row["producer"]
            in self.node_id_collection_dict["first_level_background_node_ids_static"]
        ):
            return {
                self.reversed_database_date_dict[x]: v
                for x, v in row["interpolation_weights"].items()
            }
        return None

    def get_consumer_name(self, idx: int) -> str:
        """
        Returns the name of consumer node.
        If consuming node is the functional unit, returns -1.

        Parameters
        ----------
        id : int
            Id of node.

        Returns
        -------
        str
            Name of the node or -1
        """
        try:
            return bd.get_node(id=idx)["name"]
        except:
            return "-1"  # functional unit
