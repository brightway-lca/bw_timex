from datetime import datetime, timedelta
from typing import Union, Tuple, Optional
import bw2data as bd
import bw2calc as bc
import bw_processing as bwp
import uuid
import logging
import numpy as np
import warnings
import pandas as pd


def safety_razor(
    consumer: Union[bd.Node, Tuple[str, str], int],
    previous_producer: Union[bd.Node, Tuple[str, str], int],
    new_producer: Union[bd.Node, Tuple[str, str], int],
    datapackage: Optional[bwp.Datapackage] = None,
    amount: Optional[float] = None,
    name: Optional[str] = None,
) -> bwp.Datapackage:
    """Replace an existing edge with another edge. Zeroes out the existing edge.

    Inputs:
    consumer: Union[bd.Node, Tuple[str, str], int]
        The consuming node
    previous_producer: Union[bd.Node, Tuple[str, str], int]
        The producing node which should be replaced
    new_producer: Union[bd.Node, Tuple[str, str], int]
        The new producing node
    datapackage: Optional[bwp.Datapackage]
        Append to this datapackage, if available. Otherwise create a new datapackage.
    amount: Optional[float]
        Amount of the new edge. Will be the *sum of all (previous_producer, consumer) edge amounts if not provided.
    name: Optional[str]
        Name of this datapackage resource.

    Returns a `bw_processing.Datapackage` with the modified data."""

    def resolve_node(node: Union[bd.Node, Tuple[str, str], int]) -> bd.Node:
        """Return a Brightway node from many different input possibilities.

        This isn't super-efficient - you could look up the `id` values ahead of time.
        In production you don't need fancy logging messages."""
        if isinstance(node, tuple):
            assert len(node) == 2
            return bd.get_node(database=node[0], code=node[1])
        elif isinstance(node, int):
            return bd.get_node(id=int)
        elif isinstance(node, bd.Node):
            return node
        else:
            raise ValueError(f"Can't understand {node}")

    consumer = resolve_node(consumer)
    previous_producer = resolve_node(previous_producer)
    new_producer = resolve_node(new_producer)

    assert new_producer.get("type", "process") == "process", "Wrong type of edge source"
    # Remove if creating new edge instead of moving or replacing existing an edge
    assert any(exc.input == previous_producer for exc in consumer.technosphere())

    if not name:
        name = uuid.uuid4().hex
        # logger.info(f"Using random name {name}")

    if not amount:
        amount = sum(
            exc["amount"]
            for exc in consumer.technosphere()
            if exc.input == previous_producer
        )
        # logger.info(f"Using database net amount {amount}")

    # logger.info(f"Zeroing exchange from {previous_producer} to {consumer}")
    # logger.info(f"Adding exchange of {amount} {new_producer} to {consumer}")

    if datapackage is None:
        datapackage = bwp.create_datapackage()

    datapackage.add_persistent_vector(
        # This function would need to be adapted for biosphere edges
        matrix="technosphere_matrix",
        name=name,
        data_array=np.array([0, amount], dtype=float),
        indices_array=np.array(
            [(previous_producer.id, consumer.id), (new_producer.id, consumer.id)],
            dtype=bwp.INDICES_DTYPE,
        ),
        flip_array=np.array([False, True], dtype=bool),
    )
    return datapackage

def add_column_interpolation_weights_on_timeline(tl_df, dates_list, interpolation_type="linear"):
    """
    Add a column to a timeline with the weights for an interpolation between the two nearest dates, from the list of dates from the available databases.

    :param tl_df: Timeline as a dataframe.
    :param dates_list: List of years of the available databases.
    :param interpolation_type: Type of interpolation between the nearest lower and higher dates. For now, 
    only "linear" is available.
    
    :return: Timeline as a dataframe with a column 'interpolation_weights' (object:dictionnary) added.
    -------------------
    Example:
    >>> dates_list = [
        datetime.strptime("2020", "%Y"),
        datetime.strptime("2022", "%Y"),
        datetime.strptime("2025", "%Y"),
    ]
    >>> add_column_interpolation_weights_on_timeline(tl_df, dates_list, interpolation_type="linear")
    """
    def find_closest_date(target, dates):
        """
        Find the closest date to the target in the dates list.
    
        :param target: Target datetime.datetime object.
        :param dates: List of datetime.datetime objects.
        :return: Dictionary with the key as the closest datetime.datetime object from the list and a value of 1.
    
        ---------------------
        # Example usage
        target = datetime.strptime("2023-01-15", "%Y-%m-%d")
        dates_list = [
            datetime.strptime("2020", "%Y"),
            datetime.strptime("2022", "%Y"),
            datetime.strptime("2025", "%Y"),
        ]
    
        print(closest_date(target, dates_list))
        """
    
        # If the list is empty, return None
        if not dates:
            return None
    
        # Sort the dates
        dates = sorted(dates)
    
        # Use min function with a key based on the absolute difference between the target and each date
        closest = min(dates, key=lambda date: abs(target - date))
    
        return {closest.year: 1}
    
    def get_weights_for_interpolation_between_nearest_years(date, dates_list, interpolation_type="linear"):
        """
        Find the nearest dates (before and after) a given date in a list of dates and calculate the interpolation weights.
    
        :param date: Target date.
        :param dates_list: List of years of the available databases.
        :param interpolation_type: Type of interpolation between the nearest lower and higher dates. For now, 
        only "linear" is available.
        
        :return: Dictionary with years of the available databases to use as keys and the weights for interpolation as values.
        -------------------
        Example:
        >>> dates_list = [
            datetime.strptime("2020", "%Y"),
            datetime.strptime("2022", "%Y"),
            datetime.strptime("2025", "%Y"),
        ]
        >>> date_test = datetime(2021,10,11)
        >>> add_column_interpolation_weights_on_timeline(date_test, dates_list, interpolation_type="linear")
        """
        dates_list = sorted (dates_list)
        
        diff_dates_list = [date - x for x in dates_list]
        if timedelta(0) in diff_dates_list:
            exact_match = dates_list[diff_dates_list.index(timedelta(0))]
            return {exact_match.year: 1}
    
        closest_lower = min(
            dates_list, 
            key=lambda x: abs(date - x) if (date - x) > timedelta(0) else timedelta.max
        )
        closest_higher = min(
            dates_list, 
            key=lambda x: abs(date - x) if (date - x) < timedelta(0) else timedelta.max
        )
    
        if closest_lower == closest_higher:
            warnings.warn("Date outside the range of dates covered by the databases.", category=Warning)
            return {closest_lower.year: 1}
            
        if interpolation_type == "linear":
            weight = int((date - closest_lower).total_seconds())/int((closest_higher - closest_lower).total_seconds())
        else:
            raise ValueError(f"Sorry, but {interpolation_type} interpolation is not available yet.")
        return {closest_lower.year: weight, closest_higher.year: 1-weight}
        if "date" not in list(tl_df.columns):
            raise ValueError("The timeline does not contain dates.")
        
    if interpolation_type == "nearest":
        tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: find_closest_date(x, dates_list))
        return tl_df
        
    tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: get_weights_for_interpolation_between_nearest_years(x, dates_list, interpolation_type))
    return tl_df

def create_grouped_edge_dataframe(tl,dates_list, time_res="year", interpolation_type="linear"):
    """create a timeline dataframe where the various activities are aggregated based on their datetime.
    Currently they are all aggregated to the same time resolution, but in the future this can be differentiated
    for different types of activties.
    
    arguments:
    tl              : Should be a timeline from edge_extractor
    dates_list      : dictionary containing the different background databases and their years   # NEED TO UPDATE THIS DESCRIPTION
    time_res        : the time resolution to which the activitiess need to be aggregated. Should be in {'year'}
    interpolation   : how to interpolate between different background databases

    TODO: make  categorial aggregation possible. EG electricity markets on hourly bases, other processes on yearly bases. 
    Maybe include seasonal resolution
    """
    # For now only yearly ressolution possible
    time_res = 'year'
    
    edges_dict_list = [{"datetime": edge.distribution.date, 'amount': edge.distribution.amount, 'producer': edge.producer, 'consumer': edge.consumer, "leaf": edge.leaf} for edge in tl]
    edges_dataframe = pd.DataFrame(edges_dict_list)
    edges_dataframe = edges_dataframe.explode(['datetime', "amount"])
    edges_dataframe['time_stamp'] = edges_dataframe['datetime'].apply(lambda x: x.year)
    edge_dataframe = edges_dataframe.loc[:, "amount":].groupby(['year', 'producer', 'consumer']).sum().reset_index()
    edge_dataframe['date'] = edge_dataframe['year'].apply(lambda x: datetime(x, 1, 1))
    timeline_df_with_interpolation = add_column_interpolation_weights_on_timeline(edge_dataframe, dates_list, interpolation_type="linear")
    timeline_df_with_interpolation['producer_name'] = timeline_df_with_interpolation.producer.apply(lambda x: bd.get_node(id=x)["name"])
    return edge_dataframe

def create_patches_from_timeline(timeline, database_date_dict):
    """
    Creates patches from a given timeline.

    :param timeline: The input timeline.
    :param database_date_dict: Dictionary of available prospective database dates and their names.
    :return: A list of patches as datapackages.
    """

    def extract_new_edges_from_row(row, database_dates_dict):
        """
        Extracts new edges based on a row from the timeline DataFrame.

        :param row: A row from the timeline DataFrame.
        :param database_dates_dict: Dictionary of available prospective database dates and their names.
        :return: List of new edges.
        """
        consumer = bd.get_node(id=row.consumer)
        previous_producer = bd.get_node(id=row.producer)

        # Create new edges based on interpolation_weights from the row
        return [
            (
                consumer,
                previous_producer,
                bd.get_activity((database_dates_dict[date], previous_producer["code"])),
                bd.get_node(
                    **{
                        "database": database_dates_dict[date],
                        "name": previous_producer["name"],
                        "product": previous_producer["reference product"],
                        "location": previous_producer["location"],
                    }
                ),
                row.amount * share,
            )
            for date, share in row.interpolation_weights.items()
        ]

    # Use a nested list comprehension to generate patches from the timeline
    return [
        safety_razor(*edge)
        for row in timeline.df.itertuples()
        for edge in extract_new_edges_from_row(row, database_date_dict)
    ]

def get_datapackage_from_edge_timeline(
    timeline: pd.DataFrame, 
    database_date_dict: dict, 
    datapackage: Optional[bwp.Datapackage] = None,
    name: Optional[str] = None,
) -> bwp.Datapackage:
    """
    Creates patches from a given timeline. # UPDATE THIS!!!

    Inputs:
    timeline: list
        A timeline of edges, typically created from EdgeExtracter.create_edge_timeline()
    database_date_dict: dict
        A dict of the available prospective database years and their names.  
        For example:
        database_date_dict = {
            2030: 'wind-example-2030',
            2040: 'wind-example-2040',
            2050: 'wind-example-2050',
        }  
    datapackage: Optional[bwp.Datapackage]
        Append to this datapackage, if available. Otherwise create a new datapackage.
    name: Optional[str]
        Name of this datapackage resource.

    Returns:
    bwp.Datapackage
        A list of patches formatted as datapackages.
    """

    def add_row_to_datapackage(row, datapackage, database_dates_dict, new_nodes): 
        """
        Extracts new edges based on a row from the timeline DataFrame.

        :param row: A row from the timeline DataFrame.
        :param database_dates_dict: Dictionary of available prospective database dates and their names.
        :return: List of new edges; each edge contains the consumer, the previous producer, the new producer, and the amount.
        """
        new_consumer_id = abs(row.consumer)*1000000+row.year
        new_producer_id = abs(row.producer)*1000000+row.year
        new_nodes.add(new_consumer_id)
        new_nodes.add(new_producer_id)
        previous_producer_id = row.producer
        previous_producer_node = bd.get_node(id=previous_producer_id) # in future versions, insead of getting node, just provide list of producer ids
        
        # Check if previous producer comes from prospective databases
        if not previous_producer_node['database'] in database_dates_dict.values():
            datapackage.add_persistent_vector(
                        matrix="technosphere_matrix",
                        name=uuid.uuid4().hex,
                        data_array=np.array([row.amount], dtype=float),
                        indices_array=np.array(
                            [(new_producer_id, new_consumer_id)],
                            dtype=bwp.INDICES_DTYPE,
                        ),
                        flip_array=np.array([True], dtype=bool),
                )
        
        else:
            # Create new edges based on interpolation_weights from the row
            for date, share in row.interpolation_weights.items():
                print(date, database_date_dict[date], previous_producer_node["name"])
                new_producer_id = bd.get_node(
                        **{
                            "database": database_dates_dict[date],
                            "name": previous_producer_node["name"],
                            # "product": previous_producer_node["reference product"],
                            # "location": previous_producer_node["location"],
                        }
                    ).id   # Get new producer id by looking for the same activity in the new database
                print(previous_producer_id, row.consumer, new_producer_id, new_consumer_id)
                datapackage.add_persistent_vector(
                        matrix="technosphere_matrix",
                        name=uuid.uuid4().hex,
                        data_array=np.array([row.amount*share], dtype=float),
                        indices_array=np.array(
                            [(new_producer_id, new_consumer_id)],
                            dtype=bwp.INDICES_DTYPE,
                        ),
                        flip_array=np.array([True], dtype=bool),
                )
    
    if not name:
        name = uuid.uuid4().hex
        # logger.info(f"Using random name {name}")

    if datapackage is None:
        datapackage = bwp.create_datapackage(sum_intra_duplicates=False)

    new_nodes = set()
    for row in timeline.itertuples():
        add_row_to_datapackage(row, datapackage, database_date_dict, new_nodes)
        
    datapackage.add_persistent_vector(
        matrix="technosphere_matrix",
        name=uuid.uuid4().hex,
        data_array=np.ones(len(new_nodes)),
        indices_array=np.array([(i, i) for i in new_nodes], dtype=bwp.INDICES_DTYPE),
    )

    return datapackage


def extract_date_as_integer(dt_obj : datetime, time_res : Optional[str] ='year') -> int:
    """
    Converts a datetime object to an integer in the format YYYY 
    #FIXME: ideally we want to add YYYYMMDDHH to the ids, but this cretaes integers that are too long for 32-bit C long

    :param dt_obj: Datetime object.
    :time_res: time resolution to be returned: year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH
    :return: INTEGER in the format YYYY.
    """
    time_res_dict = {'year':'%Y','month':'%Y%m','day':'%Y%m%d','hour':'%Y%m%d%M'}
    if time_res not in time_res_dict.keys():
        warnings.warn('time_res: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                      time_res, time_res_dict.keys()), category=Warning)
    formatted_date = dt_obj.strftime('{}'.format(time_res_dict[time_res]))
    date_as_integer = int(formatted_date)

    return date_as_integer
