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


# def add_column_interpolation_weights_to_timeline(tl_df, dates_list, interpolation_type="linear"):
#     """
#     Add a column to a timeline with the weights for an interpolation between the two nearest dates, from the list of dates from the available databases.

#     :param tl_df: Timeline as a dataframe.
#     :param dates_list: List of years of the available databases.
#     :param interpolation_type: Type of interpolation between the nearest lower and higher dates. For now, 
#     only "linear" is available.
    
#     :return: Timeline as a dataframe with a column 'interpolation_weights' (object:dictionnary) added.
#     -------------------
#     Example:
#     >>> dates_list = [
#         datetime.strptime("2020", "%Y"),
#         datetime.strptime("2022", "%Y"),
#         datetime.strptime("2025", "%Y"),
#     ]
#     >>> add_column_interpolation_weights_on_timeline(tl_df, dates_list, interpolation_type="linear")
#     """
#     def find_closest_date(target, dates):
#         """
#         Find the closest date to the target in the dates list.
    
#         :param target: Target datetime.datetime object.
#         :param dates: List of datetime.datetime objects.
#         :return: Dictionary with the key as the closest datetime.datetime object from the list and a value of 1.
    
#         ---------------------
#         # Example usage
#         target = datetime.strptime("2023-01-15", "%Y-%m-%d")
#         dates_list = [
#             datetime.strptime("2020", "%Y"),
#             datetime.strptime("2022", "%Y"),
#             datetime.strptime("2025", "%Y"),
#         ]
    
#         print(closest_date(target, dates_list))
#         """
    
#         # If the list is empty, return None
#         if not dates:
#             return None
    
#         # Sort the dates
#         dates = sorted(dates)
    
#         # Use min function with a key based on the absolute difference between the target and each date
#         closest = min(dates, key=lambda date: abs(target - date))
    
#         return {closest.year: 1}
    
#     def get_weights_for_interpolation_between_nearest_years(date, dates_list, interpolation_type="linear"):
#         """
#         Find the nearest dates (before and after) a given date in a list of dates and calculate the interpolation weights.
    
#         :param date: Target date.
#         :param dates_list: List of years of the available databases.
#         :param interpolation_type: Type of interpolation between the nearest lower and higher dates. For now, 
#         only "linear" is available.
        
#         :return: Dictionary with years of the available databases to use as keys and the weights for interpolation as values.
#         -------------------
#         Example:
#         >>> dates_list = [
#             datetime.strptime("2020", "%Y"),
#             datetime.strptime("2022", "%Y"),
#             datetime.strptime("2025", "%Y"),
#         ]
#         >>> date_test = datetime(2021,10,11)
#         >>> add_column_interpolation_weights_on_timeline(date_test, dates_list, interpolation_type="linear")
#         """
#         dates_list = sorted (dates_list)
        
#         diff_dates_list = [date - x for x in dates_list]
#         if timedelta(0) in diff_dates_list:
#             exact_match = dates_list[diff_dates_list.index(timedelta(0))]
#             return {exact_match.year: 1}
    
#         closest_lower = min(
#             dates_list, 
#             key=lambda x: abs(date - x) if (date - x) > timedelta(0) else timedelta.max
#         )
#         closest_higher = min(
#             dates_list, 
#             key=lambda x: abs(date - x) if (date - x) < timedelta(0) else timedelta.max
#         )
    
#         if closest_lower == closest_higher:
#             warnings.warn("Date outside the range of dates covered by the databases.", category=Warning)
#             return {closest_lower.year: 1}
            
#         if interpolation_type == "linear":
#             weight = int((date - closest_lower).total_seconds())/int((closest_higher - closest_lower).total_seconds())
#         else:
#             raise ValueError(f"Sorry, but {interpolation_type} interpolation is not available yet.")
#         return {closest_lower.year: weight, closest_higher.year: 1-weight}
#         if "date" not in list(tl_df.columns):
#             raise ValueError("The timeline does not contain dates.")
        
#     if interpolation_type == "nearest":
#         tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: find_closest_date(x, dates_list))
#         return tl_df
        
#     tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: get_weights_for_interpolation_between_nearest_years(x, dates_list, interpolation_type))
#     return tl_df

def create_grouped_edge_dataframe(tl, dates_list, interpolation_type="linear"):
    edges_dict_list = [{"datetime": edge.distribution.date, 'amount': edge.distribution.amount, 'producer': edge.producer, 'consumer': edge.consumer, "leaf": edge.leaf} for edge in tl]
    edges_dataframe = pd.DataFrame(edges_dict_list)
    edges_dataframe = edges_dataframe.explode(['datetime', "amount"])
    edges_dataframe['year'] = edges_dataframe['datetime'].apply(lambda x: x.year)
    edge_dataframe = edges_dataframe.loc[:, "amount":].groupby(['year', 'producer', 'consumer']).sum().reset_index()
    edge_dataframe['date'] = edge_dataframe['year'].apply(lambda x: datetime(x, 1, 1))
    timeline_df_with_interpolation = add_column_interpolation_weights_to_timeline(edge_dataframe, dates_list, interpolation_type=interpolation_type)
    timeline_df_with_interpolation['producer_name'] = timeline_df_with_interpolation.producer.apply(lambda x: bd.get_node(id=x)["name"])
    return edge_dataframe

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
        if row.consumer == -1:
            warnings.warn(f"\nMay I have your attention please? Will the real producer please stand up? \n{bd.get_node(id=row.producer)} has no producer. Production exchange missing?", category=Warning)
            return
        
        new_consumer_id = row.consumer*100000+row.year
        new_producer_id = row.producer*100000+row.year
        assert new_consumer_id > 0 , f"New consumer id for {row.consumer} is negative"
        assert new_producer_id > 0, f"New producer id for {row.producer} is negative"
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
                print(f"Choosing {database_date_dict[date]} for year {date}; Previous producer was {previous_producer_node.key}")
                new_producer_id = bd.get_node(
                        **{
                            "database": database_dates_dict[date],
                            "name": previous_producer_node["name"],
                            # "product": previous_producer_node["reference product"],
                            # "location": previous_producer_node["location"],
                        }
                    ).id   # Get new producer id by looking for the same activity in the new database
                print(f'Previous producer: {previous_producer_node.key}, id = {previous_producer_id}')
                print(f'Previous consumer: {bd.get_node(id=row.consumer).key}, id = {row.consumer}')
                print(f'New producer id = {new_producer_id}')
                print(f'New consumer id = {new_consumer_id}')
                print()
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
        datapackage = bwp.create_datapackage()

    # for db in bd.databases:
    #     datapackage += [bd.Database(db).datapackage()]

    new_nodes = set()
    for row in timeline.itertuples():
        add_row_to_datapackage(row, datapackage, database_date_dict, new_nodes)
    
    # Adding ones on diagonal for new nodes
    datapackage.add_persistent_vector(
        matrix="technosphere_matrix",
        name=uuid.uuid4().hex,
        data_array=np.ones(len(new_nodes)),
        indices_array=np.array([(i, i) for i in new_nodes], dtype=bwp.INDICES_DTYPE),
    )

    return datapackage

def add_column_interpolation_weights_to_timeline(tl_df, dates_list, interpolation_type="linear"):
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

from bw2data import (
    Database,
    Method,
    Normalization,
    Weighting,
    databases,
    methods,
    normalizations,
    projects,
    weightings,
)
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.backends.schema import get_id
from bw2data.errors import Brightway2Project

def unpack(dct):
    for obj in dct:
        if hasattr(obj, "key"):
            yield obj.key
        else:
            yield obj

def prepare_medusa_lca_inputs(
    demand=None,
    method=None,
    weighting=None,
    normalization=None,
    demands=None,
    remapping=True,
    demand_database_last=True,
):
    """Prepare LCA input arguments in Brightway 2.5 style. 
    ORIGINALLY FROM bw2data.compat.py"""
    if not projects.dataset.data.get("25"):
        raise Brightway2Project(
            "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
        )

    databases.clean()
    data_objs = []
    remapping_dicts = None

    # if demands:
    #     demand_database_names = [
    #         db_label for dct in demands for db_label, _ in unpack(dct)
    #     ]
    # elif demand:
    #     demand_database_names = [db_label for db_label, _ in unpack(demand)]
    # else:
    #     demand_database_names = []

    demand_database_names = [db_label for db_label in databases] # Always load all databases

    if demand_database_names:
        database_names = set.union(
            *[
                Database(db_label).find_graph_dependents()
                for db_label in demand_database_names
            ]
        )

        if demand_database_last:
            database_names = [
                x for x in database_names if x not in demand_database_names
            ] + demand_database_names

        data_objs.extend([Database(obj).datapackage() for obj in database_names])

        if remapping:
            # This is technically wrong - we could have more complicated queries
            # to determine what is truly a product, activity, etc.
            # However, for the default database schema, we know that each node
            # has a unique ID, so this won't produce incorrect responses,
            # just too many values. As the dictionary only exists once, this is
            # not really a problem.
            reversed_mapping = {
                i: (d, c)
                for d, c, i in AD.select(AD.database, AD.code, AD.id)
                .where(AD.database << database_names)
                .tuples()
            }
            remapping_dicts = {
                "activity": reversed_mapping,
                "product": reversed_mapping,
                "biosphere": reversed_mapping,
            }

    if method:
        assert method in methods
        data_objs.append(Method(method).datapackage())
    if weighting:
        assert weighting in weightings
        data_objs.append(Weighting(weighting).datapackage())
    if normalization:
        assert normalization in normalizations
        data_objs.append(Normalization(normalization).datapackage())

    if demands:
        indexed_demand = [{get_id(k): v for k, v in dct.items()} for dct in demands]
    elif demand:
        indexed_demand = {get_id(k): v for k, v in demand.items()}
    else:
        indexed_demand = None

    return indexed_demand, data_objs, remapping_dicts