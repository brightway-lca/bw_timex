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

# for prepare_medusa_lca_inputs
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

def create_demand_timing_dict(timeline, demand):
    """
    Generate a dictionary mapping producer to year for specific demands using the timeline.
    
    :param timeline: Timeline DataFrame, generated by `create_grouped_edge_dataframe`.
    :param demand: Demand dict
    
    :return: Dictionary mapping producer ids to years for the specified demands.
    """
    demand_names = [bd.get_activity(key).id for key in demand.keys()]
    demand_rows = timeline[timeline['producer'].isin(demand_names) & (timeline['consumer'] == -1)]
    return {row.producer: row.year for row in demand_rows.itertuples()}  

def create_grouped_edge_dataframe(tl, dates_list, interpolation_type="linear"):
    """
    Create a grouped edge dataframe.

    :param tl: Timeline containing edge information.
    :param dates_list: List of dates to be used for interpolation.
    :param interpolation_type: Type of interpolation (default is "linear").
    :return: Grouped edge dataframe.
    """
    
    def extract_edge_data(edge):
        return {
            "datetime": edge.distribution.date,
            "amount": edge.distribution.amount,
            "producer": edge.producer,
            "consumer": edge.consumer,
            "leaf": edge.leaf
        }
    
    def get_consumer_name(id):
        try:
            return bd.get_node(id=id)['name']
        except:
            return '-1'

    # Extract edge data into a list of dictionaries
    edges_data = [extract_edge_data(edge) for edge in tl]
    
    # Convert list of dictionaries to dataframe
    edges_df = pd.DataFrame(edges_data)
    
    # Explode datetime and amount columns
    edges_df = edges_df.explode(['datetime', 'amount'])
    
    # Extract year from datetime column
    edges_df['year'] = edges_df['datetime'].apply(lambda x: x.year)
    
    # Group by year, producer, and consumer and sum amounts
    edge_df = edges_df.groupby(['year', 'producer', 'consumer'])['amount'].sum().reset_index()
    
    # Convert year back to datetime format
    edge_df['date'] = edge_df['year'].apply(lambda x: datetime(x, 1, 1))
    
    # Add interpolation weights to the dataframe
    timeline_with_interpolation = add_column_interpolation_weights_to_timeline(edge_df, dates_list, interpolation_type=interpolation_type)
    
    # Retrieve producer and consumer names
    timeline_with_interpolation['producer_name'] = timeline_with_interpolation.producer.apply(lambda x: bd.get_node(id=x)["name"])
    timeline_with_interpolation['consumer_name'] = timeline_with_interpolation.consumer.apply(get_consumer_name)
    
    return edge_df

def create_datapackage_from_edge_timeline(
    timeline: pd.DataFrame, 
    database_date_dict: dict, 
    demand_timing: dict,
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
    demand_timing: dict
        A dict of the demand ids and the years they should be linked to. Can be created using create_demand_timing_dict().
    datapackage: Optional[bwp.Datapackage]
        Append to this datapackage, if available. Otherwise create a new datapackage.
    name: Optional[str]
        Name of this datapackage resource.

    Returns:
    bwp.Datapackage
        A list of patches formatted as datapackages.
    """

    def add_row_to_datapackage(row, datapackage, database_date_dict, demand_timing, new_nodes): 
        """
        Extracts new edges based on a row from the timeline DataFrame.

        :param row: A row from the timeline DataFrame.
        :param database_dates_dict: Dictionary of available prospective database dates and their names.
        :param demand_timing: Dictionary of the demand ids and the years they should be linked to. Can be created using create_demand_timing_dict().
        :return: List of new edges; each edge contains the consumer, the previous producer, the new producer, and the amount.
        """
        print('Current row:', row.year, ' | ', row.producer_name, ' | ', row.consumer_name)

        if row.consumer == -1: # ? Why? Might be in the timeline-building code that starts graph traversal at FU and directly goes down the supply chain
            print('Row contains the functional unit - exploding to new time-specific node')
            new_producer_id = row.producer*1000000+row.year
            new_nodes.add(new_producer_id)
            print(f'New producer id = {new_producer_id}')
            print()
            return
        
        new_consumer_id = row.consumer*1000000+row.year
        new_producer_id = row.producer*1000000+row.year # In case the producer comes from a background database, we overwrite this. It currently still gets added to new_nodes, but this is not necessary.
        new_nodes.add(new_consumer_id)
        new_nodes.add(new_producer_id) 
        previous_producer_id = row.producer
        previous_producer_node = bd.get_node(id=previous_producer_id) # in future versions, insead of getting node, just provide list of producer ids
        
        # Check if previous producer comes from foreground database
        if not previous_producer_node['database'] in database_date_dict.values():
            
            # dont create new consumer id if consumer is the functional unit
            if row.consumer in demand_timing.keys():
                new_consumer_id = row.consumer*1000000+demand_timing[row.consumer]

            print('Row contains internal foreground edge - exploding to new time-specific nodes')
            print(f'New producer id = {new_producer_id}')
            print(f'New consumer id = {new_consumer_id}')
            print()
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
        
        else:   # Previous producer comes from background database
            print('Row links to background database')

            # dont create new consumer id if consumer is the functional unit
            if row.consumer in demand_timing.keys():
                new_consumer_id = row.consumer*1000000+demand_timing[row.consumer]

            # Create new edges based on interpolation_weights from the row
            for date, share in row.interpolation_weights.items():
                print(f'New link goes to {database_date_dict[datetime.strptime(str(date), "%Y")]} for year {date}')
                new_producer_id = bd.get_node(
                        **{
                            "database": database_date_dict[datetime.strptime(str(date), "%Y")],
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
        add_row_to_datapackage(row, datapackage, database_date_dict, demand_timing, new_nodes)
    
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
        return {closest_lower.year: 1-weight, closest_higher.year: weight}
        if "date" not in list(tl_df.columns):
            raise ValueError("The timeline does not contain dates.")
        
    if interpolation_type == "nearest":
        tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: find_closest_date(x, dates_list))
        return tl_df
        
    tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: get_weights_for_interpolation_between_nearest_years(x, dates_list, interpolation_type))
    return tl_df

def unpack(dct):
    for obj in dct:
        if hasattr(obj, "key"):
            yield obj.key
        else:
            yield obj

def prepare_medusa_lca_inputs(
    demand=None,
    method=None,
    demand_timing_dict=None,
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
    # demand_database_names = ['background_2023', 'background_2020'] # Always load all databases
    

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
        indexed_demand = [{get_id(k)*1000000+demand_timing_dict[get_id(k)]: v for k, v in dct.items()} for dct in demands]
    elif demand:
        indexed_demand = {get_id(k)*1000000+demand_timing_dict[get_id(k)]: v for k, v in demand.items()}
    else:
        indexed_demand = None

    return indexed_demand, data_objs, remapping_dicts