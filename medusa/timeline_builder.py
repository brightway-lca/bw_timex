
import bw2data as bd
import warnings
import pandas as pd
import numpy as np
from typing import Union, Tuple, Optional
from datetime import datetime, timedelta
from typing import KeysView

from .edge_extractor import Edge
from bw_temporalis import TemporalDistribution


def create_grouped_edge_dataframe(tl: list, database_date_dict: dict, temporal_grouping: str = 'year', interpolation_type: str ="linear") -> pd.DataFrame:
    """
    Create a grouped edge dataframe. 
    
    Edges that occur at different times within the same unit of time are grouped together. Th etemporal grouping is currently possible by year and month. Hopefully soon also by day and hour.

    The column "interpolation weights" assigns the ratio [0-1] of the edge's amount to be taken from the database with the closest time of representativeness. 
    Available interpolation types are:
        - "linear": linear interpolation between the two closest databases, based on temporal distance
        - "closest": closest database is assigned 1

    :param tl: Timeline containing edge information.
    :param database_date_dict: Mapping dictionary between the time of representativeness (key) and name of database (value)
    :param temporal_grouping: Type of temporal grouping (default is "year"). Available options are "year", "month", "day" and "hour" (TODO fix day and hour)
    :param interpolation_type: Type of interpolation (default is "linear").
    :return: Grouped edge dataframe.
    """
       
    # def extract_edge_data(edge: Edge) -> dict:
    #     """
    #     Stores the attributes of an Edge instance in a dictionary.

    #     :param edge: Edge instance
    #     :return: Dictionary with attributes of the edge 
    #     """
        
    #     return {
    #         "datetime": edge.distribution.date,
    #         "amount": edge.distribution.amount, # Do we even need this? 
    #         "producer": edge.producer,
    #         "consumer": edge.consumer,
    #         "leaf": edge.leaf,
    #         "total": edge.value.total if isinstance(edge.value, TemporalDistribution) else edge.value,
    #         "share": edge.distribution.amount / edge.distribution.total,
    #         "share2": edge.value.amount / edge.value.total if isinstance(edge.value, TemporalDistribution) else edge.value,
    #         #"td_consumer": edge.td_consumer,
    #         #"td_producer": edge.td_producer,
    #         "td_abs_consumer": edge.td_abs_consumer,
    #         'td_abs_producer': edge.td_abs_producer,
    #     }
        
    def extract_edge_data(edge: Edge) -> dict:
        """
        Stores the attributes of an Edge instance in a dictionary.

        :param edge: Edge instance
        :return: Dictionary with attributes of the edge 
        """
        try:
            consumer_date = edge.abs_td_consumer.date
            consumer_date = np.array([consumer_date for i in range(len(edge.td_producer))]).T.flatten()
            #print(consumer_date)
        except AttributeError: 
            consumer_date = None

        return {
            "producer": edge.producer,
            "consumer": edge.consumer,
            "leaf": edge.leaf,
            "consumer_date": consumer_date,
            'producer_date': edge.abs_td_producer.date,
            "amount": edge.abs_td_producer.amount,
        }
    
    def get_consumer_name(id: int) -> str:
        """
        Returns the name of consumer node. 
        If consuming node is the functional unit, returns -1.

        :param id: Id of node
        :return: string of node's name or -1
        """
        try:
            return bd.get_node(id=id)['name']
        except:
            return '-1' #functional unit
        
    def extract_grouping_date_as_string(temporal_grouping: str, timestamp: datetime):
        """
        Extracts the grouping date as a string from a datetime object, based on the chosen temporal grouping.
        e.g. for temporal grouping = 'year', and timestamp = 2023-03-29T01:00:00, it extracts '2023'.
        """
        time_res_dict = {'year':'%Y','month':'%Y%m','day':'%Y%m%d','hour':'%Y%m%d%M'}
        
        if temporal_grouping not in time_res_dict.keys():
            warnings.warn('temporal_grouping: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                        temporal_grouping, time_res_dict.keys()), category=Warning)
            
        return timestamp.strftime(time_res_dict[temporal_grouping])
   
        
    def convert_grouping_date_string_to_datetime(temporal_grouping, datestring):
        '''
        Converts the string of a date used for grouping back to datetime object.
        e.g. for temporal grouping = 'year', and datestring = '2023', it extracts 2023 (?)
        '''
        time_res_dict = {'year':'%Y','month':'%Y%m','day':'%Y%m%d','hour':'%Y%m%d%M'}
        
        if temporal_grouping not in time_res_dict.keys():
            warnings.warn('temporal grouping: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                        temporal_grouping, time_res_dict.keys()), category=Warning)
            
        return(datetime.strptime(datestring, time_res_dict[temporal_grouping]))
            


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
        #print(dt_obj)
        formatted_date = dt_obj.strftime(time_res_dict[time_res]) 
        date_as_integer = int(formatted_date)
        
        return date_as_integer 
    
        
    # check if database names match with databases in BW project
    check_database_names(database_date_dict)
        
    # Check if temporal_grouping is a valid value
    valid_temporal_groupings = ['year', 'month', 'day', 'hour']
    if temporal_grouping not in valid_temporal_groupings:
        raise ValueError(f"Invalid value for 'temporal_grouping'. Allowed values are {valid_temporal_groupings}.")
    
    # warning about day and hour not working yet
    if temporal_grouping in ['day', 'hour']:
        raise ValueError(f"Sorry, but temporal grouping is not yet available for 'day' and 'hour'.")
    
    # Extract edge data into a list of dictionaries
    edges_data = [extract_edge_data(edge) for edge in tl]
    
    # Convert list of dictionaries to dataframe
    edges_df = pd.DataFrame(edges_data)
   
    # Explode datetime and amount columns: each row with multiple dates and amounts is exploded into multiple rows with one date and one amount
    edges_df = edges_df.explode(['consumer_date', 'producer_date', 'amount'])
    
    # For the Functional Unit: set consumer date = producer date as it occurs at the same time
    edges_df.loc[edges_df['consumer']== -1, 'consumer_date'] = edges_df.loc[edges_df['consumer']== -1, 'producer_date']
    
    # extract grouping time of consumer and producer
    edges_df['consumer_grouping_time'] = edges_df['consumer_date'].apply(lambda x: extract_grouping_date_as_string(temporal_grouping, x))
    edges_df['producer_grouping_time'] = edges_df['producer_date'].apply(lambda x: extract_grouping_date_as_string(temporal_grouping, x))
    
    #group by unique pair of consumer and producer within their grouping times
    grouped_edges = edges_df.groupby(['producer_grouping_time', 'consumer_grouping_time', 'producer', 'consumer']).agg({'amount': 'sum'}).reset_index()
    
    #date is not really used
    grouped_edges['date_producer'] = grouped_edges['producer_grouping_time'].apply(lambda x: convert_grouping_date_string_to_datetime(temporal_grouping, x)) # date is date producer, but in long format
    grouped_edges['hash_producer'] =  grouped_edges['date_producer'].apply(lambda x: extract_date_as_integer(x, time_res=temporal_grouping)) #grouped_edges['year']  # for now just year but could be calling the function --> extract_date_as_integer(grouped_edges['date'])       
    
    grouped_edges['date_consumer'] = grouped_edges['consumer_grouping_time'].apply(lambda x: convert_grouping_date_string_to_datetime(temporal_grouping, x)) # date is date producer, but in long format
    grouped_edges['hash_consumer'] =  grouped_edges['date_consumer'].apply(lambda x: extract_date_as_integer(x, time_res=temporal_grouping)) #grouped_edges['year']  # for now just year but could be calling the function --> extract_date_as_integer(grouped_edges['date'])       
              
    # Add interpolation weights to the dataframe
    grouped_edges = add_column_interpolation_weights_to_timeline(grouped_edges, database_date_dict, interpolation_type=interpolation_type)
    
    # Retrieve producer and consumer names
    grouped_edges['producer_name'] = grouped_edges.producer.apply(lambda x: bd.get_node(id=x)["name"])
    grouped_edges['consumer_name'] = grouped_edges.consumer.apply(get_consumer_name) 
    
    # Reorder columns
    grouped_edges = grouped_edges[['hash_producer', 'date_producer', 'producer', 'producer_name', 'hash_consumer', 'date_consumer',  'consumer', 'consumer_name', 'amount', 'interpolation_weights']]

    return grouped_edges

def add_column_interpolation_weights_to_timeline(tl_df: pd.DataFrame, database_date_dict: dict, interpolation_type: str ="linear") -> pd.DataFrame:
    """
    Add a column to a timeline with the weights for an interpolation between the two nearest dates, from the list of dates from the available databases.

    :param tl_df: Timeline as a dataframe.
    :param database_date_dict: Mapping dictionary between the time of representativeness (key) and name of database (value)
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
    
    def find_closest_date(target: datetime, dates: KeysView[datetime]) -> dict:
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
    
        return {closest: 1}
    
    def get_weights_for_interpolation_between_nearest_years(reference_date: datetime, dates_list: KeysView[datetime], interpolation_type: str = "linear") -> dict:
        """
        Find the nearest dates (before and after) a given date in a list of dates and calculate the interpolation weights.
    
        :param reference_date: Target date.
        :param dates_list: KeysView[datetime], which is a list of temporal coverage of the available databases,.
        :param interpolation_type: Type of interpolation between the nearest lower and higher dates. For now, 
        only "linear" is available.
        
        :return: Dictionary with temporal coverage of the available databases to use as keys and the weights for interpolation as values.
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
        dates_list = sorted(dates_list)
        
        diff_dates_list = [reference_date - x for x in dates_list]
        if timedelta(0) in diff_dates_list:
            exact_match = dates_list[diff_dates_list.index(timedelta(0))]
            return {exact_match: 1}
        

        closest_lower = None
        closest_higher = None

        for date in dates_list:
            if date < reference_date:
                if closest_lower is None or reference_date - date < reference_date - closest_lower:
                    closest_lower = date
            elif date > reference_date:
                if closest_higher is None or date - reference_date < closest_higher - reference_date:
                    closest_higher = date
        
        if closest_lower is None:
            print(f"Warning: Reference date {reference_date} is lower than all provided dates. Data will be taken from closest higher year.")
            return {closest_higher: 1}
        
        if closest_higher is None:
            print(f"Warning: Reference date {reference_date} is higher than all provided dates. Data will be taken from the closest lower year.")
            return {closest_lower: 1}
  
        if closest_lower == closest_higher:
            warnings.warn("Date outside the range of dates covered by the databases.", category=Warning)
            return {closest_lower: 1}
            
        if interpolation_type == "linear":
            weight = int((reference_date - closest_lower).total_seconds())/int((closest_higher - closest_lower).total_seconds())
        else:
            raise ValueError(f"Sorry, but {interpolation_type} interpolation is not available yet.")
        return {closest_lower: 1-weight, closest_higher: weight}
    
    dates_list= database_date_dict.keys()
    if "date_producer" not in list(tl_df.columns):
        raise ValueError("The timeline does not contain dates.")
        
    if interpolation_type == "nearest":
        tl_df['interpolation_weights'] = tl_df['date_producer'].apply(lambda x: find_closest_date(x, dates_list))
        tl_df['interpolation_weights'] = tl_df['interpolation_weights'].apply(lambda d: {database_date_dict[x]: v for x, v in d.items()})  
        return tl_df
    
    if interpolation_type == "linear":
        tl_df['interpolation_weights'] = tl_df['date_producer'].apply(lambda x: get_weights_for_interpolation_between_nearest_years(x, dates_list, interpolation_type))
        tl_df['interpolation_weights'] = tl_df['interpolation_weights'].apply(lambda d: {database_date_dict[x]: v for x, v in d.items()})
        
    else:
        raise ValueError(f"Sorry, but {interpolation_type} interpolation is not available yet.")
        
    return tl_df


def check_database_names(database_date_dict):
    """
    Check that the strings of the databases (values of database_date_dict) exist in the databases of the brightway2 project
   
    """
    for db in database_date_dict.values():
        assert db in bd.databases, f"{db} not in your brightway2 project databases."
    else:
        print("All databases in database_date_dict exist as brightway project databases")
    return