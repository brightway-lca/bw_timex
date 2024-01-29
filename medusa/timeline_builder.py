
import bw2data as bd
import warnings
import pandas as pd

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
       
    def extract_edge_data(edge: Edge) -> dict:
        """
        Stores the attributes of an Edge instance in a dictionary.

        :param edge: Edge instance
        :return: Dictionary with attributes of the edge 
        """
        
        return {
            "datetime": edge.distribution.date,
            "amount": edge.distribution.amount, # Do we even need this? 
            "producer": edge.producer,
            "consumer": edge.consumer,
            "leaf": edge.leaf,
            "total": edge.value.total if isinstance(edge.value, TemporalDistribution) else edge.value,
            "share": edge.value.amount/edge.value.total  if isinstance(edge.value, TemporalDistribution) else edge.value,
            # "share": edge.distribution.amount / edge.distribution.total,
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
    print(edges_df)
    # Explode datetime and amount columns
    edges_df = edges_df.explode(['datetime', 'share'])
    
    # Extract different temporal groupings from datetime column: year to hour
    edges_df['year'] = edges_df['datetime'].apply(lambda x: x.year)
    edges_df['year_month'] = edges_df['datetime'].apply(lambda x: x.strftime("%Y-%m"))
    edges_df['year_month_day'] = edges_df['datetime'].apply(lambda x: x.strftime("%Y-%m-%d"))
    edges_df['year_month_day_hour'] = edges_df['datetime'].apply(lambda x: x.strftime("%Y-%m-%dT%H"))
          
    # Group by selected temporal scope & convert temporal grouping to datetime format: 
    # #FIXME: each assignment uses the first timestamp in the respective period, 
    # e.g. for year: 2024-12-31 gets turned into 2024, possibly grouped with other 2024 rows and then reassigned to 2024-01-01
    if temporal_grouping == 'year': 
        grouped_edges = edges_df.groupby(['year', 'producer', 'consumer']).agg({'amount': 'sum', 'total': 'max', 'share': 'sum'}).reset_index()
        grouped_edges['date'] = grouped_edges['year'].apply(lambda x: datetime(x, 1, 1))
    elif temporal_grouping == 'month':
        grouped_edges = edges_df.groupby(['year', 'year_month', 'producer', 'consumer'])['amount'].sum().reset_index() 
        grouped_edges['date'] = grouped_edges['year_month'].apply(lambda x: datetime.strptime(x, '%Y-%m'))
    elif temporal_grouping == 'day': 
        grouped_edges = edges_df.groupby(['year', 'year_month_day', 'producer', 'consumer'])['amount'].sum().reset_index()
        grouped_edges['date'] = grouped_edges['year_month_day'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
    elif temporal_grouping == 'hour': 
        grouped_edges = edges_df.groupby(['year', 'year_month_day_hour', 'producer', 'consumer'])['amount'].sum().reset_index()
        grouped_edges['date'] = grouped_edges['year_month_day_hour'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H"))
    else:
        raise ValueError(f"Sorry, but {temporal_grouping} temporal scope grouping is not available yet.")
    
    
    # Add interpolation weights to the dataframe
    grouped_edges = add_column_interpolation_weights_to_timeline(grouped_edges, database_date_dict, interpolation_type=interpolation_type)
    
    # Retrieve producer and consumer names
    grouped_edges['producer_name'] = grouped_edges.producer.apply(lambda x: bd.get_node(id=x)["name"])
    grouped_edges['consumer_name'] = grouped_edges.consumer.apply(get_consumer_name)
    
    grouped_edges['timestamp'] = grouped_edges['year']  # for now just year but could be calling the function --> extract_date_as_integer(grouped_edges['date'])
    # grouped_edges = grouped_edges[['date', 'year', 'producer', 'producer_name', 'consumer', 'consumer_name', 'amount', 'interpolation_weights']]
     #TODO: remove year, since we now have flexible time grouping and everything is stored in date. Currently still kept for clarity
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
    if "date" not in list(tl_df.columns):
        raise ValueError("The timeline does not contain dates.")
        
    if interpolation_type == "nearest":
        tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: find_closest_date(x, dates_list))
        # change key of interplation weights dictionaries to database name instead of year
        tl_df['interpolation_weights'] = tl_df['interpolation_weights'].apply(lambda d: {database_date_dict[x]: v for x, v in d.items()})  
        return tl_df
    
    if interpolation_type == "linear":
        tl_df['interpolation_weights'] = tl_df['date'].apply(lambda x: get_weights_for_interpolation_between_nearest_years(x, dates_list, interpolation_type))
        # change key of interplation weights dictionaries to database name instead of year
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