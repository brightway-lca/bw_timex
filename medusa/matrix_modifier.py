from typing import Optional
import bw2data as bd
import bw_processing as bwp
import uuid
import numpy as np
import pandas as pd

def create_datapackage_from_edge_timeline(
    timeline: pd.DataFrame, 
    database_date_dict: dict, 
    demand_timing: dict,
    datapackage: Optional[bwp.Datapackage] = None,
    name: Optional[str] = None,
) -> bwp.Datapackage:
    """
    Creates patches from a given timeline. Patches are datapackages that add or overwrite datapoints in the LCA matrices before LCA calculations.
    
    The heavy lifting of this function happens in its inner function "add_row_to_datapackage":
    Here, each node with a temporal distribution is "exploded", which means each occurrence of this node (e.g. steel production on 2020-01-01 
    and steel production 2015-01-01) becomes separate new node with its own unique id. The exchanges on these node-clones get relinked to the activities 
    with the same name, reference product and location as previously, but now from the corresponding databases in time.

    The new node-clones also need a 1 on the diagonal of their technosphere matrix, which symbolizes the production of the new clone-reference product.

    Inputs:
    timeline: list
        A timeline of edges, typically created from EdgeExtracter.create_edge_timeline()
    database_date_dict: dict
        A dict of the available prospective database: their temporal representativeness (key) and their names (value).
    demand_timing: dict
        A dict of the demand ids and the timing they should be linked to. Can be created using create_demand_timing_dict().
    datapackage: Optional[bwp.Datapackage]
        Append to this datapackage, if available. Otherwise create a new datapackage.
    name: Optional[str]
        Name of this datapackage resource.

    Returns:
    bwp.Datapackage
        A list of patches formatted as datapackages.
    """

    def add_row_to_datapackage(row: pd.core.frame, datapackage: bwp.Datapackage,
                               database_date_dict: dict, demand_timing: dict,
                               new_nodes: set, consumer_timestamps: dict) -> None: 
        """
        create a datapackage for the new edges based on a row from the timeline DataFrame.

        :param row: A row from the timeline DataFrame.
        :param database_dates_dict: Dictionary of available prospective database dates and their names.
        :param demand_timing: Dictionary of the demand ids and the dates they should be linked to. Can be created using create_demand_timing_dict().
        :param new_nodes: empty set to which new node ids are added
        :return: None, but adds new edges to the set new_nodes and adds a patch for this new edge to the bwp.Datapackage
    """
        # print('Current row:', row.year, ' | ', row.producer_name, ' | ', row.consumer_name)

        if row.consumer == -1: # ? Why? Might be in the timeline-building code that starts graph traversal at FU and directly goes down the supply chain
            # print('Row contains the functional unit - exploding to new time-specific node')
            new_producer_id = row.producer*1000000+row.hash_producer
            new_nodes.add(new_producer_id)
            # print(f'New producer id = {new_producer_id}')
            # print()
            return
        
        new_consumer_id = row.consumer*1000000+row.hash_consumer
        # print(f'New consumer id = {new_consumer_id}')
        # print(f'New added year= {extract_date_as_integer(row.date)}')
        new_producer_id = row.producer*1000000+row.hash_producer# In case the producer comes from a background database, we overwrite this. It currently still gets added to new_nodes, but this is not necessary.
        new_nodes.add(new_consumer_id)
        new_nodes.add(new_producer_id) 
        previous_producer_id = row.producer
        previous_producer_node = bd.get_node(id=previous_producer_id) # in future versions, insead of getting node, just provide list of producer ids
        
        # Add entry between exploded consumer and exploded producer (not in background database)
        datapackage.add_persistent_vector(
                matrix="technosphere_matrix",
                name=uuid.uuid4().hex,
                data_array=np.array([row.amount], dtype=float), # old: [row.total * row.share]
                indices_array=np.array(
                    [(new_producer_id, new_consumer_id)],
                    dtype=bwp.INDICES_DTYPE,
                ),
                flip_array=np.array([True], dtype=bool),
                ) 

        # Check if previous producer comes from background database
        if previous_producer_node['database'] in database_date_dict.values():
            
            # # create new consumer id if consumer is the functional unit
            # if row.consumer in demand_timing.keys():
            #     new_consumer_id = row.consumer*1000000+row.consumer_datestamp #Why?

            # print('Row contains internal foreground edge - exploding to new time-specific nodes')
            # print(f'New producer id = {new_producer_id}')
            # print(f'New consumer id = {new_consumer_id}')
            # print()
            # datapackage.add_persistent_vector(
            #             matrix="technosphere_matrix",
            #             name=uuid.uuid4().hex,
            #             data_array=np.array([row.amount], dtype=float),
            #             indices_array=np.array(
            #                 [(new_producer_id, new_consumer_id)], #FIXME: I think if orevious producer comes from foreground database, new_producer_id should be assigned back to original producer_id from foreground database.
            #                 dtype=bwp.INDICES_DTYPE,
            #             ),
            #             flip_array=np.array([True], dtype=bool),
            #     )
                               
            # Create new edges based on interpolation_weights from the row
            for database, db_share in row.interpolation_weights.items():             
                
                # Get the producer activity in the corresponding background database
                producer_id_in_background_db = bd.get_node(
                        **{
                            "database": database, 
                            "name": previous_producer_node["name"],
                            "product": previous_producer_node["reference product"], 
                            "location": previous_producer_node["location"], 
                        }
                    ).id  
                # Add entry between exploded producer and producer in background database ("Temporal Market")
                datapackage.add_persistent_vector(
                        matrix="technosphere_matrix",
                        name=uuid.uuid4().hex,
                        data_array=np.array([db_share], dtype=float),
                        indices_array=np.array(
                            [(producer_id_in_background_db, new_producer_id)],
                            dtype=bwp.INDICES_DTYPE,
                        ),
                        flip_array=np.array([True], dtype=bool),
                )
    
    if not name:
        name = uuid.uuid4().hex # we dont use this variable? 
        # logger.info(f"Using random name {name}")

    if datapackage is None:
        datapackage = bwp.create_datapackage(sum_inter_duplicates=False)  # 'sum_inter_duplicates=False': If the same market is used mby multiple foreground processes, the market get's created again, inputs should not be summed. 

    new_nodes = set()
    
    consumer_timestamps = {}  # a dictionary to store the year of the consuming processes so that the inputs from previous times get linked right
    for row in timeline.iloc[::-1].itertuples():
        # if row.consumer not in consumer_timestamps.keys():
        #     consumer_timestamps[row.consumer] = row.date#row.timestamp
        # consumer_timestamps[row.producer] = row.date #row.timestamp  # the year of the producer will be the consumer year for this procuess until a it becomesa producer again
        # print(row.timestamp, row.producer, row.consumer, consumer_timestamps[row.consumer])
        add_row_to_datapackage(row,
                               datapackage,
                               database_date_dict,
                               demand_timing,
                               new_nodes,
                               consumer_timestamps,)
    
    # Adding ones on diagonal for new nodes
    datapackage.add_persistent_vector(
        matrix="technosphere_matrix",
        name=uuid.uuid4().hex,
        data_array=np.ones(len(new_nodes)),
        indices_array=np.array([(i, i) for i in new_nodes], dtype=bwp.INDICES_DTYPE),
    )

    return datapackage