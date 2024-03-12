import bw2data as bd
import bw_processing as bwp
import uuid
import numpy as np
import pandas as pd
from typing import Optional
from datetime import datetime


class MatrixModifier:
    """
    This class is responsible for creating a datapackage that contains matrix entries for the temporally "exploded" processes, based on a timeline dataframe (created from TimelineBuilder.build_timeline()).

    :param timeline: A DataFrame representing the timeline.
    :param database_date_dict: A dictionary mapping databases to dates.
    :param demand_timing: A dictionary representing the demand timing.
    :param name: An optional name for the MatrixModifier instance. Default is None.
    """
    def __init__(
        self,
        timeline: pd.DataFrame,
        database_date_dict: dict,
        demand_timing: dict,
        name: Optional[str] = None,
    ):
        self.timeline = timeline
        self.database_date_dict = database_date_dict
        self.demand_timing = demand_timing
        self.name = name

    def create_technosphere_datapackage(self) -> bwp.Datapackage:
        """
        Creates patches to the technosphere matrix from a given timeline of grouped exchanges to add these temporal processes to the technopshere database.
        Patches are datapackages that add or overwrite datapoints in the LCA matrices before LCA calculations.

        The heavy lifting of this function happens in its inner function "add_row_to_datapackage":
        Here, each node with a temporal distribution is "exploded", which means each occurrence of this node (e.g. steel production on 2020-01-01
        and steel production 2015-01-01) becomes a separate new node, by adding the respective elements to the technosphere matrix. 
        For processes at the interface with background databases, the timing of the exchanges determines which background database to link to in so called "Temporal Market".
         
    
        :param timeline: A timeline of edges, typically created from EdgeExtracter.create_edge_timeline()
        :param database_date_dict: A dict of the available prospective databases: their names (key) and temporal representativeness (value).
        :param demand_timing: A dict of the demand ids and the timing occur. Can be created using create_demand_timing_dict().
        :param datapackage: Append to this datapackage, if available. Otherwise create a new datapackage.
        :param name: Name of this datapackage resource.
        :return: A list of patches formatted as datapackages.
        """

        def add_row_to_datapackage(
            row: pd.core.frame,
            datapackage: bwp.Datapackage,
            database_date_dict: dict,
            demand_timing: dict,
            new_nodes: set,
        ) -> None:
            """
            This adds the required technosphere matrix modifications for each time-dependent exchange (edge) as datapackage elements to a given bwp.Datapackage.
            Modifications include: 
            1) Exploded processes: new matrix elements between exploded consumer and exploded producer, representing the temporal edge between them. 
            2) Temporal markets: new matrix entries between "temporal markets" and the producer in temporally matching background database, with shares based on interpolation. 
               Processes in the background databases are matched on name, reference product and location.
            3) Diagonal entries: ones on the diagonal for new nodes.
              
            This function also updates the set of new nodes with the ids of any new nodes created during this process.

            :param row: A row from the timeline DataFrame.
            :param datapackage: The datapackage to which the new patches will be added.
            :param database_date_dict: A dict of the available prospective databases: their names (key) and temporal representativeness (value).
            :param demand_timing: Dictionary of the demand ids and the dates they should be linked to. Can be created using create_demand_timing_dict().
            :param new_nodes: Set to which new node ids are added.
            :return: None, but updates the set new_nodes and adds a patch for this new edge to the bwp.Datapackage.
            """
            if row.consumer == -1:
                new_producer_id = row.time_mapped_producer
                new_nodes.add(new_producer_id)
                return

            new_consumer_id = row.time_mapped_consumer
            new_nodes.add(new_consumer_id)

            new_producer_id = row.time_mapped_producer
            new_nodes.add(new_producer_id)

            previous_producer_id = row.producer
            previous_producer_node = bd.get_node(
                id=previous_producer_id
            )  # in future versions, insead of getting node, just provide list of producer ids

            # Add entry between exploded consumer and exploded producer (not in background database)
            datapackage.add_persistent_vector(
                matrix="technosphere_matrix",
                name=uuid.uuid4().hex,
                data_array=np.array(
                    [row.amount], dtype=float
                ),  # old: [row.total * row.share]
                indices_array=np.array(
                    [(new_producer_id, new_consumer_id)],
                    dtype=bwp.INDICES_DTYPE,
                ),
                flip_array=np.array([True], dtype=bool),
            )

            # Check if previous producer comes from background database
            if previous_producer_node["database"] in self.database_date_dict.keys():
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

        datapackage = bwp.create_datapackage(
            sum_inter_duplicates=False
        )  # 'sum_inter_duplicates=False': If the same market is used by multiple foreground processes, the market gets created again, inputs should not be summed.

        new_nodes = set()

        for row in self.timeline.iloc[::-1].itertuples():
            add_row_to_datapackage(
                row,
                datapackage,
                self.database_date_dict,
                self.demand_timing,
                new_nodes,
            )

        # Adding ones on diagonal for new nodes
        datapackage.add_persistent_vector(
            matrix="technosphere_matrix",
            name=uuid.uuid4().hex,
            data_array=np.ones(len(new_nodes)),
            indices_array=np.array(
                [(i, i) for i in new_nodes], dtype=bwp.INDICES_DTYPE
            ),
        )

        return datapackage

    def create_biosphere_datapackage(self) -> bwp.Datapackage:
        """
        Creates list of patches formatted as datapackages for modifications to the biosphere matrix.
        It adds the biosphere flows to the exploded technosphere processes.

        This function iterates over each unique producer and for each biosphere exchange of the original activity,
        it creates a new biosphere exchange for the new node.

        :param timeline: A DataFrame representing the timeline of edges.
        :param database_date_dict:A dict of the available prospective databases: their names (key) and temporal representativeness (value).
        :return: The updated datapackage with new biosphere exchanges for the new nodes.
        """
        unique_producers = (
            self.timeline.groupby(["producer", "time_mapped_producer"])
            .count()
            .index.values
        )  # array of unique (producer, timestamp) tuples

        datapackage_bio = bwp.create_datapackage(sum_inter_duplicates=False)
        
        for producer in unique_producers:
            if (
                bd.get_activity(producer[0])["database"] not in self.database_date_dict.keys() # skip temporal markets
            ):
                producer_id = producer[1]
                # the producer_id is a combination of the activity_id and the timestamp
                producer_node = bd.get_node(id=producer[0])
                indices = (
                    []
                )  # list of (biosphere, technosphere) indices for the biosphere flow exchanges
                amounts = []  # list of amounts corresponding to the bioflows
                for exc in producer_node.biosphere():
                    indices.append(
                        (exc.input.id, producer_id)
                    )  # directly build a list of tuples to pass into the datapackage, the producer_id is used to for the column of that activity
                    amounts.append(exc.amount)
                    
                datapackage_bio.add_persistent_vector(
                    matrix="biosphere_matrix",
                    name=uuid.uuid4().hex,
                    data_array=np.array(amounts, dtype=float),
                    indices_array=np.array(
                        indices,
                        dtype=bwp.INDICES_DTYPE,
                    ),
                    flip_array=np.array([False], dtype=bool),
                )
        return datapackage_bio

    def create_datapackage(self) -> None:
        """
        Creates a list of datapackages for the technosphere and biosphere matrices.
        """
        technosphere_datapackage = self.create_technosphere_datapackage()
        biosphere_datapackge = self.create_biosphere_datapackage()
        return [technosphere_datapackage, biosphere_datapackge]
