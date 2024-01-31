import bw2data as bd
import bw_processing as bwp
import uuid
import numpy as np
import pandas as pd
from typing import Optional
from datetime import datetime



class MatrixModifier:
    def __init__(
        self,
        timeline: pd.DataFrame,
        database_date_dict: dict,
        demand_timing: dict,
        datapackage: Optional[bwp.Datapackage] = None,
        name: Optional[str] = None,
    ):
        self.timeline = timeline
        self.database_date_dict = database_date_dict
        self.demand_timing = demand_timing
        self.datapackage = datapackage
        self.name = name

    def create_technosphere_datapackage(self) -> bwp.Datapackage:
        """
        Creates patches from a given timeline. Patches are datapackages that add or overwrite datapoints in the LCA matrices before LCA calculations.

        The heavy lifting of this function happens in its inner function "add_row_to_datapackage":
        Here, each node with a temporal distribution is "exploded", which means each occurrence of this node (e.g. steel production on 2020-01-01
        and steel production 2015-01-01) becomes separate new node with its own unique id. The exchanges on these node-clones get relinked to the activities
        with the same name, reference product and location as previously, but now from the corresponding databases in time.

        The new node-clones also need a 1 on the diagonal of their technosphere matrix, which symbolizes the production of the new clone-reference product.

        :param timeline: A timeline of edges, typically created from EdgeExtracter.create_edge_timeline()
        :param database_date_dict: A dict of the available prospective database: their temporal representativeness (key) and their names (value).
        :param demand_timing: A dict of the demand ids and the timing they should be linked to. Can be created using create_demand_timing_dict().
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
            Adds a new row to the given datapackage based on the provided row from the timeline DataFrame.

            This function also updates the set of new nodes with the ids of any new nodes created during this process.

            :param row: A row from the timeline DataFrame.
            :param datapackage: The datapackage to which the new patches will be added.
            :param database_date_dict: Dictionary of available prospective database dates and their names.
            :param demand_timing: Dictionary of the demand ids and the dates they should be linked to. Can be created using create_demand_timing_dict().
            :param new_nodes: Set to which new node ids are added.
            :return: None, but updates the set new_nodes and adds a patch for this new edge to the bwp.Datapackage.
            """
            if row.consumer == -1:
                new_producer_id = row.producer * 1000000 + row.hash_producer
                new_nodes.add(new_producer_id)
                return

            new_consumer_id = row.consumer * 1000000 + row.hash_consumer
            new_nodes.add(new_consumer_id)

            new_producer_id = row.producer * 1000000 + row.hash_producer
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
            if (
                previous_producer_node["database"] in database_date_dict.keys()
            ):
                # Create new edges based on interpolation_weights from the row
                for database, db_share in row.interpolation_weights.items():
                    # Get the producer activity in the corresponding background database
                    print(database, previous_producer_node["name"], previous_producer_node["reference product"], previous_producer_node["location"])
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
        )  # 'sum_inter_duplicates=False': If the same market is used mby multiple foreground processes, the market get's created again, inputs should not be summed.

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
        Creates a new biosphere datapackage to add the biosphere flows to the exploded technosphere processes.

        This function iterates over each unique producer and for each biosphere exchange of the original activity,
        it creates a new biosphere exchange for the new node.

        :param timeline: A DataFrame representing the timeline of edges.
        :param database_date_dict: Dictionary of available prospective database dates and their names.
        :return: The updated datapackage with new biosphere exchanges for the new nodes.
        """
        unique_producers = (
            self.timeline.groupby(["producer", "hash_producer"]).count().index.values
        )  # array of unique (producer, timestamp) tuples

        datapackage_bio = bwp.create_datapackage(sum_inter_duplicates=False)
        for producer in unique_producers:
            # Skip the -1 producer as this is just a dummy producer of the functional unit
            if (
                not producer[0] == -1
                and not bd.get_activity(producer[0])["database"]
                in self.database_date_dict.values()
            ):
                producer_id = (
                    producer[0] * 1000000 + producer[1]
                )  # the producer_id is a combination of the activity_id and the timestamp
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
        technosphere_datapackage = self.create_technosphere_datapackage()
        biosphere_datapackge = self.create_biosphere_datapackage()
        return [technosphere_datapackage, biosphere_datapackge]
