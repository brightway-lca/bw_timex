import uuid
from typing import Optional

import bw2data as bd
import bw_processing as bwp
import numpy as np
import pandas as pd


class MatrixModifier:
    """
    Class for adding and re-linking time-explicit processes in the LCA matrices.

    This is done by creating datapackages that add or change matrix entries in the technosphere and
    biosphere matrices, based on a process timeline (from TimelineBuilder.build_timeline()).
    "Temporal markets" are created for processes that are linked to background databases, temporally
    distributing the amounts to time-explicit background databases.
    """

    def __init__(
        self,
        timeline: pd.DataFrame,
        database_date_dict_static_only: dict,
        demand_timing: dict,
        name: Optional[str] = None,
    ) -> None:
        """
        Initializes the MatrixModifier object and creates empty sets to collect the ids of temporalized processes and temporal markets.

        Parameters
        ----------
        timeline : pd.DataFrame
            A DataFrame of the timeline of exchanges
        database_date_dict_static_only : dict
            A dictionary mapping the static background databases to dates.
        demand_timing : dict
            A dictionary mapping the demand to its timing.
        name : str, optional
            An optional name for the MatrixModifier instance. Default is None.

        """

        self.timeline = timeline
        self.database_date_dict_static_only = database_date_dict_static_only
        self.demand_timing = demand_timing
        self.name = name
        self.temporalized_process_ids = set()
        self.temporal_market_ids = set()

    def create_datapackage(self) -> None:
        """
        Creates a list of datapackages for the technosphere and biosphere matrices, by calling the respective functions.

        Parameters
        ----------
        None

        Returns
        -------
        list
            A list of the technosphere and biosphere datapackages.
        """
        technosphere_datapackage = self.create_technosphere_datapackage()
        biosphere_datapackage = self.create_biosphere_datapackage()

        return [technosphere_datapackage, biosphere_datapackage]

    def create_technosphere_datapackage(self) -> bwp.Datapackage:
        """
        Creates the modifications to the technosphere matrix in form of a datapackage. Datapackages add or overwrite datapoints in the LCA matrices before LCA calculations.
        The technosphere datapackage adds the temporalized processes from the timeline to the technosphere matrix.

        The heavy lifting of this method happens in the method `add_row_to_datapackage()`.
        Here, each node with a temporal distribution is "exploded", which means each occurrence of this node (e.g. steel production on 2020-01-01
        and steel production on 2015-01-01) becomes a separate, time-explicit new node, by adding the new elements to the technosphere matrix.
        For processes at the intersection with background databases, the timing of the exchanges determines which background database to link to in so called "Temporal Markets".

        Parameters
        ----------
        None

        Returns
        -------
        bwp.Datapackage
            A datapackage containing the modifications for the technosphere matrix.
        """
        datapackage = bwp.create_datapackage(
            sum_inter_duplicates=False
        )  # 'sum_inter_duplicates=False': If the same market is used by multiple foreground processes, the market gets created again, inputs should not be summed.

        new_nodes = set()

        for row in self.timeline.iloc[::-1].itertuples():
            self.add_row_to_datapackage(
                row,
                datapackage,
                new_nodes,
            )

        # Adding the production exchanges for new nodes
        for node_id, production_amount in new_nodes:
            datapackage.add_persistent_vector(
                matrix="technosphere_matrix",
                name=uuid.uuid4().hex,
                data_array=np.array([production_amount], dtype=float),
                indices_array=np.array([(node_id, node_id)], dtype=bwp.INDICES_DTYPE),
            )

        return datapackage

    def create_biosphere_datapackage(self) -> bwp.Datapackage:
        """
        Creates the modifications to the biosphere matrix in form of a datapackage. Datapackages add or overwrite datapoints in the LCA matrices before LCA calculations.
        It adds the biosphere flows to the exploded technosphere processes.

        This function iterates over each unique producer, and for each biosphere exchange of the original activity,
        it creates a new biosphere exchange for the new "temporalized" node.

        Temporal markets have no biosphere exchanges, as they only divide the amount of a technosphere exchange between the different databases.

        Parameters
        ----------
        None

        Returns
        -------
        bwp.Datapackage
            A datapackage containing the modifications for the biosphere matrix.
        """
        unique_producers = (
            self.timeline.groupby(["producer", "time_mapped_producer"])
            .count()
            .index.values
        )  # array of unique ((original) producer_id, (new) time_mapped_producer_id) tuples

        datapackage_bio = bwp.create_datapackage(sum_inter_duplicates=False)

        for producer in unique_producers:
            if (
                bd.get_activity(producer[0])["database"]
                not in self.database_date_dict_static_only.keys()  # skip temporal markets
            ):
                new_producer_id = producer[1]
                # id of time-mapped process
                orig_producer_node = bd.get_node(id=producer[0])
                indices = (
                    []
                )  # list of (biosphere, technosphere) indices for the biosphere flow exchanges
                amounts = []  # list of amounts corresponding to the bioflows
                for exc in orig_producer_node.biosphere():
                    indices.append(
                        (exc.input.id, new_producer_id)
                    )  # directly build a list of tuples to pass into the datapackage, the new_producer_id is the new column index
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

    def add_row_to_datapackage(
        self,
        row: pd.core.frame,
        datapackage: bwp.Datapackage,
        new_nodes: set,
    ) -> None:
        """
        This adds the modifications to the technosphere matrix for each time-dependent exchange as datapackage elements to a given `bwp.Datapackage`.
        Modifications include:
        1) Exploded processes: new matrix elements for time-explicit consumer and time-explicit producer, representing the temporal edge between them.
        2) Temporal markets: new matrix entries for "temporal markets" and links to the producers in temporally matching background databases. Processes in the background databases are matched on name, reference product and location.
        3) Diagonal entries: ones on the diagonal for new nodes.

        This function also collects the ids of new nodes, temporalized nodes and temporal markets.

        Parameters
        ----------
        row : pd.core.frame
            A row of the timeline DataFrame representing an temporalized edge
        datapackage : bwp.Datapackage
            Append to this datapackage, if available. Otherwise create a new datapackage.
        new_nodes : set
            Set of tuples (node_id, production_amount) to which new node ids are added.

        Returns
        -------
        None but adds elements for this edge to the bwp.Datapackage and stores the ids of new nodes, temporalized nodes and temporal markets.
        """

        if row.consumer == -1:  # functional unit
            new_producer_id = row.time_mapped_producer
            fu_production_amount = bd.get_node(id=row.producer).rp_exchange().amount
            new_nodes.add((new_producer_id, fu_production_amount))
            self.temporalized_process_ids.add(
                new_producer_id
            )  # comes from foreground, so it is a temporalized process
            return

        new_consumer_id = row.time_mapped_consumer
        new_producer_id = row.time_mapped_producer

        previous_producer_id = row.producer
        previous_producer_node = bd.get_node(
            id=previous_producer_id
        )  # in future versions, insead of getting node, just provide list of producer ids

        # Add entry between exploded consumer and exploded producer (not in background database)
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

        # Check if previous producer comes from background database -> temporal market
        if (
            previous_producer_node["database"]
            in self.database_date_dict_static_only.keys()
        ):
            # Create new edges based on interpolation_weights from the row
            for database, db_share in row.interpolation_weights.items():
                # Get the producer activity in the corresponding background database
                try:
                    producer_id_in_background_db = bd.get_node(
                        **{
                            "database": database,
                            "name": previous_producer_node["name"],
                            "product": previous_producer_node["reference product"],
                            "location": previous_producer_node["location"],
                        }
                    ).id
                except:
                    print(
                        f"Could not find producer in database {database} with name {previous_producer_node['name']}, product {previous_producer_node['reference product']}, location {previous_producer_node['location']}"
                    )
                    raise SystemExit

                # Add entry between exploded producer and producer in background database ("Temporal Market")
                datapackage.add_persistent_vector(
                    matrix="technosphere_matrix",
                    name=uuid.uuid4().hex,
                    data_array=np.array(
                        [db_share], dtype=float
                    ),  # temporal markets produce 1, so shares divide amount between dbs
                    indices_array=np.array(
                        [(producer_id_in_background_db, new_producer_id)],
                        dtype=bwp.INDICES_DTYPE,
                    ),
                    flip_array=np.array([True], dtype=bool),
                )
                self.temporal_market_ids.add(new_producer_id)
                producer_production_amount = (
                    1  # Shares sum up to 1, so production amount is 1
                )

        else:  # comes from foreground, so it is a temporalized process
            self.temporalized_process_ids.add(new_producer_id)

            # Get the production amount of the previous producer if it's not a temporal market - needed for diagonal matrix entry
            if isinstance(
                previous_producer_node, bd.backends.iotable.proxies.IOTableActivity
            ):
                if len(previous_producer_node.production()) == 1:
                    producer_production_amount = list(
                        previous_producer_node.production()
                    )[0].amount
                else:
                    raise ValueError(
                        "The producer activity is of type IOTableActivity, but has more than one production exchange. This is currently not supported."
                    )
            elif isinstance(previous_producer_node, bd.backends.proxies.Activity):
                producer_production_amount = (
                    bd.get_node(id=row.producer).rp_exchange().amount
                )
            else:
                raise ValueError(
                    f"Can't determine the production amount of the producer activity {previous_producer_node['name']} , as it's of an unknown type."
                )

        # Add newly created producing process to new_nodes
        new_nodes.add((new_producer_id, producer_production_amount))
