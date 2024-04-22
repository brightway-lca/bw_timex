from scipy import sparse as sp
import pandas as pd
import numpy as np
import bw2data as bd
from bw_temporalis import TemporalDistribution
from .remapping import TimeMappingDict
from bw2calc import LCA
from datetime import datetime


class DynamicBiosphereBuilder:
    """
    This class is used to build a dynamic biosphere matrix, which in contrast to the normal biosphere matrix has rows for each biosphere flow at their time of emission
    Thus, the dimensions are (bio_flows at a specific timestep) x (processes).
    """

    def __init__(
        self,
        lca_obj: LCA,
        activity_time_mapping_dict: dict,
        biosphere_time_mapping_dict: dict,
        demand_timing_dict: dict,
        node_id_collection_dict: dict,
        temporal_grouping: str,
        database_date_dict: dict,
        database_date_dict_static_only: dict,
        len_technosphere_dbs: int,
    ):
        self._time_res_dict = {
            "year": "datetime64[Y]",
            "month": "datetime64[M]",
            "day": "datetime64[D]",
            "hour": "datetime64[h]",
        }

        self.lca_obj = lca_obj
        self.technosphere_matrix = lca_obj.technosphere_matrix
        self.activity_dict = lca_obj.dicts.activity
        self.activity_time_mapping_dict = activity_time_mapping_dict
        self.biosphere_time_mapping_dict = biosphere_time_mapping_dict
        self.demand_timing_dict = demand_timing_dict
        self.node_id_collection_dict = node_id_collection_dict
        self.time_res = self._time_res_dict[temporal_grouping]
        self.database_date_dict = database_date_dict
        self.database_date_dict_static_only = database_date_dict_static_only
        self.len_technosphere_dbs = len_technosphere_dbs
        self.dynamic_supply_array = lca_obj.supply_array
        self.rows = []
        self.cols = []
        self.values = []

    def build_dynamic_biosphere_matrix(self):
        """
        This function creates a separate biosphere matrix, with the dimenions (bio_flows at a specific timestep) x (processes).
        Thus, every temporally resolved biosphere flow has its own row in the matrix, making it highly sparse.
        The timing of the emitting process and potential additional temporal information of the bioshpere flow (e.g. delay of emission compared to timing of process) are considered.
        """

        for id in self.node_id_collection_dict["temporalized_processes"]:
            process_col_index = self.activity_dict[id]  # get the matrix column index

            ((original_db, original_code), time) = (
                self.activity_time_mapping_dict.reversed()[id]
            )

            td_producer = TemporalDistribution(
                date=np.array([str(time)], dtype=self.time_res), amount=np.array([1])
            ).date  # TODO: Simplify
            date = td_producer[0]

            act = bd.get_node(database=original_db, code=original_code)

            for exc in act.biosphere():
                if exc.get("temporal_distribution"):
                    td_dates = exc["temporal_distribution"].date  # time_delta
                    td_values = exc["temporal_distribution"].amount
                    dates = (
                        td_producer + td_dates
                    )  # we can add a datetime of length 1 to a timedelta of length N without problems
                    values = exc["amount"] * td_values

                else:  # exchange has no TD
                    dates = td_producer  # datetime array, same time as producer
                    values = [exc["amount"]]

                # Add entries to dynamic bio matrix
                for date, amount in zip(dates, values):
                    # first create a row index for the tuple((db, bioflow), date))
                    time_mapped_matrix_id = self.biosphere_time_mapping_dict.add(
                        (exc.input, date)
                    )

                    # populate lists with which sparse matrix is constructed
                    self.add_matrix_entry_for_biosphere_flows(
                        row=time_mapped_matrix_id,
                        col=process_col_index,
                        amount=amount,
                    )

        for id in self.node_id_collection_dict["temporal_markets"]:
            process_col_index = self.activity_dict[id]  # get the matrix column index
            technosphere_column = (
                self.technosphere_matrix[:, process_col_index].toarray().flatten()
            )  # 1-d np.array
            demand = dict()
            for idx, amount in enumerate(technosphere_column):
                if idx == self.activity_dict[id]:  # Skip production exchange
                    continue
                if amount == 0:
                    continue

                node_id = self.activity_dict.reversed[idx]

                if (
                    node_id in self.node_id_collection_dict["foreground_node_ids"]
                ):  # We only aggregate background process bioflows
                    continue

                # demand[bd.get_node(id=node_id)] = -amount
                demand[node_id] = -amount

            self.lca_obj.redo_lci(demand)
            aggregated_inventory = self.lca_obj.inventory.sum(
                axis=1
            )  # aggregated biosphere flows of background supply chain emissions. Rows are bioflows.

            for idx, amount in enumerate(aggregated_inventory.flatten().tolist()[0]):
                bioflow = bd.get_activity(self.lca_obj.dicts.biosphere.reversed[idx])
                ((_, _), time) = self.activity_time_mapping_dict.reversed()[id]

                td_producer = TemporalDistribution(
                    date=np.array([str(time)], dtype=self.time_res),
                    amount=np.array([1]),
                ).date  # TODO: Simplify
                date = td_producer[0]

                time_mapped_matrix_id = self.biosphere_time_mapping_dict.add(
                    (bioflow, date)
                )

                self.add_matrix_entry_for_biosphere_flows(
                    row=time_mapped_matrix_id, col=process_col_index, amount=amount
                )

        # now build the dynamic biosphere matrix
        shape = (max(self.rows) + 1, len(self.activity_time_mapping_dict))
        dynamic_biomatrix = sp.coo_matrix((self.values, (self.rows, self.cols)), shape)
        self.dynamic_biomatrix = dynamic_biomatrix.tocsr()

        return self.dynamic_biomatrix

    def add_matrix_entry_for_biosphere_flows(self, row, col, amount):
        """
        Adds an entry to the lists of row, col and values, which are used to construct the dynamic biosphere matrix, using build_biomatrix()

        :param row: A row index of a new element to the dynamic biosphere matrix
        :param col: A column index of a new element to the dynamic biosphere matrix
        :param amount: The amount of the new element to the dynamic biosphere matrix
        :return: None, but the lists of row, col and values are updated
        """
        self.rows.append(row)
        self.cols.append(col)
        self.values.append(amount)
