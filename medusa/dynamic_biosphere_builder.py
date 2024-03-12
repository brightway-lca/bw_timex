from scipy import sparse as sp
import pandas as pd
import numpy as np
import bw2data as bd
from bw_temporalis import TemporalDistribution
from .remapping import TimeMappingDict
from bw2calc import LCA


class DynamicBiosphere:
    """
    This class is used to build a dynamic biosphere matrix, which in contrast to the normal biosphere matrix has rows for each biosphere flow at their time of emission
    Thus, the dimensions are (bio_flows at a specific timestep) x (processes).
    """

    def __init__(
        self,
        activity_dict: dict,
        activity_time_mapping_dict: dict,
        biosphere_time_mapping_dict: dict,
        temporal_grouping: str,
        database_date_dict: dict,
        supply_array: np.array,
        len_technosphere_dbs: int,
    ):
        self._time_res_dict = {
            "year": "datetime64[Y]",
            "month": "datetime64[M]",
            "day": "datetime64[D]",
            "hour": "datetime64[h]",
        }

        self.activity_dict = activity_dict
        self.activity_time_mapping_dict = activity_time_mapping_dict
        self.biosphere_time_mapping_dict = biosphere_time_mapping_dict
        self.time_res = self._time_res_dict[temporal_grouping]
        self.database_date_dict = database_date_dict
        self.dynamic_supply_array = supply_array
        self.len_technosphere_dbs = len_technosphere_dbs
        self.rows = []
        self.cols = []
        self.values = []

    def build_dynamic_biosphere_matrix(self):
        """
        This function creates a separate biosphere matrix, with the dimenions (bio_flows at a specific timestep) x (processes).
        Thus, every temporally resolved biosphere flow has its own row in the matrix, making it highly sparse.
        The timing of the emitting process and potential additional temporal information of the bioshpere flow (e.g. delay of emission compared to timing of process) are considered.
        """
        self.nr_procs = len(
            self.activity_time_mapping_dict
        )  # these are all the processes in the mapping (incl background)

        # looping over all activities:
        for ((db, code), time), id in self.activity_time_mapping_dict.items():
            # Check if activity comes from foreground, if so, continue to next activity
            # because there's no need to include their bioflows, as these processes will be exploded and hence no activity links to them
            if time == "dynamic":
                continue

            process_col_index = self.activity_dict[id]  # get the matrix column index
            act = bd.get_node(database=db, code=code)

            # Create TD instance of producer timestamp, which is currently a pd.Timestamp and get the date
            # direct conversion with pd.Timestamp.to_pydatetime() leads to wrong dtype for some reason
            td_producer = TemporalDistribution(
                date=np.array([str(time)], dtype=self.time_res), amount=np.array([1])
            ).date

            # supply chain emissions of activity
            # to avoid double counting, only add supply chain emissions for processes, which are linked to background database,
            if db in self.database_date_dict.keys() and db != "foreground":
                # then calculate lci
                bg_lca = LCA({act: 1})
                bg_lca.lci()
                inventory = bg_lca.inventory.sum(axis=1)
                # retrieve timing and amount of each supply chain emission
                for key, row in bg_lca.dicts.biosphere.items():
                    amount = inventory[row, 0]
                    bio_flow = bd.get_activity(key)
                    bio_date = td_producer[
                        0
                    ]  # datetime array, only one array element since already exploded, occuring at time of emitting process (no additional TD in the background database)
                    bio_value = amount

                    # Add entry to dynamic bio matrix
                    # first create a row index for the tuple((db,bio_flow), date))
                    self.biosphere_time_mapping_dict.add((bio_flow, bio_date))
                    bio_row_index = self.biosphere_time_mapping_dict[(bio_flow, bio_date)]
                    # populate lists with which sparse matrix is constructed
                    self.add_matrix_entry_for_biosphere_flows(
                        row=bio_row_index, col=process_col_index, amount=bio_value
                    )

            # direct emissions at activity
            for exc in act.biosphere():
                try:  # case 1: exchange has biosphere TD
                    td_dates = exc["temporal_distribution"].date  # time_delta
                    td_values = exc["temporal_distribution"].amount
                    bio_dates = (
                        td_producer + td_dates
                    )  # we can add a datetime of length 1 to a timedelta of length N without problems
                    bio_values = exc["amount"] * td_values

                except KeyError:  # case 2: exchange does not have TD
                    bio_dates = td_producer  # datetime array, same time as producer
                    bio_values = [exc["amount"]]

                # Add entries to dynamic bio matrix
                for bio_date, bio_flow in zip(bio_dates, bio_values):
                    # first create a row index for the tuple((db,bio_flow), date))
                    self.biosphere_time_mapping_dict.add((exc.input, bio_date))
                    bio_row_index = self.biosphere_time_mapping_dict[(exc.input, bio_date)]
                    # populate lists with which sparse matrix is constructed
                    self.add_matrix_entry_for_biosphere_flows(
                        row=bio_row_index, col=process_col_index, amount=bio_flow
                    )

        # now build the dynamic biosphere matrix
        self.build_biomatrix()

        # and set the supply for the original databases to 0 -> why?
        self.dynamic_supply_array[: self.len_technosphere_dbs] = 0

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

    def build_biomatrix(self):
        """
        Builds the dynamic biosphere matrix from the lists of row, col and values
        """
        shape = (max(self.rows) + 1, self.nr_procs)
        dynamic_biomatrix = sp.coo_matrix((self.values, (self.rows, self.cols)), shape)
        self.dynamic_biomatrix = dynamic_biomatrix.tocsr()
