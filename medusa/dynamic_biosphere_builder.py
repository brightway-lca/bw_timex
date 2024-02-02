from scipy import sparse as sp
import pandas as pd
import numpy as np
import bw2data as bd
from bw_temporalis import TemporalDistribution
from .remapping import TimeMappingDict
from bw2calc import LCA



class DynamicBiosphere():

    def __init__(
            self,
            activity_dict: dict,
            activity_time_mapping_dict: dict,
            temporal_grouping: str,
            database_date_dict : dict,
            supply_array: np.array,
            len_technosphere_dbs: int
               # {(('db_name', 'process_code'), timestamp): matrix_index} (Timo is creating this)
           ):
        self._time_res_dict = {
                "year": "datetime64[Y]",
                "month": "datetime64[M]",
                "day": "datetime64[D]",
                "hour": "datetime64[h]",
            }

        self.activity_dict = activity_dict
        self.act_time_mapping = activity_time_mapping_dict
        self.time_res = self._time_res_dict[temporal_grouping]
        self.database_date_dict = database_date_dict
        self.dynamic_supply_array = supply_array
        self.len_technosphere_dbs = len_technosphere_dbs
        self.rows = []
        self.cols = []
        self.values = []
        

    def build_dynamic_biosphere_matrix(self):
        self.nr_procs = len(self.act_time_mapping)  # these are all the processes in the mapping (incl background)
        self.bio_row_mapping = TimeMappingDict(start_id=0)  # create new instance of TimeMappingdict for the biosphere flows

        # for producing_act in cleaned_production_timeline.itertuples():
        for (((db,code),time), id) in self.act_time_mapping.items():
            # Check if activity comes from foreground, if so, continue to next activity
            # because there's no need to include bioflows, as these processes will be exploded and hence no activity links to them
            if time == 'dynamic':
                continue
            process_col_index = self.activity_dict[id]   # get the matrix column index 
            act = bd.get_node(database=db, code=code)
            # print('time: {}, act["code"]: {}, db: {}, id: {}, process_col_index: {}'.format(time, act['code'], db, id, process_col_index))
            
             # Create TD from producer timestamp which is currently a pd.Timestamp and get the date
            # direct conversion with pd.Timestamp.to_pydatetime() leads to wrong dtype for some reason
            td_producer = TemporalDistribution(date=np.array([str(time)], dtype=self.time_res),
                                               amount=np.array([1])).date

            # check if act comes from background database, then calculate and move the lci to exploded processes
            if db in self.database_date_dict.keys() and db!='foreground':
                bg_lca = LCA({act : 1})
                bg_lca.lci()
                inventory = bg_lca.inventory.sum(axis=1)

                for key, row in bg_lca.dicts.biosphere.items():
                    amount = inventory[row,0]
                    bio_flow = bd.get_activity(key)
                    bio_date = td_producer[0]  # datetime array
                    bio_value = amount

                    # Add entry to dynamic bio matrix
                    # first create a row index for the tuple((db,bio_flow), date))
                    self.bio_row_mapping.add((bio_flow, bio_date))
                    bio_row_index = self.bio_row_mapping[(bio_flow, bio_date)]
                    # populate lists with which sparse matrix is constructed
                    # print('matrix entry: value: {} (row:{}, col:{})'.format(bio_flow,bio_row_index, process_col_index))
                    self.add_matrix_entry_for_biosphere_flows(row=bio_row_index, col=process_col_index, amount=bio_value)
            
            for exc in act.biosphere():
               
                try:  # case 1: exchange has biosphere TD
                    td_dates = exc['temporal_distribution'].date  # time_delta
                    td_values = exc['temporal_distribution'].amount
                    bio_dates = td_producer + td_dates  # we can add a datetime of length 1 to a timedelta of length N without problems
                    bio_values = exc['amount']*td_values

                except KeyError:  # case 2: exchange does not have TD
                    bio_dates = td_producer  # datetime array
                    bio_values = [exc['amount']]
                # print('bio_dates: ', bio_dates)
                # Add entries to dynamic bio matrix
                for bio_date, bio_flow in zip(bio_dates, bio_values):
                    # first create a row index for the tuple((db,bio_flow), date))
                    self.bio_row_mapping.add((exc.input, bio_date))
                    bio_row_index = self.bio_row_mapping[(exc.input, bio_date)]
                    # populate lists with which sparse matrix is constructed
                    # print('matrix entry: value: {} (row:{}, col:{})'.format(bio_flow,bio_row_index, process_col_index))
                    self.add_matrix_entry_for_biosphere_flows(row=bio_row_index, col=process_col_index, amount=bio_flow)
        
        # now build the dynamic biosphere matrix
        self.build_biomatrix()
        # and set the supply for the original databases to 0
        self.dynamic_supply_array[:self.len_technosphere_dbs] = 0




    def add_matrix_entry_for_biosphere_flows(self, 
                                             row,
                                             col,
                                             amount
                                             ):
        self.rows.append(row)
        self.cols.append(col)
        self.values.append(amount)
        
    def build_biomatrix(self):
        shape = (max(self.rows)+1, self.nr_procs)
        dynamic_biomatrix = sp.coo_matrix((self.values, (self.rows, self.cols)), shape)
        self.dynamic_biomatrix = dynamic_biomatrix.tocsr()
        # return dynamic_biomatrix.tocsr()


    
