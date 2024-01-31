from scipy import sparse as sp
import pandas as pd
import numpy as np
import bw2data as bd
from bw_temporalis import TemporalDistribution
from .remapping import TimeMappingDict


class DynamicBiosphere():

    def __init__(
            self,
            bio_matrix: sp.csr_matrix,
            supply: np.array,  # the supply array from the mlca
            timeline: pd.DataFrame,    # later perhapds just a mapping dictionary
            database_date_dict: dict,  # the timestamps of the (prospective) background databases
            # mapping_dictionary_medusa_processes: dict,    # {(('db_name', 'process_name'), timestamp): matrix_index} (Timo is creating this)
           ):
        self.bio_matrix = bio_matrix
        self.supply = supply
        self.timeline = timeline
        self.database_date_dict = database_date_dict
        self.rows = []
        self.cols = []
        self.values = []
        self.bio_row_mapping = TimeMappingDict(start_id=0)
        self.act_col_mapping = TimeMappingDict(start_id=0)

    def build_dynamic_biosphere_matrix(self):
        cleaned_production_timeline = self.timeline[['hash_producer', 'date_producer', 'producer_name', 'producer']].drop_duplicates()
        
        self.nr_procs = len(cleaned_production_timeline)  # Needs to be changed as soon as we also include background database biosphere flows

        for producing_act in cleaned_production_timeline.itertuples():
            act = bd.get_node(id=producing_act.producer)
            for exc in act.biosphere():
                #print(exc.input)
                # Create TD from producer timestamp which is currently a pd.Timestamp and get the date
                # direct conversion with pd.Timestamp.to_pydatetime() leads to wrong dtype for some reason
                td_producer = TemporalDistribution(date=np.array([producing_act.date_producer], dtype='datetime64[s]'),
                                                       amount=np.array([1])).date
                
                try:  # case 1: exchange has biosphere TD
                    td_dates = exc['temporal_distribution'].date  # time_delta
                    td_values = exc['temporal_distribution'].amount
                    bio_dates = td_producer + td_dates  # we can add a datetime of length 1 to a timedelta of length N without problems
                    bio_values = exc['amount']*td_values

                except KeyError:  # case 2: exchange does not have TD
                    bio_dates = td_producer  # datetime array
                    bio_values = [exc['amount']]
                
                # Add entries to dynamic bio matrix
                for bio_date, bio_flow in zip(bio_dates, bio_values):
                    # print(bio_date, type(bio_date), bio_flow, type(bio_flow))
                    # first create a row index for the tuple((db,bio_flow), date))
                    self.create_dynamic_biosphere_matrix_indices(exc.input, bio_date, producing_act.producer, producing_act.hash_producer)
                    bio_row_index = self.bio_row_mapping[(exc.input, bio_date)]
                    process_col_index = self.act_col_mapping[(producing_act.producer, producing_act.hash_producer)]

                    # populate lists with which sparse matrix is constructed
                    self.add_matrix_entry_for_biosphere_flows(row=bio_row_index, col=process_col_index, amount=bio_flow)
     
        self.build_biomatrix()



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
        # print((self.values, (self.rows, self.cols)), shape)
        dynamic_biomatrix = sp.coo_matrix((self.values, (self.rows, self.cols)), shape)
        self.dynamic_biomatrix = dynamic_biomatrix.tocsr()
        # return dynamic_biomatrix.tocsr()


    def create_dynamic_biosphere_matrix_indices(self,bio_flow, date, act, act_hash):
        """creates matrix index for bio flow and adds the bioflow: index to self.bio_row_mapping"""
        self.bio_row_mapping.add((bio_flow,date))
        self.act_col_mapping.add((act, act_hash))

    
    
