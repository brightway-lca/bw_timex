from scipy import sparse as sp
import pandas as pd
import numpy as np
import bw2data as bd
from bw_temporalis import TemporalDistribution


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

    def build_dynamic_biosphere_matrix(self):
        cleaned_production_timeline = self.timeline[['hash_producer', 'date_producer', 'producer_name', 'producer']].drop_duplicates() 
        for producing_act in cleaned_production_timeline.itertuples():
            act = bd.get_node(id=producing_act.producer)
            for exc in act.biosphere():
                print(exc.as_dict())
                # Create TD from producer timestamp which is currently a pd.Timestamp and get the date
                # direct conversion with pd.Timestamp.to_pydatetime() leads to wrong dtype for some reason
                td_producer = TemporalDistribution(date=np.array([producing_act.date_producer], dtype='datetime64[s]'),
                                                       amount=np.array([1])).date
                try:  # case 1: exchange has biosphere TD
                    print(act['name'], producing_act.hash_producer, exc.input, exc['temporal_distribution'])
                    td_dates = exc['temporal_distribution'].date  # time_delta
                    td_values = exc['temporal_distribution'].amount
                    bio_dates = td_producer + td_dates  # we can add a datetime of length 1 to a timedelta of length N without problems
                    bio_values = exc['amount']*td_values

                except KeyError:  # case 2: exchange does not have TD
                    print(act['name'], producing_act.hash_producer, exc.input)
                    bio_dates = [td_producer]  # datetime
                    bio_values = exc['amount']
                print(bio_values, bio_dates)



             

        
    
    def add_matrix_entry_for_biosphere_flows_without_additional_td(self):
        pass
    
    def add_matrix_entry_for_biosphere_flows_with_additional_td(self):
        pass
