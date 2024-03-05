#from bw_temporalis 
from typing import Union, Tuple, Optional, Callable
import pandas as pd
import bw2data as bd

class DynamicCharacterization():
    '''
    This class calculates dynamic characterization of life cycle inventories.
    
    Currently, dynamic characterization functions is available for the following emissions:
    - CO2
    - CH4
    
    Characteriztaion functions are retrieved from bw_temporalis, which have them for CO2 and Methane from these publications: 
    https://doi.org/10.5194/acp-13-2793-2013
    http://pubs.acs.org/doi/abs/10.1021/acs.est.5b01118
    
    '''
    def __init__(self, 
                 #method: str, 
                 #**kwargs,
                 dynamic_inventory: dict,
                 activity_dict: dict,
                 biosphere_dict_rerversed: dict,
                 ):
        self.dynamic_inventory = dynamic_inventory
        self.activity_dict = activity_dict
        self.biosphere_dict_rerversed = biosphere_dict_rerversed
        self.dynamic_lci_df = self.format_dynamic_inventory_as_dataframe()
        
    def format_dynamic_inventory_as_dataframe(self):
        
        ''' bring the dynamic inventory into the right format to use the characterization functions from bw_temporalis
        Format needs to be: 
        | date | amount | flow | activity |
        |------|--------|------|----------|
        | 101  | 33     | CO2    | 2        |
        | 102  | 32     | 1    | 2        |
        | 103  | 31     | 1    | 2        |
        
        date is datetime
        flow = the code of the biosphere flow #TODO implement this
        activity = activity id #TODO implement this: Here we need to store the producing activity of the emission already in the dynamic inventory dictionary
        '''
        flow_mapping= {}
        for key, values in self.dynamic_inventory.items(): #key is currently code, which is the uuid
            flow_mapping[key] = bd.get_node(code=key).id #mapping of code (uuid) to id

        dfs = []
        for key, values in self.dynamic_inventory.items():
            df = pd.DataFrame(values)
            df['flow'] = key
            df.rename(columns={'time': 'date'}, inplace=True)                  
            df['flow'] = df['flow'].replace(flow_mapping)
            dfs.append(df)
        inventory_as_dataframe = pd.concat(dfs, ignore_index=True)
        inventory_as_dataframe['activity']= 76867 #Placeholder, #TODO replace with producing activity Id from mapping'
        
        return inventory_as_dataframe
    
    
    def characterize_dynamic_inventory(self,                                                        
        characterization_dictionary: dict, # dictionary: mapping elemental flow (key) with characteization function (value) 
        flow: set[int] | None = None,
        activity: set[int] | None = None,
        cumsum: bool | None = True,
        
    ) -> pd.DataFrame:
        
        ''' 
        Function adapted from bw_temporalis to the fact that in comparison to bw_temporalis, our timeline not a Timeline instance, but a normal pd.DataFrame.
        Adjusted to filter the respective elemental flows to be characterized per characterization function, instead of assuming all flows to be categorized.
        
        can receive bw_temporalis characterization functions for CO2 and CH4 or user-defined characterization functions of the same format XZXZ.
        
        The `characterization_function` is applied to each row of the input DataFrame of a timeline for a given `period`. 
        in the case of characterize_co2 and characterize_methane, the timestep is yearly and the time horizon is 100 years
                  
        # TODO add checks, add visualization of the characterized inventory, think about dynamic characterization as in Levasseur, add correct activity id
            
        '''

        all_characterized_inventory = pd.DataFrame()
        
        mapping_flow_to_id = {flow: bd.get_activity(name=flow).id for flow in characterization_dictionary.keys()}

        for characterized_flow, characterization_function in characterization_dictionary.items():
            df = self.dynamic_lci_df.copy()
             
            df = df.loc[self.dynamic_lci_df["flow"]==mapping_flow_to_id[characterized_flow]] #subset of the inventory including characterized flow
            
            #in case the user specifies additional subsets
            if activity:
                df = df.loc[self.dynamic_lci_df["activity"].isin(activity)]
            if flow:
                df = df.loc[self.dynamic_lci_df["flow"]==flow]
                
            df.reset_index(drop=True, inplace=True)

 
            characterized_inventory = pd.concat(
                [characterization_function(row) for _, row in df.iterrows()] # using characterization function from bw_temporalis
            )
            if "date" in characterized_inventory.columns:
                characterized_inventory.sort_values(by="date", ascending=True, inplace=True)
                characterized_inventory.reset_index(drop=True, inplace=True)
            if cumsum and "amount" in characterized_inventory:
                characterized_inventory["amount_sum"] = characterized_inventory["amount"].cumsum() #not usre if cumsum here or after concatenation with other dfs

            all_characterized_inventory = pd.concat([all_characterized_inventory, characterized_inventory])
                
        return all_characterized_inventory
        

 
    