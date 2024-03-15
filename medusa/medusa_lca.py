import bw2data as bd
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb
from peewee import fn

from datetime import datetime
from typing import Optional, Callable
from bw2data import (  # for prepare_medusa_lca_inputs
    Database,
    Method,
    Normalization,
    Weighting,
    databases,
    methods,
    normalizations,
    projects,
    weightings,
)
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.backends.schema import get_id
from bw2data.errors import Brightway2Project
from bw2calc import LCA
from .timeline_builder import TimelineBuilder
from .matrix_modifier import MatrixModifier
from .dynamic_biosphere_builder import DynamicBiosphere
from .dynamic_characterization import DynamicCharacterization
from .remapping import TimeMappingDict
from .utils import extract_date_as_integer


class MedusaLCA:
    """
    Class to perform dynamic-prospective LCA calculations.

    The key point of MedusaLCA is that it retrieves the LCI of processes occuring at a certain time from a database that represents the technology landscape at this point in time.

    It first calculates a static LCA, which informs a priority-first graph traversal, from which the temporal relationships between exchanges and processes are derived.
    For processes at the interface with background databases, the timing of the exchanges determines which background database to link to.
    This temporal relinking is achieved by using datapackages to add new processes, that represent processes happening at a specific time period, to the technopshere matrix
    and their corresponding biosphere flows to the biosphere matrix and then linking these to the original demand.
    The temporal information can also be used for dynamic LCIA chalculations.

    MedusaLCA calculates:
     1) a conventional LCA score (MedusaLCA.static_lca.score(), same as BW2 lca.score()),
     2) a dynamic-prospective LCA score, which links LCIs to the respective background databases but without additional temporal dynamics of the biosphere flows (MedusaLCA.lca.score())
     3) a dynamic-prospective LCA score with dynamic inventory and dynamic charaterization factors (not yet operational: TODO dynamic CFs in general and optional add Levasseur methodology with TH-cutoff)

    """

    def __init__(
        self,
        demand,
        method,
        edge_filter_function: Callable,
        database_date_dict: dict,
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
        **kwargs,
    ):
        self.demand = demand
        self.method = method
        self.edge_filter_function = edge_filter_function
        self.database_date_dict = database_date_dict
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type

        # Calculate static LCA results using a custom prepare_lca_inputs function that includes all background databases in the LCA. We need all the IDs for the time mapping dict.
        fu, data_objs, remapping = self.prepare_static_lca_inputs(
            demand=self.demand, method=self.method
        )
        self.static_lca = LCA(fu, data_objs=data_objs, remapping_dicts=remapping)
        self.static_lca.lci()
        self.static_lca.lcia()

        # Create a time mapping dict that maps each activity to a activity_time_mapping_id in the format (('database', 'code'), datetime_as_integer): time_mapping_id)
        self.activity_time_mapping_dict = TimeMappingDict(
            start_id=bd.backends.ActivityDataset.select(fn.MAX(AD.id)).scalar() + 1
        )  # making sure to use unique ids for the time mapped processes by counting up from the highest current activity id
        self.add_activities_to_time_mapping_dict()
        
        # Create a similar dict for the biosphere flows. This is populated by the dynamic_biosphere_builder
        self.biosphere_time_mapping_dict = TimeMappingDict(
            start_id=0
        )  

        # Create static_only dict that excludes dynamic processes that will be exploded later. This way we only have the "background databases" that we can later link to from the dates of the timeline.
        self.database_date_dict_static_only = {
            k: v for k, v in self.database_date_dict.items() if type(v) == datetime
        }

        # Create timeline builder that does a the graph traversal (similar to bw_temporalis) and extracts all edges with their temporal information. Can later be used to build a timeline with the TimelineBuilder.build_timeline() method.
        self.tl_builder = TimelineBuilder(
            self.static_lca,
            self.edge_filter_function,
            self.database_date_dict_static_only,
            self.activity_time_mapping_dict,
            self.temporal_grouping,
            self.interpolation_type,
            **kwargs,
        )

    def add_activities_to_time_mapping_dict(self):
        """
        Adds all activities to activity_time_mapping_dict, an instance of TimeMappingDict.
        This gives a unique mapping in the form of (('database', 'code'), datetime_as_integer): time_mapping_id) that is later used to uniquely identify time-resolved processes.

        """
        for id in self.static_lca.dicts.activity.keys():  # activity ids
            key = self.static_lca.remapping_dicts["activity"][
                id
            ]  # ('database', 'code')
            time = self.database_date_dict[
                key[0]
            ]  # datetime (or 'dynamic' for TD'd processes)
            if type(time) == str:  # if 'dynamic', just add the string
                self.activity_time_mapping_dict.add((key, time), key=id)
            elif type(time) == datetime:
                self.activity_time_mapping_dict.add(
                    (key, extract_date_as_integer(time, self.temporal_grouping)), key=id
                )  # if datetime, map to the date as integer
            else:
                warnings.warn(f"Time of activity {key} is neither datetime nor str.")

    def build_timeline(self):
        """
        Build a timeline DataFrame of the exchanges using the TimelineBuilder class.
        """
        self.timeline = self.tl_builder.build_timeline()
        return self.timeline

    def build_datapackage(self):
        """
        Create the datapackages that contain the modifications to the technopshere and biosphere matrix using the MatrixModifier class.
        """
        if not hasattr(self, "timeline"):
            warnings.warn(
                "Timeline not yet built. Call MedusaLCA.build_timeline() first."
            )
            return

        # mapping of the demand id to demand time
        self.demand_timing_dict = self.create_demand_timing_dict()

        # Create matrix modifier that creates the new datapackages with the exploded processes and new links to background databases.
        self.matrix_modifier = MatrixModifier(
            self.timeline, self.database_date_dict_static_only, self.demand_timing_dict
        )
        self.datapackage = self.matrix_modifier.create_datapackage()

    def lci(self):
        """
        Calculate the Medusa LCI, which links its LCIs to correct background databases but without timing of biosphere flows, so without dynamic LCIA, which is implemented in build_dynamic_biosphere().
        """

        if not hasattr(self, "timeline"):
            warnings.warn(
                "Timeline not yet built. Call MedusaLCA.build_timeline() first."
            )
            return
        if not hasattr(self, "datapackage"):
            warnings.warn(
                "Datapackage not yet built. Call MedusaLCA.build_datapackage() first."
            )
            return

        self.fu, self.data_objs, self.remapping = self.prepare_medusa_lca_inputs(
            demand=self.demand,
            method=self.method,
            demand_timing_dict=self.demand_timing_dict,
        )
        # using the datapackages with the matrix modifications
        self.lca = LCA(
            self.fu,
            data_objs=self.data_objs + self.datapackage,
            remapping_dicts=self.remapping,
        )
        self.lca.lci()
        self.calculate_dynamic_biosphere_lci()

    def lcia(self):
        """
        Calculate the Medusa LCIA, usings LCIs from the correct background databases.
        """
        if not hasattr(self, "lca"):
            warnings.warn("LCI not yet calculated. Call MedusaLCA.lci() first.")
            return
        self.lca.lcia()

    #TODO maybe restructure the dynamic biosphere building into one method, similar to lci() and lcia()? dynamic_lci(), which calls build_dynamic_biosphere() and calculate_dynamic_lci()?
    
    def build_dynamic_biosphere(self):
        """
        Build the dynamic biosphere matrix, which links the biosphere flows to the correct background databases and tracks the timing each biosphere flows, using DynamicBiosphere class.
        
        This returns a matrix of the dimensions (bio_flows at a specific timestep) x (processes)

        """

        if not hasattr(self, "lca"):
            warnings.warn(
                "Static Medusa LCA has not been run. Call MedusaLCA.lci() first."
            )
            return

        self.len_technosphere_dbs = sum(
            [len(bd.Database(db)) for db in self.database_date_dict.keys()]
        )
        self.dynamic_biosphere_builder = DynamicBiosphere(
            self.dicts.activity,
            self.activity_time_mapping_dict,
            self.biosphere_time_mapping_dict,
            self.temporal_grouping,
            self.database_date_dict,
            self.supply_array,
            self.len_technosphere_dbs,
        )
        self.dynamic_biosphere_builder.build_dynamic_biosphere_matrix()
        self.dynamic_biomatrix = self.dynamic_biosphere_builder.dynamic_biomatrix

    def calculate_dynamic_biosphere_lci(
        self,
    ):
        """
        Calcluates the dynamic inventory from the dynamic biosphere matrix by calling build_dynamic_inventory_dict.
        Returns a dictionary with the dynamic inventory of the LCI in the form of
          {CO2: {
                time: [2022, 2023], 
                amount:[3,5], 
                emitting_process: [((database, code), year), ((database, code), year)]
                }, 
          CH4: {time: [2022, 2023], 
                amount:[3,5], 
                emitting_process: [((database, code), year), ((database, code), year)]
                },
          ...}
        """
        self.build_dynamic_biosphere()

        # calculate lci from dynamic biosphere matrix
        # unordered_dynamic_lci = self.dynamic_biomatrix.dot(
        #     self.dynamic_supply_array 
        # )  

        #diagnolization of supply array keeps the dimension of the process, which we want to pass as additional information to the dynamic inventory dict    
        diagonal_supply_array = np.diag(self.dynamic_supply_array.flatten()) #dynamic supply array includes the original databases, contrary to former implementation as otherwise background processes were excluded. TODO: validate with Timo
        diagonalized_dynamic_lci = self.dynamic_biomatrix.dot(diagonal_supply_array)
        
        #unordered_dynamic_lci= np.sum(diagonalized_dynamic_lci, axis=1) #normal lci

        
        self.build_dynamic_inventory_dict(diagonalized_dynamic_lci)

    def build_dynamic_inventory_dict(
        self,
        diagonalized_dynamic_lci: np.array,
    ):
        """
        Create dynamic lci dictionary with structure 
        {CO2: {
                time: [2022, 2023], 
                amount:[3,5], 
                emitting_process: [((database, code), year), ((database, code), year)]
                }, 
          CH4: {time: [2022, 2023], 
                amount:[3,5], 
                emitting_process: [((database, code), year), ((database, code), year)]
                },
          ...}
        :param diagonalized_dynamic_lci: diagnolized lci results, pertaining additional information of emitting activity
        :return: none but sets the dynamic_inventory attribute of the MedusaLCA instance
        """
        self.dynamic_inventory = (
            {}
        )  # dictionary to store the dynamic lci {CO2: {time: [2022, 2023], amount:[3,5], emitting_process: (database, code)}}
        
        self.act_time_mapping_reversed= {v: k for k, v in self.activity_time_mapping.items()} #reversed mapping of activity_time_mapping_dict #TODO check if used

        for (flow, time), row_id in self.biosphere_time_mapping_dict.items(): #looping over the rows of the diagnolized df
            
            # add biosphere flow to dictionary if it does not exist yet
            if not flow["code"] in self.dynamic_inventory.keys():
                self.dynamic_inventory[flow["code"]] = {"time": [], "amount": [], "emitting_process": []}  
            
            for col_id in self.activity_dict.reversed.keys(): #looping over the columns of the diagnolized df
                
                if diagonalized_dynamic_lci[row_id, col_id] != 0: #store only non-zero elements of diagnolized inventory
                    
                    amount = diagonalized_dynamic_lci[row_id, col_id]
                    emitting_process = self.activity_dict.reversed[col_id]
                    #print(emitting_process)
                    self.dynamic_inventory[flow["code"]]["time"].append(time)
                    self.dynamic_inventory[flow["code"]]["amount"].append(amount)
                    self.dynamic_inventory[flow["code"]]["emitting_process"].append(emitting_process) 
            
            #old code, can be deleted if confidence in new code is established
            #amount = unordered_dynamic_lci[row_id]
            # add biosphere flow to dictionary if it does not exist yet
            # if not flow["code"] in self.dynamic_inventory.keys():
            #     self.dynamic_inventory[flow["code"]] = {"time": [], "amount": [], "emitting_process": []}
            # # fill dictionary
            # self.dynamic_inventory[flow["code"]]["time"].append(time)
            # self.dynamic_inventory[flow["code"]]["amount"].append(amount)
            # self.dynamic_inventory[flow["code"]]["emitting_process"].append(np.nan)

        # now sort flows based on time
        for flow, _ in self.dynamic_inventory.items():
            order = np.argsort(self.dynamic_inventory[flow]["time"])

            self.dynamic_inventory[flow]["time"] = np.array(
                self.dynamic_inventory[flow]["time"]
            )[order]
            self.dynamic_inventory[flow]["amount"] = np.array(
                self.dynamic_inventory[flow]["amount"]
            )[order]
            self.dynamic_inventory[flow]["emitting_process"] = np.array(
                self.dynamic_inventory[flow]["emitting_process"]
            )[order]


    def characterize_dynamic_lci(
            
            self,
            cumsum: bool | None = True,
            type: str | None = "radiative_forcing",
            fixed_TH: bool | None = False, #True: Levasseur approach TH for all emissions is calculated from FU, false: TH is calculated from t emission
            TH: int | None = 100, 
            #characterization_dictionary: dict, #dict is now stored inside dynamic_inventory_characterizer, not sure if this is pythonic
    ):
        """
        Characterize the dynamic inventory dictionaries using dynamic characterization functions using the DynamicCharacterization class.
        Characterization function are provided imported from BW_temporalis and are planned to be extended. The format of the characterization_dictionary is {biosphere_flow_name: characterization_function}.
        Users can you provide their own dynamic characterization functions, which needs to have the format XZXZ (TODO complete description).

        """

        if not hasattr(self, "dynamic_inventory"):
            warnings.warn(
                "Dynamic lci not yet calculated. Call MedusaLCA.calculate_dynamic_lci() first."
            )
            
        self.dynamic_inventory_characterizer = DynamicCharacterization(
                                            self.dynamic_inventory, 
                                            self.dicts.activity,
                                            self.dicts.biosphere.reversed,
                                            self.act_time_mapping_reversed,
                                            self.demand_timing_dict,
                                            self.temporal_grouping,
                                            )
        
        (self.characterized_inventory, self.type_of_method, self.fixed_TH, self.TH) = self.dynamic_inventory_characterizer.characterize_dynamic_inventory(
            cumsum,
            type,
            fixed_TH, #True: Levasseur approach TH for all emissions is calculated from FU, false: TH is calculated from t emission
            TH, 
        )
        self.characterized_dynamic_score = self.characterized_inventory["amount"].sum() 
        

    def prepare_static_lca_inputs(
        self,
        demand=None,
        method=None,
        weighting=None,
        normalization=None,
        demands=None,
        remapping=True,
        demand_database_last=True,
    ):
        """
        Prepare LCA input arguments in Brightway 2.5 style.
        ORIGINALLY FROM bw2data.compat.py

        The difference to the original method is that we load all available databases into the matrices instead of just the ones depending on the demand.
        We need this for the creation of the time mapping dict that creates a mapping between the producer id and the reference timing of the databases in the database_date_dict.
        """
        if not projects.dataset.data.get("25"):
            raise Brightway2Project(
                "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
            )

        databases.clean()
        data_objs = []
        remapping_dicts = None

        demand_database_names = [
            db_label for db_label in databases
        ]  # Always load all databases. This could be handled more elegantly..

        if demand_database_names:
            database_names = set.union(
                *[
                    Database(db_label).find_graph_dependents()
                    for db_label in demand_database_names
                ]
            )

            if demand_database_last:
                database_names = [
                    x for x in database_names if x not in demand_database_names
                ] + demand_database_names

            data_objs.extend([Database(obj).datapackage() for obj in database_names])

            if remapping:
                # This is technically wrong - we could have more complicated queries
                # to determine what is truly a product, activity, etc.
                # However, for the default database schema, we know that each node
                # has a unique ID, so this won't produce incorrect responses,
                # just too many values. As the dictionary only exists once, this is
                # not really a problem.
                reversed_mapping = {
                    i: (d, c)
                    for d, c, i in AD.select(AD.database, AD.code, AD.id)
                    .where(AD.database << database_names)
                    .tuples()
                }
                remapping_dicts = {
                    "activity": reversed_mapping,
                    "product": reversed_mapping,
                    "biosphere": reversed_mapping,
                }

        if method:
            assert method in methods
            data_objs.append(Method(method).datapackage())
        if weighting:
            assert weighting in weightings
            data_objs.append(Weighting(weighting).datapackage())
        if normalization:
            assert normalization in normalizations
            data_objs.append(Normalization(normalization).datapackage())

        if demands:
            indexed_demand = [{get_id(k): v for k, v in dct.items()} for dct in demands]
        elif demand:
            indexed_demand = {get_id(k): v for k, v in demand.items()}
        else:
            indexed_demand = None

        return indexed_demand, data_objs, remapping_dicts

    def prepare_medusa_lca_inputs(
        self,
        demand=None,
        method=None,
        demand_timing_dict=None,
        weighting=None,
        normalization=None,
        demands=None,
        remapping=True,
        demand_database_last=True,
    ):
        """
        Prepare LCA input arguments in Brightway 2.5 style.
        ORIGINALLY FROM bw2data.compat.py

        Changes include:
        - always load all databases in demand_database_names
        - indexed_demand has the id of the new consumer_id of the "exploded" demand (TODO: think about more elegant way)

        """
        if not projects.dataset.data.get("25"):
            raise Brightway2Project(
                "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
            )

        databases.clean()
        data_objs = []
        remapping_dicts = None

        demand_database_names = [
            db_label for db_label in databases
        ]  # Always load all databases. This could be handled more elegantly..

        if demand_database_names:
            database_names = set.union(
                *[
                    Database(db_label).find_graph_dependents()
                    for db_label in demand_database_names
                ]
            )

            if demand_database_last:
                database_names = [
                    x for x in database_names if x not in demand_database_names
                ] + demand_database_names

            data_objs.extend([Database(obj).datapackage() for obj in database_names])

            if remapping:
                # This is technically wrong - we could have more complicated queries
                # to determine what is truly a product, activity, etc.
                # However, for the default database schema, we know that each node
                # has a unique ID, so this won't produce incorrect responses,
                # just too many values. As the dictionary only exists once, this is
                # not really a problem.
                reversed_mapping = {
                    i: (d, c)
                    for d, c, i in AD.select(AD.database, AD.code, AD.id)
                    .where(AD.database << database_names)
                    .tuples()
                }
                remapping_dicts = {
                    "activity": reversed_mapping,
                    "product": reversed_mapping,
                    "biosphere": reversed_mapping,
                }

        if method:
            assert method in methods
            data_objs.append(Method(method).datapackage())
        if weighting:
            assert weighting in weightings
            data_objs.append(Weighting(weighting).datapackage())
        if normalization:
            assert normalization in normalizations
            data_objs.append(Normalization(normalization).datapackage())

        if demands:
            indexed_demand = [
                {
                    self.activity_time_mapping_dict[
                        (
                            bd.get_node(id=bd.get_id(k)).key,
                            self.demand_timing_dict[bd.get_id(k)],
                        )
                    ]: v
                    for k, v in dct.items()
                }
                for dct in demands
            ]
        elif demand:
            indexed_demand = {
                self.activity_time_mapping_dict[
                    (
                        bd.get_node(id=bd.get_id(k)).key,
                        self.demand_timing_dict[bd.get_id(k)],
                    )
                ]: v
                for k, v in demand.items()
            }
        else:
            indexed_demand = None

        return indexed_demand, data_objs, remapping_dicts

    def create_demand_timing_dict(self) -> dict:
        """
        Generate a dictionary mapping producer (key) to reference timing (value) for specific demands.
        Reference timing can have flexible resolution (year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH) defined in temporal_grouping.

        It searches the timeline for those rows that contain the functional units (demand-processes as producer and -1 as consumer) and returns the time of the demand.

        :param timeline: Timeline DataFrame, generated by `create_grouped_edge_dataframe`.
        :param demand: Demand dict

        :return: Dictionary mapping producer ids to reference timing  for the specified demands.
        """
        demand_ids = [bd.get_activity(key).id for key in self.demand.keys()]
        demand_rows = self.timeline[
            self.timeline["producer"].isin(demand_ids)
            & (self.timeline["consumer"] == -1)
        ]
        self.demand_timing_dict = {
            row.producer: row.hash_producer for row in demand_rows.itertuples()
        }
        return self.demand_timing_dict

    def create_labelled_technosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the technosphere matrix as a dataframe with comprehensible labels instead of ids.
        """

        df = pd.DataFrame(self.technosphere_matrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.dicts.activity.reversed,
            columns=self.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(  # from activity id to ((database, code), time)
            index=self.activity_time_mapping_dict.reversed(),
            columns=self.activity_time_mapping_dict.reversed(),
            inplace=True,
        )
        return df

    def create_labelled_biosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the biosphere matrix as a dataframe with comprehensible labels instead of ids.
        """

        df = pd.DataFrame(self.biosphere_matrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.dicts.biosphere.reversed,
            columns=self.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(
            index=self.remapping_dicts["biosphere"],  # from activity id to bioflow name
            columns=self.activity_time_mapping_dict.reversed(),  # from activity id to ((database, code), time)
            inplace=True,
        )
        
        # Replace id indexes with biosphere codes
        bio_labels = []
        for index in df.index:
            bio_code= bd.get_node(id=index)['code']
            bio_labels.append(bio_code)

        df.index = bio_labels

        return df

    def create_labelled_dynamic_biosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the dynamic biosphere matrix as a dataframe with comprehensible labels instead of ids.
        """
        biosphere_time_mapping_dict_reversed = {
            index: (flow["code"], time)
            for (flow, time), index in self.biosphere_time_mapping_dict.items()
        }

        df = pd.DataFrame(self.dynamic_biomatrix.toarray())
        df.rename(  # from matrix id to activity id
            index=biosphere_time_mapping_dict_reversed,
            columns=self.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(  # from activity id to ((database, code), time)
            columns=self.activity_time_mapping_dict.reversed(),
            inplace=True,
        )
        return df

    def plot_dynamic_inventory(self, bio_flow: str):
        """ "
        Simple plot of dynamic inventory of a biosphere flow over time.
        :param bio_flow: str, name of the biosphere flow to plot
        :return: none, but shows a plot
        """
        plt.plot(
            self.dynamic_inventory[bio_flow]["time"],
            self.dynamic_inventory[bio_flow]["amount"],
        )
        plt.ylabel(bio_flow)
        plt.xlabel("time")
        plt.show()

    def remap_inventory_dicts(self) -> None:
        warnings.warn(
            "bw25's original mapping function doesn't work with our new time-mapped matrix entries. The medusa mapping can be found in acvitity_time_mapping_dict and biosphere_time_mapping_dict."
        )
        return

    def plot_dynamic_characterized_inventory(self, cumsum: bool = False):
        """
        Plot the characterized inventory of the dynamic LCI in a very simple plot
        """
        if cumsum is True:
            amount = "amount_sum"
        else: 
            amount = "amount"

        if not hasattr(self, "characterized_inventory"):
            warnings.warn(
                "Characterized inventory not yet calculated. Call MedusaLCA.characterize_dynamic_lci() first."
            )
            return
        
        if self.type_of_method == "radiative_forcing":
            label_legend = "Radiative forcing [W/m2]"
            title = "Radiative forcing"
        if self.type_of_method == "GWP":
            label_legend = "GWP [kg CO2-eq]"
            title = "GWP"
        
        if self.fixed_TH == False: # if TH is calculated from t emission
            
            title += " with TH of " + str(self.TH) + " " + str(self.temporal_grouping) + " starting at each emission"

        if self.fixed_TH == True: # if TH is calculated from FU
            title += " with TH of " + str(self.TH) + " " + str(self.temporal_grouping) + " starting at FU"

        axes = sb.scatterplot(
            x="date", 
            y=amount,
            hue="activity_name",
            style="flow_name",
            data=self.characterized_inventory
        )
        
        
        axes.set_ylabel(label_legend)
        axes.set_xlabel("Time (" +  str(self.temporal_grouping) +")")
        
        axes.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        axes.set_title(title)
        plt.show()

    def __getattr__(self, name):
        """
        Delegate attribute access to the self.lca object if the attribute
        is not found in the MedusaLCA instance itself.
        """
        if hasattr(self.lca, name):
            return getattr(self.lca, name)
        elif hasattr(self.dynamic_biosphere_builder, name):
            return getattr(self.dynamic_biosphere_builder, name)
        else:
            raise AttributeError(
                f"'MedusaLCA' object and its 'lca'- and dynamic_biosphere_builder- attributes have no attribute '{name}'"
            )
