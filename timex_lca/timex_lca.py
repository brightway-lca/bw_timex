import bw2data as bd
import warnings
import pandas as pd
import numpy as np
import copy
import matplotlib.pyplot as plt
import seaborn as sb
from peewee import fn
from collections import defaultdict
from scipy import sparse

from datetime import datetime
from typing import Optional, Callable
from bw2data import (  # for prepare_timex_lca_inputs
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
from .dynamic_biosphere_builder import DynamicBiosphereBuilder
from .dynamic_characterization import DynamicCharacterization
from .remapping import TimeMappingDict
from .utils import extract_date_as_integer


class TimexLCA:
    """
    Class to perform dynamic-prospective LCA calculations.

    The key point of TimexLCA is that it retrieves the LCI of processes occuring at a certain time from a database that represents the technology landscape at this point in time.

    It first calculates a static LCA, which informs a priority-first graph traversal, from which the temporal relationships between exchanges and processes are derived.
    For processes at the interface with background databases, the timing of the exchanges determines which background database to link to.
    This temporal relinking is achieved by using datapackages to add new processes, that represent processes happening at a specific time period, to the technopshere matrix
    and their corresponding biosphere flows to the biosphere matrix and then linking these to the original demand.
    The temporal information can also be used for dynamic LCIA chalculations.

    TimexLCA calculates:
     1) a conventional LCA score (TimexLCA.static_lca.score(), same as BW2 lca.score()),
     2) a dynamic-prospective LCA score, which links LCIs to the respective background databases but without additional temporal dynamics of the biosphere flows (TimexLCA.lca.score())
     3) a dynamic-prospective LCA score with dynamic inventory and dynamic charaterization factors (not yet operational: TODO dynamic CFs in general and optional add Levasseur methodology with TH-cutoff)

    """

    def __init__(
        self,
        demand,
        method,
        database_date_dict: dict = None,
        edge_filter_function: Callable = None,
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
        *args,
        **kwargs,
    ):
        self.demand = demand
        self.method = method
        self.database_date_dict = database_date_dict
        self.edge_filter_function = edge_filter_function
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type

        if not self.database_date_dict:
            warnings.warn(
                "No database_date_dict provided. Treating the databases containing the functional unit as dynamic. No remapping to time explicit databases will be done."
            )
            self.database_date_dict = {key[0]: "dynamic" for key in demand.keys()}

        # Create static_only dict that excludes dynamic processes that will be exploded later. This way we only have the "background databases" that we can later link to from the dates of the timeline.
        self.database_date_dict_static_only = {
            k: v for k, v in self.database_date_dict.items() if type(v) == datetime
        }

        if not edge_filter_function:
            warnings.warn(
                "No edge filter function provided. Skipping all edges within background databases."
            )
            skippable = []
            for db in self.database_date_dict_static_only.keys():
                skippable.extend([node.id for node in bd.Database(db)])
            self.edge_filter_function = lambda x: x in skippable

        # Create some collections of nodes that will be useful down the line, e.g. all nodes from the background databases that link to foregroud nodes.
        self.create_node_id_collection_dict()

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
        self.biosphere_time_mapping_dict = TimeMappingDict(start_id=0)

        # Create timeline builder that does the graph traversal (similar to bw_temporalis) and extracts all edges with their temporal information. Can later be used to build a timeline with the TimelineBuilder.build_timeline() method.
        self.timeline_builder = TimelineBuilder(
            self.static_lca,
            self.edge_filter_function,
            self.database_date_dict,
            self.database_date_dict_static_only,
            self.activity_time_mapping_dict,
            self.node_id_collection_dict,
            self.temporal_grouping,
            self.interpolation_type,
            *args,
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
            ]  # datetime (or 'dynamic' for foreground processes)
            if type(time) == str:  # if 'dynamic', just add the string
                self.activity_time_mapping_dict.add((key, time), unique_id=id)
            elif type(time) == datetime:
                self.activity_time_mapping_dict.add(
                    (key, extract_date_as_integer(time, self.temporal_grouping)),
                    unique_id=id,
                )  # if datetime, map to the date as integer
            else:
                warnings.warn(f"Time of activity {key} is neither datetime nor str.")

    def create_node_id_collection_dict(self):
        """
        Creates a dict of collections of nodes that will be useful down the line, e.g. to determine static nodes for the graph traversal or create the dynamic biosphere matrix.

        Available collections are:
        - demand_database_names: set of database names of the demand processes
        - demand_dependent_database_names: set of database names of all processes that depend on the demand processes
        - demand_dependent_background_database_names: set of database names of all processes that depend on the demand processes and are in the background databases
        - demand_dependent_background_node_ids: set of node ids of all processes that depend on the demand processes and are in the background databases
        - foreground_node_ids: set of node ids of all processes that are not in the background databases
        - first_level_background_node_ids_static: set of node ids of all processes that are in the background databases and are directly linked to the demand processes
        - first_level_background_node_ids_all: like first_level_background_node_ids_static, but includes first level background processes from other time explicit databases.
        """
        self.node_id_collection_dict = {}

        # Original variable names preserved, set types for performance and uniqueness
        demand_database_names = {
            db
            for db in self.database_date_dict.keys()
            if db not in self.database_date_dict_static_only.keys()
        }
        self.node_id_collection_dict["demand_database_names"] = demand_database_names

        demand_dependent_database_names = set()
        for db in demand_database_names:
            demand_dependent_database_names.update(bd.Database(db).find_dependents())
        self.node_id_collection_dict["demand_dependent_database_names"] = (
            demand_dependent_database_names
        )

        demand_dependent_background_database_names = (
            demand_dependent_database_names & self.database_date_dict_static_only.keys()
        )
        self.node_id_collection_dict["demand_dependent_background_database_names"] = (
            demand_dependent_background_database_names
        )

        demand_dependent_background_node_ids = {
            node.id
            for db in demand_dependent_background_database_names
            for node in bd.Database(db)
        }
        self.node_id_collection_dict["demand_dependent_background_node_ids"] = (
            demand_dependent_background_node_ids
        )

        foreground_node_ids = {
            node.id
            for db in self.database_date_dict.keys()
            if db not in self.database_date_dict_static_only.keys()
            for node in bd.Database(db)
        }
        self.node_id_collection_dict["foreground_node_ids"] = foreground_node_ids

        first_level_background_node_ids_static = set()
        first_level_background_node_ids_all = set()
        for node_id in foreground_node_ids:
            node = bd.get_node(id=node_id)
            for exc in node.technosphere():
                if exc.input["database"] in self.database_date_dict_static_only.keys():
                    first_level_background_node_ids_static.add(exc.input.id)
                    for background_db in self.database_date_dict_static_only.keys():
                        try:
                            other_node = bd.get_node(
                                **{
                                    "database": background_db,
                                    "name": exc.input["name"],
                                    "product": exc.input["reference product"],
                                    "location": exc.input["location"],
                                }
                            )
                            first_level_background_node_ids_all.add(other_node.id)
                        except Exception as e:
                            warnings.warn(
                                f"Failed to find process in database {background_db} for name='{exc.input['name']}', reference product='{exc.input['reference product']}', location='{exc.input['location']}': {e}"
                            )
                            pass

        self.node_id_collection_dict["first_level_background_node_ids_static"] = (
            first_level_background_node_ids_static
        )
        self.node_id_collection_dict["first_level_background_node_ids_all"] = (
            first_level_background_node_ids_all
        )

    def build_timeline(self):
        """
        Build a timeline DataFrame of the exchanges using the TimelineBuilder class.
        """
        self.timeline = self.timeline_builder.build_timeline()
        return self.timeline[
            [
                "date_producer",
                "producer_name",
                "date_consumer",
                "consumer_name",
                "amount",
                "interpolation_weights",
            ]
        ]

    def build_datapackage(self):
        """
        Create the datapackages that contain the modifications to the technopshere and biosphere matrix using the MatrixModifier class.
        """
        # mapping of the demand id to demand time
        self.demand_timing_dict = self.create_demand_timing_dict()

        # Create matrix modifier that creates the new datapackages with the exploded processes and new links to background databases.
        self.matrix_modifier = MatrixModifier(
            self.timeline, self.database_date_dict_static_only, self.demand_timing_dict
        )
        self.node_id_collection_dict["temporal_markets"] = (
            self.matrix_modifier.temporal_market_ids
        )
        self.node_id_collection_dict["temporalized_processes"] = (
            self.matrix_modifier.temporalized_process_ids
        )
        return self.matrix_modifier.create_datapackage()

    def lci(self, build_dynamic_biosphere: Optional[bool] = True):
        """
        Calculate the time-explicit LCI.

        Building the dynamic biosphere matrix is optional. Set build_dynamic_biosphere to False if you only want to get a new overall score and don't care about the timing of the emissions. This saves time and memory.

        :param build_dynamic_biosphere: bool, if True, build the dynamic biosphere matrix and calculate the dynamic LCI. Default is True.
        """

        if not hasattr(self, "timeline"):
            warnings.warn(
                "Timeline not yet built. Call TimexLCA.build_timeline() first."
            )
            return

        self.datapackage = (
            self.build_datapackage()
        )  # this contains the matrix modifications

        self.fu, self.data_objs, self.remapping = self.prepare_timex_lca_inputs(
            demand=self.demand,
            method=self.method,
            demand_timing_dict=self.demand_timing_dict,
        )

        self.lca = LCA(
            self.fu,
            data_objs=self.data_objs
            + self.datapackage,  # here we include the datapackage
            remapping_dicts=self.remapping,
        )

        if build_dynamic_biosphere:
            self.lca.lci(factorize=True)
            self.calculate_dynamic_inventory()
            self.lca.redo_lci(
                self.fu
            )  # to get back the original LCI - necessary because we do some redo_lci's in the dynamic inventory calculation
        else:
            self.lca.lci()

    def calculate_dynamic_inventory(
        self,
    ):
        """
        Calcluates the dynamic inventory from the dynamic biosphere matrix.
        """

        if not hasattr(self, "lca"):
            warnings.warn(
                "Static Timex LCA has not been run. Call TimexLCA.lci() first."
            )
            return

        self.len_technosphere_dbs = sum(
            [len(bd.Database(db)) for db in self.database_date_dict.keys()]
        )

        self.dynamic_biosphere_builder = DynamicBiosphereBuilder(
            self.lca,
            self.activity_time_mapping_dict,
            self.biosphere_time_mapping_dict,
            self.demand_timing_dict,
            self.node_id_collection_dict,
            self.temporal_grouping,
            self.database_date_dict,
            self.database_date_dict_static_only,
            self.len_technosphere_dbs,
        )
        self.dynamic_biomatrix = (
            self.dynamic_biosphere_builder.build_dynamic_biosphere_matrix()
        )

        # Build the dynamic inventory
        count = len(self.activity_time_mapping_dict)
        diagonal_supply_array = sparse.spdiags(
            [self.dynamic_supply_array], [0], count, count
        )  # diagnolization of supply array keeps the dimension of the process, which we want to pass as additional information to the dynamic inventory dict
        self.dynamic_inventory = self.dynamic_biomatrix @ diagonal_supply_array

        self.biosphere_time_mapping_dict_reversed = {
            v: k for k, v in self.biosphere_time_mapping_dict.items()
        }

        self.activity_time_mapping_dict_reversed = {
            v: k for k, v in self.activity_time_mapping_dict.items()
        }
        self.dynamic_inventory_df = self.create_dynamic_inventory_dataframe()

    def static_lcia(self):
        """
        Static LCIA usings time-explicit LCIs with the standard static characterization factors.
        """
        if not hasattr(self, "lca"):
            warnings.warn("LCI not yet calculated. Call TimexLCA.lci() first.")
            return
        self.lca.lcia()
        self.static_score = self.lca.score

    def dynamic_lcia(
        self,
        metric: str | None = "GWP",
        time_horizon: int | None = 100,
        fixed_time_horizon: (
            bool | None
        ) = False,  # True: Levasseur approach TH for all emissions is calculated from FU, false: TH is calculated from t emission
        characterization_function_dict: dict = None,
        cumsum: bool | None = True,
    ):
        """
        Characterize the dynamic inventory dictionaries using dynamic characterization functions.

        A fixed time horizon for the impact asssessment can be set where the emission time horizon for all emissions is calculated from the functional unit (fixed_time_horizon=True) or from the time of the emission (fixed_time_horizon=False).

        Characterization functions dict of the form {biosphere_flow_database_id: characterization_function} can be given by the user. If none are given, the default dynamic characterization functions based on IPCC AR6 meant to work with biosphere3 flows are used.
        Functions from the original bw_temporalis can are compatible.

        If there is no characterization function for a biosphere flow, it will be ignored.

        Returns characterized inventory dataframe.
        """

        if not hasattr(self, "dynamic_inventory"):
            warnings.warn(
                "Dynamic lci not yet calculated. Call TimexLCA.calculate_dynamic_lci() first."
            )

        self.metric = metric
        self.time_horizon = time_horizon
        self.fixed_time_horizon = fixed_time_horizon

        self.dynamic_characterizer = DynamicCharacterization(
            self.dynamic_inventory_df,
            self.dicts.activity,
            self.dicts.biosphere,
            self.activity_time_mapping_dict_reversed,
            self.biosphere_time_mapping_dict_reversed,
            self.demand_timing_dict,
            self.temporal_grouping,
            self.method,
            characterization_function_dict,
        )

        self.characterized_inventory = (
            self.dynamic_characterizer.characterize_dynamic_inventory(
                metric,
                time_horizon,
                fixed_time_horizon,
                cumsum,
            )
        )

        self.dynamic_score = self.characterized_inventory["amount"].sum()

        return self.characterized_inventory

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
            db_label for db_label in self.database_date_dict.keys()
        ]  # Load all databases that could lateron be linked to

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

    def prepare_timex_lca_inputs(
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
            db_label for db_label in self.database_date_dict.keys()
        ]  # Load all databases that could lateron be linked to

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

    def create_dynamic_inventory_dataframe(self):
        """bring the dynamic inventory into the right format to use the characterization functions
        Format needs to be:
        | date | amount | flow | activity |
        |------|--------|------|----------|
        | 101  | 33     | 1    | 2        |
        | 102  | 32     | 1    | 2        |
        | 103  | 31     | 1    | 2        |

        date is datetime
        flow = flow id
        activity = activity id
        """

        dataframe_rows = []
        for i in range(self.dynamic_inventory.shape[0]):
            row_start = self.dynamic_inventory.indptr[i]
            row_end = self.dynamic_inventory.indptr[i + 1]
            for j in range(row_start, row_end):
                row = i
                col = self.dynamic_inventory.indices[j]
                value = self.dynamic_inventory.data[j]

                col_database_id = self.activity_dict.reversed[col]

                bioflow_node, date = self.biosphere_time_mapping_dict_reversed[
                    row
                ]  # indices are already the same as in the matrix, as we create an entirely new biosphere instead of adding new entries (like we do with the technosphere matrix)
                emitting_process_key, _ = self.activity_time_mapping_dict_reversed[
                    col_database_id
                ]

                dataframe_rows.append(
                    (
                        date,
                        value,
                        bioflow_node.id,
                        bd.get_activity(emitting_process_key).id,
                    )
                )

        df = pd.DataFrame(
            dataframe_rows, columns=["date", "amount", "flow", "activity"]
        )

        return df.sort_values(by=["date", "amount"], ascending=[True, False])

    def create_demand_timing_dict(self) -> dict:
        """
        Generate a dictionary mapping producer (key) to timing (value) for specific demands.
        It searches the timeline for those rows that contain the functional units (demand-processes as producer and -1 as consumer) and returns the time of the demand.
        Time of demand can have flexible resolution (year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH) defined in temporal_grouping.

        :param timeline: Timeline DataFrame, generated by `create_grouped_edge_dataframe`.
        :param demand: Demand dict

        :return: Dictionary mapping producer ids to reference timing for the specified demands.
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

        return df

    def create_labelled_dynamic_biosphere_dataframe(self) -> pd.DataFrame:
        """
        Returns the dynamic biosphere matrix as a dataframe with comprehensible labels instead of ids.
        """
        df = pd.DataFrame(self.dynamic_biomatrix.toarray())
        df.rename(  # from matrix id to activity id
            index=self.biosphere_time_mapping_dict_reversed,
            columns=self.dicts.activity.reversed,
            inplace=True,
        )
        df.rename(  # from activity id to ((database, code), time)
            columns=self.activity_time_mapping_dict.reversed(),
            inplace=True,
        )

        df = df.loc[(df != 0).any(axis=1)]  # For readablity, remove all-zero rows

        return df

    def plot_dynamic_inventory(self, bio_flows, cumulative=False):
        """
        Simple plot of dynamic inventory of a biosphere flow over time, with optional cumulative plotting.
        :param bio_flows: list of int, database ids of the biosphere flows to plot
        :param cumulative: bool, if True, plot cumulative amounts over time
        :return: none, but shows a plot
        """
        plt.figure(figsize=(14, 6))

        filtered_df = self.dynamic_inventory_df[
            self.dynamic_inventory_df["flow"].isin(bio_flows)
        ]
        aggregated_df = filtered_df.groupby("date").sum()["amount"].reset_index()

        if cumulative:
            aggregated_df["amount"] = np.cumsum(aggregated_df["amount"])

        plt.plot(
            aggregated_df["date"], aggregated_df["amount"], marker="o", linestyle="none"
        )

        plt.ylim(bottom=0)
        plt.xlabel("time")
        plt.ylabel("amount [kg]")
        plt.grid(True)
        plt.tight_layout()  # Adjust layout to make room for the rotated date labels
        plt.show()

    def remap_inventory_dicts(self) -> None:
        warnings.warn(
            "bw25's original mapping function doesn't work with our new time-mapped matrix entries. The Timex mapping can be found in acvitity_time_mapping_dict and biosphere_time_mapping_dict."
        )
        return

    def plot_dynamic_characterized_inventory(
        self,
        cumsum: bool = False,
        sum_emissions_within_activity: bool = False,
        sum_activities: bool = False,
    ):
        """
        Plot the characterized inventory of the dynamic LCI in a very simple plot
        """
        if cumsum:
            amount = "amount_sum"
        else:
            amount = "amount"

        if not hasattr(self, "characterized_inventory"):
            warnings.warn(
                "Characterized inventory not yet calculated. Call TimexLCA.characterize_dynamic_lci() first."
            )
            return

        # Fetch the inventory to use in plotting, modify based on flags
        plot_data = self.characterized_inventory.copy()

        if sum_emissions_within_activity:
            plot_data = plot_data.groupby(["date", "activity_name"]).sum().reset_index()
            plot_data["amount_sum"] = plot_data["amount"].cumsum()

        if sum_activities:
            plot_data = plot_data.groupby("date").sum().reset_index()
            plot_data["amount_sum"] = plot_data["amount"].cumsum()
            plot_data["activity_label"] = "All activities"

        else:
            plot_data["activity_label"] = plot_data.apply(
                lambda row: bd.get_activity(row["activity_name"])["name"], axis=1
            )
        # Plotting
        plt.figure(figsize=(14, 6))
        axes = sb.scatterplot(x="date", y=amount, hue="activity_label", data=plot_data)

        # Determine the plotting labels and titles based on the characterization method
        if self.metric == "radiative_forcing":
            label_legend = "Radiative forcing [W/m2]"
            title = "Radiative forcing"
        elif self.metric == "GWP":
            label_legend = "GWP [kg CO2-eq]"
            title = "GWP"

        if self.fixed_time_horizon:
            suptitle = f" \nTH of {self.time_horizon} years starting at FU,"
        else:
            suptitle = f" \nTH of {self.time_horizon} years starting at each emission,"

        suptitle += f" temporal resolution of inventories: {self.temporal_grouping}"

        # Determine y-axis limit flexibly
        if plot_data[amount].min() < 0:
            ymin = plot_data[amount].min() * 1.1
        else:
            ymin = 0

        axes.set_title(title)
        axes.set_axisbelow(True)
        axes.set_ylim(bottom=ymin)
        axes.set_ylabel(label_legend)
        axes.set_xlabel("Time")

        handles, labels = axes.get_legend_handles_labels()
        axes.legend(handles[::-1], labels[::-1])
        plt.title(title, fontsize=16, y=1.05)
        plt.suptitle(suptitle, fontsize=12, y=0.95)
        plt.grid()
        plt.show()

    def __getattr__(self, name):
        """
        Delegate attribute access to the self.lca object if the attribute
        is not found in the TimexLCA instance itself.
        """
        if hasattr(self.lca, name):
            return getattr(self.lca, name)
        elif hasattr(self.dynamic_biosphere_builder, name):
            return getattr(self.dynamic_biosphere_builder, name)
        else:
            raise AttributeError(
                f"'TimexLCA' object and its 'lca'- and dynamic_biosphere_builder- attributes have no attribute '{name}'"
            )
