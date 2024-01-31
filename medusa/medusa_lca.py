import bw2data as bd
import warnings
import pandas as pd

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
from .remapping import TimeMappingDict
from .utils import extract_date_as_integer


class MedusaLCA:
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
        fu, data_objs, remapping = self.prepare_static_lca_inputs(demand=self.demand, method=self.method)
        self.static_lca = LCA(
            fu, data_objs=data_objs, remapping_dicts=remapping
        )
        self.static_lca.lci()
        self.static_lca.lcia()
         
        self.time_mapping_dict = TimeMappingDict()
        # self.reversed_database_date_dict = {v: k for k, v in self.database_date_dict.items()}
        
        # Add all existing processes to the time mapping dict.
        # TODO create function that handles this
        for id in self.static_lca.dicts.activity.keys(): # activity ids
            key = self.static_lca.remapping_dicts['activity'][id] # ('database', 'code')
            time = self.database_date_dict[key[0]] # datetime (or 'dynamic' for TD'd processes)
            if type(time) == str:
                self.time_mapping_dict.add((key, time))
            elif type(time) == datetime:
                self.time_mapping_dict.add((key, extract_date_as_integer(time, self.temporal_grouping)))
            else:
                warnings.warn(f"Time of activity {key} is neither datetime nor str.")
                
        self.database_date_dict_static_only = {k: v for k, v in self.database_date_dict.items() if type(v) == datetime}
        self.tl_builder = TimelineBuilder(
            self.static_lca,
            self.edge_filter_function,
            self.database_date_dict_static_only,
            self.time_mapping_dict,
            self.temporal_grouping,
            self.interpolation_type,
            **kwargs,
        )
        

    def build_timeline(self):
        self.timeline = self.tl_builder.build_timeline()
        return self.timeline

    def build_datapackage(self):
        if not hasattr(self, "timeline"):
            warnings.warn(
                "Timeline not yet built. Call MedusaLCA.build_timeline() first."
            )
            return

        self.demand_timing_dict = self.create_demand_timing_dict()
        self.matrix_modifier = MatrixModifier(
            self.timeline, self.database_date_dict_static_only, self.demand_timing_dict
        )
        self.datapackage = self.matrix_modifier.create_datapackage()

    def lci(self):
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
        self.lca = LCA(
            self.fu, data_objs=self.data_objs + self.datapackage, remapping_dicts=self.remapping
        )
        self.lca.lci()

    def lcia(self):
        if not hasattr(self, "lca"):
            warnings.warn("LCI not yet calculated. Call MedusaLCA.lci() first.")
            return
        self.lca.lcia()

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
        """Prepare LCA input arguments in Brightway 2.5 style."""
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
                    self.time_mapping_dict[(("exploded", bd.get_node(id=bd.get_id(k))['code']), self.demand_timing_dict[bd.get_id(k)])]: v
                    for k, v in dct.items()
                }
                for dct in demands
            ]  # why?
        elif demand:
            indexed_demand = {
                self.time_mapping_dict[(("exploded", bd.get_node(id=bd.get_id(k))['code']), self.demand_timing_dict[bd.get_id(k)])]: v
                for k, v in demand.items()
            }
        else:
            indexed_demand = None

        return indexed_demand, data_objs, remapping_dicts

    def create_demand_timing_dict(self) -> dict:
        """
        Generate a dictionary mapping producer (key) to reference timing (currently YYYYMM) (value) for specific demands.
        It searches the timeline for those rows that contain the functional units (demand-processes as producer and -1 as consumer) and returns the time of the demand.

        :param timeline: Timeline DataFrame, generated by `create_grouped_edge_dataframe`.
        :param demand: Demand dict

        :return: Dictionary mapping producer ids to reference timing (currently YYYYMM) for the specified demands.
        """
        demand_ids = [bd.get_activity(key).id for key in self.demand.keys()]
        demand_rows = self.timeline[
            self.timeline["producer"].isin(demand_ids)
            & (self.timeline["consumer"] == -1)
        ]
        self.demand_timing_dict = {
            row.producer: row.hash_producer for row in demand_rows.itertuples()
        }
        return self.demand_timing_dict  # old: extract_date_as_integer(row.date)

    def __getattr__(self, name):
        """
        Delegate attribute access to the self.lca object if the attribute
        is not found in the MedusaLCA instance.
        """
        try:
            return getattr(self.lca, name)
        except AttributeError:
            print(f"'MedusaLCA' object has no attribute '{name}'")
