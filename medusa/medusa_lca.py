import bw2data as bd
import warnings
import pandas as pd
import numpy as np

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


class MedusaLCA:
    def __init__(
        self,
        slca: LCA,  # Not sure if this is correct
        edge_filter_function: Callable,
        database_date_dict: dict,
        temporal_grouping: str = "year",
        interpolation_type: str = "linear",
    ):
        self.slca = slca
        self.edge_filter_function = edge_filter_function
        self.database_date_dict = database_date_dict
        self.temporal_grouping = temporal_grouping
        self.interpolation_type = interpolation_type

        self.tl_builder = TimelineBuilder(
            self.slca,
            self.edge_filter_function,
            self.database_date_dict,
            self.temporal_grouping,
            self.interpolation_type,
        )

        self.dynamic_lci = {}  # dictionary to store the dynamic lci {CO2: {time: [2022, 2023], amount:[3,5]}}


    def build_timeline(self):
        self.timeline = self.tl_builder.build_timeline()
        return self.timeline

    def build_datapackage(self):
        if not hasattr(self, "timeline"):
            warnings.warn(
                "Timeline not yet built. Call MedusaLCA.build_timeline() first."
            )
            return

        self.create_demand_timing_dict()
        self.matrix_modifier = MatrixModifier(
            self.timeline, self.database_date_dict, self.demand_timing_dict
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

        fu, data_objs, remapping = self.prepare_medusa_lca_inputs(
            demand=self.slca.demand,
            method=self.slca.method,
            demand_timing_dict=self.demand_timing_dict,
        )
        self.lca = LCA(
            fu, data_objs=data_objs + self.datapackage, remapping_dicts=remapping
        )
        self.lca.lci()

    def lcia(self):
        if not hasattr(self, "lca"):
            warnings.warn("LCI not yet calculated. Call MedusaLCA.lci() first.")
            return
        self.lca.lcia()

    def build_dynamic_biosphere(self):
        if not hasattr(self, "lca"):
            warnings.warn(
                "Static Medusa LCA has not been run. Call MedusaLCA.lci() first."
            )
            return
        self.dynamic_biosphere_builder = DynamicBiosphere(self.lca.biosphere_matrix,
                                                            self.lca.supply_array,
                                                            self.timeline,
                                                            self.database_date_dict
                                                            )
        self.dynamic_biosphere_builder.build_dynamic_biosphere_matrix()
        self.dynamic_biomatrix = self.dynamic_biosphere_builder.dynamic_biomatrix  # FIXME: how to return dynamc_biomatrx from class method build_biomatrix()

    def calculate_dynamic_lci(
            self,
            ):
        if not hasattr(self, "dynamic_biomatrix"):
            warnings.warn(
                "dynamic biosphere matrix not yet built. Call MedusaLCA.build_dynamic_biosphere() first."
            )
        len_background = self.biosphere_matrix.shape[1]-self.dynamic_biomatrix.shape[1]  # dirty fix to exclude the background
        # calculate lci from dynamic biosphere matrix
        unordered_lci = self.dynamic_biomatrix.dot(self.supply_array[len_background:])  # FIXME: include background processes

        # Create dynamic lci dictionary with structure {CO2: {time: [2022, 2023], amount:[3,5]}, CH4: {time: [2022, 2023], amount:[3,5]}, ...}
        for ((flow, time), amount) in zip(self.dynamic_biosphere_builder.bio_row_mapping.__reversed__(), unordered_lci):
            print(flow['code'],time,amount)
            if not flow['code'] in self.dynamic_lci.keys():
                self.dynamic_lci[flow['code']] = {'time' : [], 'amount' : []}
            self.dynamic_lci[flow['code']]['time'].append(time)
            self.dynamic_lci[flow['code']]['amount'].append(amount)
        # now sort flows based on time
        for flow in self.dynamic_lci.keys():
            order = np.argsort(self.dynamic_lci[flow]['time'])
            self.dynamic_lci[flow]['time'] = np.array(self.dynamic_lci[flow]['time'])[order]
            self.dynamic_lci[flow]['amount'] = np.array(self.dynamic_lci[flow]['amount'])[order]



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
                    get_id(k) * 1000000 + demand_timing_dict[get_id(k)]: v
                    for k, v in dct.items()
                }
                for dct in demands
            ]  # why?
        elif demand:
            indexed_demand = {
                get_id(k) * 1000000 + demand_timing_dict[get_id(k)]: v
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
        demand_ids = [bd.get_activity(key).id for key in self.slca.demand.keys()]
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
