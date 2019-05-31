#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 05 14:54:09 2019

@author: Alex & Paul


HAMS Data API
- this class builds upon the db_query_builder and runs queries specified in json format

"""

import logging, os

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
log = logging.getLogger("hams_data_api/data_api.py")
log.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))


import pymysql
import pandas.io.sql as psql
import pandas as pd

import numpy as np

import itertools

import sys

import warnings

import re

import copy

# warnings.simplefilter(action = "ignore", category = FutureWarning)
# warnings.simplefilter(action = "ignore", category = DeprecationWarning)
warnings.simplefilter(action="ignore", category=RuntimeWarning)

from hams_data_api import db_query_builder
from hams_data_api import db_mysql


class DataAPI:
    custom_dimensions = {
        "IHC_Conv": [
            ["Initializer_Frac", "Holder_Frac", "VirtualHolder_Frac", "Closer_Frac"],
            "sum",
        ],
        "Init_Conv": [["Initializer_Frac"], "sum"],
        "Holder_Conv": [["Holder_Frac", "VirtualHolder_Frac"], "sum"],
        "Closer_Conv": [["Closer_Frac"], "sum"],
        "Adjusted_IHC_Conv": [
            [
                "Adjusted_Initializer_Frac",
                "Adjusted_Holder_Frac",
                "Adjusted_VirtualHolder_Frac",
                "Adjusted_Closer_Frac",
            ],
            "sum",
        ],
        "Adjusted_Init_Conv": [["Adjusted_Initializer_Frac"], "sum"],
        "Adjusted_Holder_Conv": [
            ["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac"],
            "sum",
        ],
        "Adjusted_Closer_Conv": [["Adjusted_Closer_Frac"], "sum"],
        "IHC_Rev": [
            [
                "Initializer_Frac",
                "Holder_Frac",
                "VirtualHolder_Frac",
                "Closer_Frac",
                "Revenue",
            ],
            "sum",
        ],
        "Init_Rev": [["Initializer_Frac", "Revenue"], "sum"],
        "Holder_Rev": [["Holder_Frac", "VirtualHolder_Frac", "Revenue"], "sum"],
        "Closer_Rev": [["Closer_Frac", "Revenue"], "sum"],
        "Adjusted_IHC_Rev": [
            [
                "Adjusted_Initializer_Frac",
                "Adjusted_Holder_Frac",
                "Adjusted_VirtualHolder_Frac",
                "Adjusted_Closer_Frac",
                "Revenue",
            ],
            "sum",
        ],
        "Adjusted_Init_Rev": [["Adjusted_Initializer_Frac", "Revenue"], "sum"],
        "Adjusted_Holder_Rev": [
            ["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac", "Revenue"],
            "sum",
        ],
        "Adjusted_Closer_Rev": [["Adjusted_Closer_Frac", "Revenue"], "sum"],
    }

    # Our Data API uses SQL Aggregation Syntax but in some cases we use Pandas Groupby Operations 
    # which have slightly different aggregation names
    sql_to_pandas_aggregations = {
        "avg": "mean",
        'count': 'count',
        "sum": "sum",
        "max": "max",
        "min": "min",
        "count_distinct": "nunique"
    }

    def __init__(self, db_details, db_credentials):
        """
            DataAPI class that takes db_details dictionary, specifying the dimensions and relations
            of the tables in all databases for a given client.
        """

        # Initialize db_credentials (without database name)
        try:
            self.db_credentials = db_credentials
        except:
            log.exception("Could not get db credentials")
            return None

        # Get necessary dictionaries from db_details
        try:
            self.database_details = db_details
        except:
            log.exception("Could not initialize database details, please check dict!")
            return None

        # Check if we can connect to the database
        try:
            utilDB = db_mysql.database_handler(
                DB_host=self.db_credentials["DB_host"],
                DB_user=self.db_credentials["DB_user"],
                DB_passwd=self.db_credentials["DB_passwd"],
                DB_db=list(self.database_details.keys())[0],
            )
            utilDB.closeConn()
            log.info("Successfully initialized DataAPI class")
        except:
            log.exception(
                "Could not create database handler, please check your credentials and if the databases in database_details exist"
            )
            return None

    def run_request(self, request_dict, sandbox_mode=False):
        """
            pass
        """

        current_request_dict = copy.deepcopy(request_dict)

        try:
            requested_database = current_request_dict["Database"]
            if requested_database == 'data_api':
                data_api_flag = True
            else:
                data_api_flag = False
        except:
            log.exception(
                "Could not find specified database in request, please check request dict"
            )
            return None

        try:
            utilDB = db_mysql.database_handler(
                DB_host=self.db_credentials["DB_host"],
                DB_user=self.db_credentials["DB_user"],
                DB_passwd=self.db_credentials["DB_passwd"],
                DB_db=requested_database,
            )
        except:
            log.exception("Could not initialize database handler")
            return None

        # check if DirectQuery is not empty
        if current_request_dict.get("DirectQuery", "") != "":
            direct_qry = current_request_dict["DirectQuery"]
            log.info("-" * 60)
            log.info("Found DirectQuery, skipping constructing query")
            log.info("Querying database now...")
            df = self.execute_query_string(utilDB, direct_qry)
            log.info("-" * 60)
            log.info("Successfully executed Query String")

            return df

        # Extract Dimensions
        try:
            current_request_dimensions = list(current_request_dict["Dimensions"])
        except:
            log.exception(
                "Could not find Dimensions (Key 'Dimensions') specified in request, please check request dict"
            )
            return None

        # Extract Metrics
        current_request_metrics = {}
        try:
            # Convert Metrics list to dictionary if type is list
            if type(current_request_dict["Metrics"]) == type([]):
                current_request_metrics = dict.fromkeys(
                    current_request_dict["Metrics"], "sum"
                )
                current_request_dict["Metrics"] = current_request_metrics
            elif type(current_request_dict["Metrics"]) == type({}):
                current_request_metrics = request_dict["Metrics"]
        except:
            current_request_dict["Metrics"] = {}

        current_request_metric_names = []
        for key, value in current_request_metrics.items():
            if type(value) == type([]):
                for val in value:
                    current_request_metric_names.append(val + '_' + key)
                continue
            else:
                current_request_metric_names.append(value + '_' + key)

        # Create list of all fields we need
        current_request_fields = current_request_dimensions + current_request_metric_names

        # Get IHC Split from request if we do not have a data api request and using default one if does not exist
        if not data_api_flag:
            try:
                ihc_split = current_request_dict["IHC_Split"]
            except:
                log.info(
                    "IHC Split not specified (Key 'IHC_Split'), using default one (1/3, 1/3, 1/3) if necessary"
                )
                ihc_split = [1 / 3, 1 / 3, 1 / 3]

        # Get database details for currently requested database
        try:
            current_database_details = self.database_details[requested_database]
        except:
            log.exception(
                "Could not find specified database: "
                + str(requested_database)
                + " in database_details dictionary (Key 'Database'), please check dict or database details"
            )
            return None

        # Initialize Query Builder using current database details
        try:
            query_builder = db_query_builder.QueryBuilder(current_database_details)
        except:
            log.exception("Could not initialize QueryBuilder using database details")
            return None

        # Handle Custom Dimensions in the request
        simplified_request_dict, custom_dimensions_mapping, get_rid_of_limit = self.prepare_request_for_custom_dimensions(
            current_request_dict, data_api_flag
        )

        # Handle Limit in the request, if we have a limit and custom metrics we need to apply it after the pandas groupby
        query_limit = None
        if simplified_request_dict.get("Limit", []) and get_rid_of_limit:
            log.info(
                "Since we have custom dimensions in our metrics we can not include the Limit in the Query itself..."
            )
            query_limit = simplified_request_dict["Limit"]
            del simplified_request_dict["Limit"]

        # Get Query String using request_dict
        try:
            query_string = self.get_query_string(query_builder, simplified_request_dict)
            if not isinstance(query_string, str):
                log.exception("Query Builder failed to create SQL string")
                return None
            log.info("Successfully received query_string from Query Builder")
        except:
            log.exception("Could not create query string")
            return None

        # Query DB if we are not in Sandbox Mode
        if not sandbox_mode:
            log.info("-" * 60)
            log.info("Querying database now...")
            df = self.execute_query_string(utilDB, query_string)
            log.info("-" * 60)
            log.info("Successfully executed Query String")

            # Get the final custom dimension columns for resulting dataframe
            df_revised = self.transform_dataframe_for_cust_dimensions(
                df, custom_dimensions_mapping, ihc_split
            )

            # Reduce dataframe to only the subset of fields we need
            try:
                df_revised = df_revised[list(np.unique(current_request_fields))]
            except:
                pass

            # Apply Groupy Operation in Pandas if necessary
            df_groupby = self.apply_groupby_operation(
                df_revised, custom_dimensions_mapping
            )

            # If there was a query limit we just subset the first n (query_limit) elements
            if query_limit != None:
                try:
                    df_groupby = df_groupby.iloc[
                        : np.min([query_limit, len(df_groupby)])
                    ]
                except:
                    pass

            df_final = self.sort_dataframe_columns(current_request_fields, df_groupby)

            return df_final

        return query_string

    def sort_dataframe_columns(self, current_request_fields, df):

        return df[current_request_fields]


    def apply_groupby_operation(self, df, custom_dimensions_mapping):
        """
            Applies Groupby operation in case our custom_dimensions_mapping contains the Groupby key

            Returns grouped and aggregated pandas DF
        """
        df_grouped = df.copy()

        if custom_dimensions_mapping.get("GroupBy", []):
            
            # Replace sql aggregation metrics with respective pandas pendant
            new_metrics_dimension_mapping = {}
            for key, value in custom_dimensions_mapping["Metrics"].items():
                try:
                    new_metrics_dimension_mapping[key] = self.sql_to_pandas_aggregations[value]
                except:
                    new_metrics_dimension_mapping[key] = value
                
            # Groupby dataframe
            df_grouped = (
                df.groupby(custom_dimensions_mapping["GroupBy"])
                .agg(new_metrics_dimension_mapping)
                .reset_index()
            )

            # Making sure column names are correct
            try:
                for key, value in custom_dimensions_mapping["Metrics"].items():
                    df_grouped = df_grouped.rename(
                        index=str, columns={key: value + "_" + key}
                    )
            except:
                pass

        return df_grouped

    def get_query_string(self, query_builder, request_dict):
        """
            pass
        """

        return query_builder.build_query(request_dict)

    def execute_query_string(self, utilDB, query_string):
        """
            Executes a given query string using a given database object.

            Returns the resulting dataframe
        """

        df = utilDB.getResultsAsDF(query_string)

        return df

    def prepare_request_for_custom_dimensions(self, request_dict, data_api_flag):
        """
            Loops over custom dimensions available in the Data API and if they are in the
            request replaces them with their respective necessary columns.

            E.g. if 'Adjusted_IHC_Conv' is in dimensions or metrics, it will be removed and replaced by:
                - Adjusted_Initializer_Frac
                - Adjusted_Holder_Frac
                - Adjusted_VirtualHolder_Frac
                - Adjusted_Closer_Frac
            From which we can ultimately build the column 'Adjusted_IHC_Conv'

            Returns the altered request dictionary, custom_dimensions_mapping containing information about the custom dimensions
            we have in our request as well as 'get_rid_of_limit' (indicating whether we could pass 'LIMIT' into our query)
        """

        request_copy = request_dict.copy()

        if data_api_flag:
            return request_copy, {}, False

        # Get lists of dimensions and metrics
        current_dimensions = request_copy["Dimensions"]
        current_metrics = list(request_copy["Metrics"].keys())

        # Initialize Custom Dimensions Mapping storing the custom dimensions we have in our request
        custom_dimensions_mapping = {}
        custom_dimensions_mapping["Dimensions"] = []
        custom_dimensions_mapping["Metrics"] = {}

        # Flag that hits when we have custom metrics in the request
        custom_metrics_flag = False

        for custom_dimension in self.custom_dimensions:

            if custom_dimension in current_dimensions:
                current_dimensions.remove(custom_dimension)
                current_dimensions += self.custom_dimensions[custom_dimension][0]
                custom_dimensions_mapping["Dimensions"].append(custom_dimension)

            if custom_dimension in current_metrics:
                current_metrics.remove(custom_dimension)
                current_metrics += self.custom_dimensions[custom_dimension][0]
                custom_metrics_flag = True

        # If we have custom metrics we put those into the dimensions and set the metrics to an empty dict
        if custom_metrics_flag:
            current_dimensions += current_metrics
            custom_dimensions_mapping["Metrics"] = request_copy["Metrics"]

            request_copy["Dimensions"] = list(np.unique(current_dimensions))
            request_copy["Metrics"] = {}

        # In case we have custom metrics we can not include the Limit and groupby in the query itself but rather do it in Pandas
        get_rid_of_limit = False
        if len(custom_dimensions_mapping["Metrics"]) > 0:
            try:
                custom_dimensions_mapping["GroupBy"] = request_copy.copy()["GroupBy"]
                request_copy["GroupBy"] = []
                get_rid_of_limit = True
            except:
                custom_dimensions_mapping["GroupBy"] = []

        return request_copy, custom_dimensions_mapping, get_rid_of_limit

    def transform_dataframe_for_cust_dimensions(
        self, df, custom_dimensions_mapping, ihc_split
    ):
        """
        For all the custom dimensions we have in our request we call the respective function to combine the necessary fields.

        Returns altered df with the final fields for all the custom dimensions
        """

        df_copy = df.copy()

        if len(custom_dimensions_mapping) == 0:
            return df_copy
        elif custom_dimensions_mapping.get("Dimensions", []):
            for dimension_remapping in custom_dimensions_mapping["Dimensions"]:
                df_copy = getattr(self, "custom_dimension_" + dimension_remapping)(
                    df_copy, ihc_split
                )
        elif custom_dimensions_mapping.get("Metrics", []):
            for dimension_remapping in custom_dimensions_mapping["Metrics"]:
                if dimension_remapping not in list(self.custom_dimensions.keys()):
                    continue
                df_copy = getattr(self, "custom_dimension_" + dimension_remapping)(
                    df_copy, ihc_split
                )

        return df_copy

    # ========================================================
    #                   Custom Dimension Functions
    # ========================================================

    def custom_dimension_Adjusted_IHC_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Initializer_Frac_Weighted"] = (
                df_copy["Adjusted_Initializer_Frac"] * ihc_split[0]
            )
            df_copy["Adjusted_Holder_Frac_Weighted"] = (
                df_copy[["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac"]].sum(
                    axis=1
                )
                * ihc_split[1]
            )
            df_copy["Adjusted_Closer_Frac_Weighted"] = (
                df_copy["Adjusted_Closer_Frac"] * ihc_split[2]
            )
            df_copy["Adjusted_IHC_Conv"] = df_copy[
                [
                    "Adjusted_Initializer_Frac_Weighted",
                    "Adjusted_Holder_Frac_Weighted",
                    "Adjusted_Closer_Frac_Weighted",
                ]
            ].sum(axis=1)

        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Init_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Init_Conv"] = (
                df_copy["Adjusted_Initializer_Frac"] * ihc_split[0]
            )

        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Holder_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Holder_Conv"] = (
                df_copy[["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac"]].sum(
                    axis=1
                )
                * ihc_split[1]
            )
        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Closer_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Closer_Conv"] = (
                df_copy["Adjusted_Closer_Frac"] * ihc_split[2]
            )

        except:
            return df

        return df_copy

    def custom_dimension_IHC_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Initializer_Frac_Weighted"] = (
                df_copy["Initializer_Frac"] * ihc_split[0]
            )
            df_copy["Holder_Frac_Weighted"] = (
                df_copy[["Holder_Frac", "VirtualHolder_Frac"]].sum(axis=1)
                * ihc_split[1]
            )
            df_copy["Closer_Frac_Weighted"] = df_copy["Closer_Frac"] * ihc_split[2]
            df_copy["IHC_Conv"] = df_copy[
                [
                    "Initializer_Frac_Weighted",
                    "Holder_Frac_Weighted",
                    "Closer_Frac_Weighted",
                ]
            ].sum(axis=1)

        except:
            return df

        return df_copy

    def custom_dimension_Init_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Init_Conv"] = df_copy["Initializer_Frac"] * ihc_split[0]
        except:
            return df

        return df_copy

    def custom_dimension_Holder_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Holder_Conv"] = (
                df_copy[["Holder_Frac", "VirtualHolder_Frac"]].sum(axis=1)
                * ihc_split[1]
            )

        except:
            return df

        return df_copy

    def custom_dimension_Closer_Conv(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Closer_Conv"] = df_copy["Closer_Frac"] * ihc_split[2]

        except:
            return df

        return df_copy

    def custom_dimension_IHC_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Initializer_Frac_Weighted"] = (
                df_copy["Initializer_Frac"] * ihc_split[0]
            )
            df_copy["Holder_Frac_Weighted"] = (
                df_copy[["Holder_Frac", "VirtualHolder_Frac"]].sum(axis=1)
                * ihc_split[1]
            )
            df_copy["Closer_Frac_Weighted"] = df_copy["Closer_Frac"] * ihc_split[2]
            df_copy["IHC_Conv"] = df_copy[
                [
                    "Initializer_Frac_Weighted",
                    "Holder_Frac_Weighted",
                    "Closer_Frac_Weighted",
                ]
            ].sum(axis=1)

            df_copy["IHC_Rev"] = df_copy["IHC_Conv"] * df_copy["Revenue"]

        except:
            return df

        return df_copy

    def custom_dimension_Init_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Init_Rev"] = (
                df_copy["Initializer_Frac"] * ihc_split[0] * df_copy["Revenue"]
            )
        except:
            return df

        return df_copy

    def custom_dimension_Holder_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Holder_Rev"] = (
                df_copy[["Holder_Frac", "VirtualHolder_Frac"]].sum(axis=1)
                * ihc_split[1]
                * df_copy["Revenue"]
            )
        except:
            return df

        return df_copy

    def custom_dimension_Closer_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Closer_Rev"] = (
                df_copy["Closer_Frac"] * ihc_split[2] * df_copy["Revenue"]
            )

        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_IHC_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Initializer_Frac_Weighted"] = (
                df_copy["Adjusted_Initializer_Frac"] * ihc_split[0]
            )
            df_copy["Adjusted_Holder_Frac_Weighted"] = (
                df_copy[["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac"]].sum(
                    axis=1
                )
                * ihc_split[1]
            )
            df_copy["Adjusted_Closer_Frac_Weighted"] = (
                df_copy["Adjusted_Closer_Frac"] * ihc_split[2]
            )
            df_copy["Adjusted_IHC_Conv"] = df_copy[
                [
                    "Adjusted_Initializer_Frac_Weighted",
                    "Adjusted_Holder_Frac_Weighted",
                    "Adjusted_Closer_Frac_Weighted",
                ]
            ].sum(axis=1)

            df_copy["Adjusted_IHC_Rev"] = (
                df_copy["Adjusted_IHC_Conv"] * df_copy["Revenue"]
            )

        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Init_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Init_Rev"] = (
                df_copy["Adjusted_Initializer_Frac"] * ihc_split[0] * df_copy["Revenue"]
            )
        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Holder_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Holder_Rev"] = (
                df_copy[["Adjusted_Holder_Frac", "Adjusted_VirtualHolder_Frac"]].sum(
                    axis=1
                )
                * ihc_split[1]
                * df_copy["Revenue"]
            )

        except:
            return df

        return df_copy

    def custom_dimension_Adjusted_Closer_Rev(self, df, ihc_split):

        df_copy = df.copy()

        try:
            df_copy["Adjusted_Closer_Rev"] = (
                df_copy["Adjusted_Closer_Frac"] * ihc_split[2] * df_copy["Revenue"]
            )
        except:
            return df

        return df_copy

    # ========================================================
    #                    Helper Functions
    # ========================================================
