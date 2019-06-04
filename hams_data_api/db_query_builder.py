#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 04 12:54:09 2019

@author: Alex & Paul


database query builder
- this class creates a query based on dictionary input

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

# warnings.simplefilter(action = "ignore", category = FutureWarning)
# warnings.simplefilter(action = "ignore", category = DeprecationWarning)
warnings.simplefilter(action="ignore", category=RuntimeWarning)

from hams_data_api.graph_class import *


class QueryBuilder:


    def __init__(self, db_details):
        """
            Query Builder class that takes db_details dictionary, specifying the dimensions and relations
            of the tables in the respective DB schema for a client.
            Also requires the graph_class.py file to be in the same directory.

        """

        self.aggregation_metric_mapping = {
            "count_distinct": "count(distinct {metric} )"
        }

        # Get necessary dictionaries from db_details
        try:
            self.tables_remapped = db_details.get("tables_remapped", {})
            self.tables_dimensions_remapped = db_details.get(
                "tables_dimensions_remapped", {}
            )
            self.tables_dimensions = db_details.get("tables_dimensions", {})
            self.table_connections = db_details.get("table_connections", {})
            self.dimension_priorities = db_details.get("dimension_priorities", {})
            self.table_prefix = db_details.get("table_prefix", {})

            # Construct "plain" table connections from table_connections dict (necessary for Graph class)
            self.table_connections_plain = {
                key: list(self.table_connections[key].keys())
                for key in self.table_connections
            }

        except:
            log.exception("Could not parse db_details, please check file")

        # Create Graph Instance and initialize with table_connections_plain
        try:
            self.graph = Graph(self.table_connections_plain)
            log.info("QueryBuilder class successfully initialized")
        except:
            log.exception(
                "Could not initialize QueryBuilder class due to issue with graph_class or connection dictionary"
            )

    def build_query(self, request_dict):
        """
            Query Builder Function

            Input: dictionary with following keys:
                - Dimensions - list. List of dimensions (i.e. fields) to select
                - Metrics - list / dictionary. When list, we assume that all
                    aggregators are SUM. When dictionary, aggregators have to
                    be specified. E.g. {"Initializer_Frac": "max"}
                - FilterClause - list of dictionaries, where each dictionary resembles one FilterBlock
                    - FilterBlock - dictionary, with following keys:
                        - operator: str, MySQL logical operator (AND, OR)
                        - filters: list of dictionaries, where each dictinary is:
                            - dimension: str, Name of Dimension / Metric
                            - value: str / list, value(-s) to be evaluated
                            - rule: MySQL logical operator (>, <>, =, <, BETWEEN, etc.)

            Output:
                str - MySQL statement

        """

        # if the request contains an mysql query, just return that
        if request_dict.get("mysql_qry"):

            return request_dict["mysql_qry"]

        # Extract Dimensions, Metrics and Filter Dimensions from request
        try:
            dimensions, metrics, filter_dimensions = self.extract_full_fields(
                request_dict
            )
            metrics_fields = list(metrics.keys())
            full_fields_list = dimensions + metrics_fields + filter_dimensions
        except:
            log.exception("Could not extract fields from request, please check!")
            return None

        # Get Request Path and necessary tables for the required dimensions
        try:
            dimensions_final, metrics_final, necessary_tables = self.get_request_path_and_tables(
                dimensions, metrics, full_fields_list
            )
        except:
            log.exception("Failed to get request path and tables!")
            return None

        ### Section to build the actual query string
        result_qry = ""

        # Adding SELECT string
        select_str = self.create_select_statement(dimensions_final, metrics_final)
        result_qry += select_str

        # Adding FROM string
        from_str = self.create_from_statement(necessary_tables)
        result_qry += from_str

        # (If Available) Adding WHERE string
        if request_dict.get("FilterClauses"):

            where_str = self.create_where_statement(request_dict["FilterClauses"])
            result_qry += where_str

        # (If Available) Adding GROUP BY string
        if request_dict.get("GroupBy"):
            groupby_str = self.create_groupby_statement(request_dict["GroupBy"])
            result_qry += groupby_str

        # (If Available) Adding LIMIT string
        if request_dict.get("Limit"):
            limit_str = self.create_limit_statemet(request_dict["Limit"])
            result_qry += limit_str

        return result_qry

    # ========================================================
    #                    Helper Functions
    # ========================================================

    def add_prefix_to_dimension(self, dimension, table_prefix):

        if "{prefix}" in dimension:
            dimension_with_prefix = re.sub(
                pattern="{prefix}", repl=table_prefix, string=dimension
            )
        else:
            dimension_with_prefix = table_prefix + "." + dimension

        return dimension_with_prefix

    def get_dimension_table_prefix(self, dimension, where_statement = False):
        """
            Function to get table prefix and (potentially remapped) dimension
            for a given dimension. Requires the dimension to be in one of the
            necessary tables.

            Returns [table prefix, (remapped) dimension] (list)
        """

        if where_statement:

            result_list = []

            new_dimension = dimension
            dimension_table = ""
            for key, value in self.tables_dimensions_remapped.items():
                if (dimension in value) and (key in self.necessary_tables):
                    new_dimension = str.split(value[dimension], " AS ")[0]
                    dimension_table = key
                    result_list.append([self.table_prefix[dimension_table], new_dimension])

            if dimension_table == "":
                for table in self.necessary_tables:
                    if dimension in self.tables_dimensions[table]:
                        dimension_table = table
                        result_list.append([self.table_prefix[dimension_table], new_dimension])
                        # break

            if dimension_table not in self.necessary_tables:
                log.exception("Condition not in tables, please check request!")
                return None
            else:
                return result_list

        else:
            new_dimension = dimension
            dimension_table = ""
            for key, value in self.tables_dimensions_remapped.items():
                if (dimension in value) and (key in self.necessary_tables):
                    new_dimension = str.split(value[dimension], " AS ")[0]
                    dimension_table = key

            if dimension_table == "":
                for table in self.necessary_tables:
                    if dimension in self.tables_dimensions[table]:
                        dimension_table = table
                        break

            if dimension_table not in self.necessary_tables:
                log.exception("Condition not in tables, please check request!")
                return None
            else:
                return [self.table_prefix[dimension_table], new_dimension]

    def remap_table(self, table_name):
        """
            Function to remap table name from tables_remapped dict

            Returns remapped table name (if applicable)
        """
        new_table_name = self.tables_remapped.get(table_name, table_name)

        return new_table_name

    def remap_metric_special_case(self, table_name, metric_name):

        tbl_remap = self.tables_dimensions_remapped.get(table_name, None)

        if tbl_remap is None:
            return metric_name
        else:
            new_metdim_name = tbl_remap.get(metric_name, metric_name)
            # print(new_metdim_name)
            # print(str.split(new_metdim_name, " AS "))
            # new_metdim_name = str.split(new_metdim_name, " AS ")[0]


        return new_metdim_name


    def remap_metric_dimension(self, table_name, dim_met_name):
        """
            Function to remap metric/dimension from tables_dimensions_remapped dict

            Returns remapped metric (if applicable)
        """

        tbl_remap = self.tables_dimensions_remapped.get(table_name, None)

        if tbl_remap is None:
            return dim_met_name
        else:
            new_metdim_name = tbl_remap.get(dim_met_name, dim_met_name)

        return new_metdim_name

    # ========================================================
    #  Functions extracting request details and finding path
    # ========================================================

    def extract_full_fields(self, request_dict):
        """
            Function that takes the request dictionary and parses the fields from
            "Dimensions", "Metrics" and "FilterClauses".
            Returns the extracted Dimensions (list), Metrics (dict) and filter_dimensions (list)
        """

        # Extract Dimensions
        if request_dict.get("Dimensions"):
            dimensions = request_dict["Dimensions"]

        else:
            log.exception("Dimensions Field does not exist, please check request")
            return None

        # Extract Metrics
        metrics = {}
        metrics_fields = []
        try:
            if type(request_dict["Metrics"]) == type([]):
                metrics = dict.fromkeys(request_dict["Metrics"], "sum")
            elif type(request_dict["Metrics"]) == type({}):
                metrics = request_dict["Metrics"]
        except:
            pass

        # If the request includes filter clauses we also need to have those dimensions
        filter_dimensions = []
        try:
            if request_dict.get("FilterClauses"):
                filter_dimensions = self.extract_dimensions_filters(
                    request_dict["FilterClauses"]
                )
        except:
            pass

        # Remove those dimensions that are already in metrics
        dimensions = [
            dimension
            for dimension in dimensions
            if dimension not in list(metrics.keys())
        ]

        return dimensions, metrics, filter_dimensions

    @staticmethod
    def extract_dimensions_filters(filter_clause):
        """
            Function to extract dimensions occurring in the FilterClause of the Request.

        """
        filter_dimensions = []
        try:
            for filter in filter_clause:
                filter_dimensions.append(filter["filters"][0]["dimension"])
        except:
            log.exception(
                "Could not extract filter dimensions from request, please make sure the format is correct!"
            )
            return None
        return filter_dimensions

    def get_request_path_and_tables(self, dimensions, metrics, full_fields_list):
        """
            Function to get the necessary tables in the DB, calculate scores for each combination
            and chooses the best one based on Score_Completeness and Score_N_Tables. In the end finds
            the path between all necessary tables and returns the final dimensions, metrics and necessary tables
            (including their table prefix).
        """

        # Get tables where we can find the dimensions
        needed_fields_and_tables = self.get_field_tables(full_fields_list)

        if needed_fields_and_tables == None:
            return None

        # Get combination of those tables (i.e. paths to connect them)
        table_combinations = self.get_table_combinations(needed_fields_and_tables)

        # Calculate score of each of the combinations
        df_combination_score = self.calc_combination_score(table_combinations)

        # Choose the best combination
        df_final_combination = self.choose_combination(df_combination_score)

        # Find the necessary tables for the best combination
        necessary_tables = self.find_necessary_tables(df_final_combination)
        self.necessary_tables = necessary_tables

        # Create subset of df_final_combination with only dimensions and metrics
        df_final_dimensions = dict(
            (k, df_final_combination[0][k])
            for k in dimensions
            if k in df_final_combination[0]
        )
        df_final_metrics = dict(
            (k, df_final_combination[0][k])
            for k in metrics
            if k in df_final_combination[0]
        )

        ## Add the aggregators back to df_final_metrics
        df_final_metrics_with_agg = {
            key: [df_final_metrics[key], metrics[key]] for key in df_final_metrics
        }

        df_final_dimensions_remapped = [
            {
                self.remap_metric_dimension(
                    df_final_dimensions[key], key
                ): df_final_dimensions[key]
                for key in df_final_dimensions
            }
        ]

        df_final_metrics_remapped = [
            {
                self.remap_metric_special_case(
                    df_final_metrics[key], key
                ): df_final_metrics_with_agg[key]
                for key in df_final_metrics_with_agg
            }
        ]


        # Get lists of metrics and dimensions (after remapping)
        metrics_new = list(df_final_metrics_remapped[0].keys())
        dimensions_new = list(df_final_dimensions_remapped[0].keys())

        # Get list of final dimensions including the table prefix
        dimensions_final = [
            self.table_prefix[df_final_dimensions_remapped[0][dimension]]
            + "."
            + str(dimension)
            for dimension in dimensions_new
        ]

        dimensions_final = [
            self.add_prefix_to_dimension(
                dimension, self.table_prefix[df_final_dimensions_remapped[0][dimension]]
            )
            for dimension in dimensions_new
        ]

        # Split the metrics into those that need remapping and those that do not
        metrics_need_remapping = [x for x in metrics_new if "AS" in x]
        metrics_not_need_remapping = [x for x in metrics_new if not "AS" in x]

        metrics_final_remapping = {
            str.split(metric, " AS ")[1]: [
                self.add_prefix_to_dimension(str.split(metric, " AS ")[0], self.table_prefix[df_final_metrics_remapped[0][metric][0]]),
                df_final_metrics_remapped[0][metric][1],
            ]
            for metric in metrics_need_remapping
        }

        metrics_final_not_remapping = {
            str(metric): [
                self.add_prefix_to_dimension(str.split(metric, " AS ")[0], self.table_prefix[df_final_metrics_remapped[0][metric][0]]),
                df_final_metrics_remapped[0][metric][1],
            ]
            for metric in metrics_not_need_remapping
        }

        metrics_final = {**metrics_final_remapping, **metrics_final_not_remapping}

        return dimensions_final, metrics_final, necessary_tables

    def get_field_tables(self, dimensions):
        """
            Looks for requested fields in tables_dimensions and returns dict with key
            being the field and the value a list of tables where the dimension appears in.
        """

        dimensions_mapping = {}
        unavailable_dimensions = []

        for dimension in dimensions:
            dimensions_mapping[dimension] = []
            for key, value in self.tables_dimensions.items():
                if dimension in value:
                    dimensions_mapping[dimension].append(key)
            for key, value in self.tables_dimensions_remapped.items():
                if dimension in value:
                    dimensions_mapping[dimension].append(key)
            if len(dimensions_mapping[dimension]) == 0:
                unavailable_dimensions.append(dimension)

        if len(unavailable_dimensions) > 0:
            log.exception(
                "Could not find following dimensions: "
                + str(unavailable_dimensions)
                + ", please make sure dimensions exists"
            )
            return None

        return dimensions_mapping

    def get_table_combinations(self, dimension_dict):
        """
            Takes dimension mapping and creates pd.DataFrame with one row for each combination of the tables
        """
        dict_values = []

        for key in sorted(dimension_dict):
            dict_values.append(dimension_dict[key])
        df_combinations = pd.DataFrame(
            data=list(itertools.product(*dict_values)),
            columns=list(sorted(dimension_dict.keys())),
        )

        return df_combinations

    def calc_combination_score(self, df_combinations):
        """
            Calculates Completeness and N_Tables Score for all combinations.

            Input:
                - pd.DataFrame with one row for all combinations of tables

            Output:
                - pd.DataFrame with the addition of two columns:
                    - "Score_Completeness" (score based on dimension_priorities dict)
                    - "Score_N_Tables" (score based on number of tables necessary)
        """

        scores_completeness = []
        scores_n_tables = []

        for index, row in df_combinations.iterrows():

            # Append scores_n_tables with number of unique tables appearing in current row
            scores_n_tables.append(len(np.unique(list(row))))
            current_row_dict = row.to_dict()

            score_completeness = 0

            for key, value in current_row_dict.items():
                try:
                    try:
                        current_score = self.dimension_priorities[key][value]
                    except:
                        current_score = 1000
                    score_completeness += current_score
                except:
                    continue

            scores_completeness.append(score_completeness)

        df_combinations["Score_Completeness"] = scores_completeness
        df_combinations["Score_N_Tables"] = scores_n_tables

        return df_combinations

    def choose_combination(self, df_combinations):
        """
            Chooses combination based on minimum Score_Completeness and then on Score_N_Tables

            Returns chosen combination row as dict
        """

        # Subset of df_combinations with minimum Score_Completeness
        df_completeness = df_combinations[
            df_combinations.Score_Completeness
            == min(df_combinations.Score_Completeness)
        ]

        # Take first row of df_completeness with minimum Score_N_Tables
        df_final = df_completeness[
            df_completeness.Score_N_Tables == min(df_completeness.Score_N_Tables)
        ].iloc[0:1]

        return df_final.drop(["Score_Completeness", "Score_N_Tables"], axis=1).to_dict(
            orient="records"
        )

    def find_necessary_tables(self, df_dimension_table_dict):
        """
            Finds necessary_tables by searching for all necessary paths between tables.

            Returns list of unique tables needed
        """

        unique_tables = np.unique(list(df_dimension_table_dict[0].values()))

        if len(unique_tables) == 1:
            return unique_tables

        paths = list(itertools.combinations(unique_tables, 2))

        path_list = []
        for path in paths:
            p = self.graph.find_shortest_path(path[0], path[1])
            if p is None:
                log.exception("Could not find path between two tables, seems fishy")
                p = []
            path_list.append(p)

        flat_paths_list = [item for sublist in path_list for item in sublist]

        return list(np.unique(flat_paths_list))

    # ========================================================
    #               SQL String Creation Functions
    # ========================================================

    def create_select_statement(self, dimensions, metrics):
        """
            Function to create Select statement based on dimensions and metrics
        """

        select_str = "SELECT "

        if len(metrics) > 0:
            select_str += ", ".join(dimensions) + ", "

        else:
            select_str += ", ".join(dimensions) + " "

        select_str += self.create_select_metrics(metrics)

        return select_str

    def create_select_metrics(self, metrics_dict):
        """
            Function to add the metrics to the select statement (including their aggregators)
        """

        measures_str = ","

        measures_list = []
        for metric, aggregator_list in metrics_dict.items():

            if type(aggregator_list[1]) == type([]):

                for aggregator in aggregator_list[1]:

                    if aggregator in self.aggregation_metric_mapping.keys():
                
                        measures_list += [
                            self.aggregation_metric_mapping[aggregator].format(metric = aggregator_list[0])
                            + " as "
                            + str(aggregator)
                            + "_"
                            + metric
                        ]
                        continue

                    measures_list += [
                        aggregator
                        + "("
                        + aggregator_list[0]
                        + ") as "
                        + str(aggregator)
                        + "_"
                        + metric
                    ]
            else:

                if aggregator_list[1] in self.aggregation_metric_mapping.keys():
                    
                    measures_list += [
                        self.aggregation_metric_mapping[aggregator_list[1]].format(metric = aggregator_list[0])
                        + " as "
                        + str(aggregator_list[1])
                        + "_"
                        + metric
                    ]
                    continue

                measures_list += [
                    aggregator_list[1]
                    + "("
                    + aggregator_list[0]
                    + ") as "
                    + str(aggregator_list[1])
                    + "_"
                    + metric
                ]
        
        measures_str = ", ".join(measures_list) + "  "

        return measures_str

    def join_tables_recursive(self, tables_list):
        """
            workaround with the issue when tables were joined before assignment
            it seems like a recursive method would be best, but I can't figure
            it out (@Alex)
        """
        inner_joins_list = []
        tables_joined = [tables_list[0]]
        tables_links_all = set(self.table_connections[tables_list[0]].keys())

        for i in range(0, len(tables_list)):
            if (tables_list[i] not in tables_links_all) & (
                tables_list[i] not in tables_joined
            ):
                continue
            inn_join = self.table_connections[tables_list[i]]
            tables_links_all = tables_links_all | set(inn_join)

            for table_conn_2, dimension in inn_join.items():
                if table_conn_2 not in tables_list:
                    continue

                if table_conn_2 in tables_joined:
                    continue

                dim_link_list = []
                for link_left, link_right in dimension.items():

                    link_left_prefix = self.add_prefix_to_dimension(
                        self.remap_metric_dimension(tables_list[i], link_left),
                        self.table_prefix[tables_list[i]],
                    )
                    link_right_prefix = self.add_prefix_to_dimension(
                        self.remap_metric_dimension(table_conn_2, link_right),
                        self.table_prefix[table_conn_2],
                    )
                    dim_link_list += [link_left_prefix + "=" + link_right_prefix]
                dimension_link = " AND ".join(dim_link_list)
                inner_joins_list += [
                    "INNER JOIN "
                    + self.remap_table(table_conn_2)
                    + " AS "
                    + self.table_prefix[table_conn_2]
                    + " ON "
                    + dimension_link
                ]

                tables_joined += [table_conn_2]

        return inner_joins_list

    def create_from_statement(self, tables_list):
        """
            Function to create the SQL FROM statement by taking a list of tables as input

            Returns:
                - from_str (str)
        """

        from_str = (
            "\nFROM "
            + self.remap_table(tables_list[0])
            + " AS "
            + self.table_prefix[tables_list[0]]
            + "\n"
        )
        inner_joins_list = self.join_tables_recursive(tables_list)

        if len(inner_joins_list) > 0:
            from_str += "\n".join(inner_joins_list) + "\n"

        return from_str

    def create_where_statement(self, filter_clause):
        """
            Function to transfer filter clause into mysql where statement
        """
        where_str = "WHERE "
        for filt_block in filter_clause:
            where_str += "("
            where_cond_list = []
            for cond in filt_block["filters"]:
                where_cond_list += [self.create_where_condition(cond)]
            where_str += (
                " OR ".join(where_cond_list) + ") " + str(filt_block["operator"] + " ")
            )

        where_str = where_str[:-4]
        where_str += "\n"

        return where_str

    def create_where_condition(self, cond):
        """
            Function to add individual where conditions to where_str.

            Function has special functionality for "BETWEEN" rule.

            Returns:
                - where_str (str)
        """

        results_list = self.get_dimension_table_prefix(cond["dimension"], where_statement=True)

        where_str_orig = ""

        counter = 0
        for where_statement in results_list:

            if counter > 0:
                where_str_orig += ") AND ("

            condition = where_statement[1]
            table_prefix = where_statement[0]
            
            condition_with_prefix = self.add_prefix_to_dimension(condition, table_prefix)

            if cond["rule"].lower() == "between":
                where_str = (
                    condition_with_prefix
                    + " BETWEEN '"
                    + '" AND "'.join(cond["value"])
                    + "'"
                )
                where_str = where_str.replace('"', "'")
            elif (cond["rule"].lower() == "is not null") or (
                cond["rule"].lower() == "is null"
            ):
                where_str = condition_with_prefix + " " + cond["rule"]
            else:
                where_str = (
                    condition_with_prefix
                    + " "
                    + cond["rule"]
                    + " '"
                    + str(cond["value"])
                    + "'"
                )

            counter += 1

            where_str_orig += where_str

        return where_str_orig

    def create_groupby_statement(self, groupby_clause):
        """
            Function to add GROUP BY statement to SQL string

            Returns:
                - groupby_str (str)
        """
        groupby_str = "GROUP BY"
        for groupby_field in groupby_clause:
            groupby_prefix, groupby_field_new = self.get_dimension_table_prefix(
                groupby_field
            )
            groupby_str += (
                " "
                + self.add_prefix_to_dimension(groupby_field_new, groupby_prefix)
                + ","
            )

        groupby_str = groupby_str[:-1]
        groupby_str += "\n"

        return groupby_str

    def create_limit_statemet(self, limit_n):
        """
            Function to add LIMIT statement to SQL string

            Returns:
                - groupby_str (str)
        """

        limit_str = "LIMIT %s" % limit_n

        return limit_str
