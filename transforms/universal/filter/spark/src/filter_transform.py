# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import ast
import json
from argparse import ArgumentParser, Namespace
from typing import Any

from data_processing.transform import TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing_spark.runtime.spark.runtime_config import (
    SparkTransformRuntimeConfiguration,
)
from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform
from pyspark.sql import DataFrame


logger = get_logger(__name__)


short_name = "filter"
cli_prefix = short_name + "_"

filter_criteria_key = "criteria_list"
""" AST Key holds the list of filter criteria (in SQL WHERE clause format)"""
filter_logical_operator_key = "logical_operator"
""" Key holds the logical operator that joins filter criteria (AND or OR)"""
filter_columns_to_drop_key = "columns_to_drop"
""" AST Key holds the list of columns to drop after filtering"""

filter_criteria_cli_param = f"{cli_prefix}{filter_criteria_key}"
""" AST Key holds the list of filter criteria (in SQL WHERE clause format)"""
filter_logical_operator_cli_param = f"{cli_prefix}{filter_logical_operator_key}"
""" Key holds the logical operator that joins filter criteria (AND or OR)"""
filter_columns_to_drop_cli_param = f"{cli_prefix}{filter_columns_to_drop_key}"
""" AST Key holds the list of columns to drop after filtering"""

captured_arg_keys = [filter_criteria_key, filter_columns_to_drop_key, filter_logical_operator_key]
""" The set of keys captured from the command line """

# defaults
filter_criteria_default = ast.literal_eval("[]")
""" The default list of filter criteria (in SQL WHERE clause format)"""
filter_logical_operator_default = "AND"
filter_columns_to_drop_default = ast.literal_eval("[]")
""" The default list of columns to drop"""


class FilterTransform(AbstractSparkTransform):
    """
    Implements Spark filtering - select from a dataset a set of rows that
    satisfy a set of filtering criteria
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, FilterTransformRuntime.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of FilterTransformConfiguration class
        super().__init__(config)
        self.filter_criteria = config.get(filter_criteria_key, filter_criteria_default)
        self.logical_operator = config.get(filter_logical_operator_key, filter_logical_operator_default)
        self.columns_to_drop = config.get(filter_columns_to_drop_key, filter_columns_to_drop_default)

    def transform(self, data: DataFrame) -> tuple[list[DataFrame], dict[str, Any]]:
        """
        This implementation filters the input Spark dataframe using a SQL
        statement and returns the filtered table and execution stats
        :param data: input Spark DataFrame
        :return: list of output Spark DataFrames and custom statistics
        """
        # initialize the metadata dictionary
        total_docs = data.count()
        total_columns = len(data.columns)
        metadata = {
            "total_docs_count": total_docs,
            "total_columns_count": total_columns,
        }

        data.createOrReplaceTempView("spark_table")

        if len(self.filter_criteria) > 0:
            # populate metadata with filtering stats for each filter criterion
            for filter_criterion in self.filter_criteria:
                query = f"SELECT * FROM spark_table WHERE {filter_criterion}"
                filter_df = data.sparkSession.sql(query)
                # filter_df = data.where(filter_criterion)
                docs_filtered = total_docs - filter_df.count()
                metadata[f"docs_filtered_out_by '{filter_criterion}'"] = docs_filtered

            # use filtering criteria to build the SQL query for filtering
            filter_clauses = [f"({x})" for x in self.filter_criteria]
            where_clause = f" {self.logical_operator} ".join(filter_clauses)

            # filter using 'where' function
            try:
                query = f"SELECT * FROM spark_table WHERE {where_clause}"
                filtered_df = data.sparkSession.sql(query)
                # filtered_df = data.where(where_clause)
            except Exception as ex:
                logger.error(f"FilterTransform::transform failed: {ex}")
                raise ex
        else:
            filtered_df = data

        data.sparkSession.catalog.dropTempView("spark_table")

        # drop any columns requested from the final result
        if len(self.columns_to_drop) > 0:
            columns_to_drop = tuple(self.columns_to_drop)
            filtered_df_cols_dropped = filtered_df.drop(*columns_to_drop)
        else:
            filtered_df_cols_dropped = filtered_df

        # add global filter stats to metadata
        metadata["docs_after_filter"] = filtered_df_cols_dropped.count()
        metadata["columns_after_filter"] = len(filtered_df_cols_dropped.columns)

        return [filtered_df_cols_dropped], metadata


class FilterTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=FilterTransform,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the FilterTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """

        sample_sql = [
            "docq_total_words > 100 AND docq_total_words < 200",
            "docq_perplex_score < 230",
            "date_acquired BETWEEN '2023-07-04' AND '2023-07-08'",
            "title LIKE 'https://%%'",
            "document_id IN ('doc-id-1', 'doc-id-2', 'doc-id-3')",
        ]
        columns_to_drop_example = ["column1", "column2"]

        parser.add_argument(
            f"--{filter_criteria_cli_param}",
            type=ast.literal_eval,
            required=True,
            default=ast.literal_eval("[]"),
            help=f"list of filter criteria (in SQL WHERE clause format), for example: {json.dumps(sample_sql, indent=2, default=str)}",
        )
        parser.add_argument(
            f"--{filter_columns_to_drop_cli_param}",
            type=ast.literal_eval,
            required=False,
            default=ast.literal_eval("[]"),
            help=f"list of columns to drop after filtering, for example: {json.dumps(columns_to_drop_example)}",
        )
        parser.add_argument(
            f"--{filter_logical_operator_cli_param}",
            type=str,
            required=False,
            default="AND",
            choices=["AND", "OR"],
            help="logical operator (AND or OR) that joins filter criteria",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True


class FilterSparkRuntimeConfiguration(SparkTransformRuntimeConfiguration):
    """
    Implements the SparkTransformConfiguration for Filter as required by the
    SparkTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=FilterTransformConfiguration())


if __name__ == "__main__":
    launcher = SparkTransformLauncher(FilterSparkRuntimeConfiguration())
    logger.info("Launching filter transform")
    launcher.launch()
