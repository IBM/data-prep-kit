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

import argparse
import ast
import json

import duckdb
import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider


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

captured_arg_keys = [filter_criteria_key, filter_columns_to_drop_key]
""" The set of keys captured from the command line """

# defaults
filter_criteria_default = ast.literal_eval("[]")
""" The default list of filter criteria (in SQL WHERE clause format)"""
filter_logical_operator_default = "AND"
filter_columns_to_drop_default = ast.literal_eval("[]")
""" The default list of columns to drop"""


class FilterTransform(AbstractTableTransform):
    """
    Implements filtering - select from a pyarrow.Table a set of rows that
    satisfy a set of filtering criteria
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, FilterTransformRuntime.  If running from the Ray orchestrator,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        super().__init__(config)
        self.filter_criteria = config.get(filter_criteria_key, filter_criteria_default)
        self.logical_operator = config.get(filter_logical_operator_key, filter_logical_operator_default)
        self.columns_to_drop = config.get(filter_columns_to_drop_key, filter_columns_to_drop_default)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict]:
        """
        This implementation filters the input table using a SQL statement and
        returns the filtered table and execution stats
        :param table: input table
        :return: list of output tables and custom statistics
        """

        # move table under a different name, to avoid SQL query parsing error
        input_table = table
        total_docs = input_table.num_rows
        total_columns = input_table.num_columns
        total_bytes = input_table.nbytes

        # initialize the metadata dictionary
        metadata = {
            "total_docs_count": total_docs,
            "total_bytes_count": total_bytes,
            "total_columns_count": total_columns,
        }

        # initialize the SQL statement used for filtering
        sql_statement = "SELECT * FROM input_table"
        if len(self.filter_criteria) > 0:
            # populate metadata with filtering stats for each filter criterion
            for filter_criterion in self.filter_criteria:
                criterion_sql = f"{sql_statement} WHERE {filter_criterion}"
                filter_table = duckdb.execute(criterion_sql).arrow()
                docs_filtered = total_docs - filter_table.num_rows
                bytes_filtered = total_bytes - filter_table.nbytes
                metadata[f"docs_filtered_out_by '{filter_criterion}'"] = docs_filtered
                metadata[f"bytes_filtered_out_by '{filter_criterion}'"] = bytes_filtered

            # use filtering criteria to build the SQL query for filtering
            filter_clauses = [f"({x})" for x in self.filter_criteria]
            where_clause = f" {self.logical_operator} ".join(filter_clauses)
            sql_statement = f"{sql_statement} WHERE {where_clause}"

            # filter using SQL statement
            try:
                filtered_table = duckdb.execute(sql_statement).arrow()
            except Exception as ex:
                self.logger.error(f"FilterTransform::transform failed: {ex}")
                raise ex
        else:
            filtered_table = table

        # drop any columns requested from the final result
        if len(self.columns_to_drop) > 0:
            filtered_table_cols_dropped = filtered_table.drop_columns(self.columns_to_drop)
        else:
            filtered_table_cols_dropped = filtered_table

        # add global filter stats to metadata
        metadata["docs_after_filter"] = filtered_table.num_rows
        metadata["columns_after_filter"] = filtered_table_cols_dropped.num_columns
        metadata["bytes_after_filter"] = filtered_table.nbytes

        return [filtered_table_cols_dropped], metadata


class FilterTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=FilterTransform,
        )

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
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

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True
