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
import io
import os
from argparse import ArgumentParser, Namespace
from typing import Any, List, Tuple

import numpy as np
import polars as pl
import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger


short_name = "fdclean"
cli_prefix = f"{short_name}_"

# configuration keys
document_id_column_key = "document_id_column"
""" This key holds the name of the column storing the unique ID assigned to each document"""
duplicate_list_location_key = "duplicate_list_location"
""" This key holds the location of the list of duplicate documents marked for removal"""
operation_mode_key = "operation_mode"
""" This key holds the operation mode: 'filter_duplicates', 'filter_non_duplicates', or 'annotate'"""

# command line arguments
document_id_column_cli_param = f"{cli_prefix}{document_id_column_key}"
""" Name of the column storing the unique ID assigned to each document"""
duplicate_list_location_cli_param = f"{cli_prefix}{duplicate_list_location_key}"
""" Location of the list of duplicate documents marked for removal"""
operation_mode_cli_param = f"{cli_prefix}{operation_mode_key}"
""" Operation mode, can be one of 'filter_duplicates', 'filter_non_duplicates', or 'annotate'"""

captured_arg_keys = [
    document_id_column_key,
    duplicate_list_location_key,
]

# defaults
document_id_column_default = "int_id_column"
""" Default name of the column storing the unique ID assigned to each document"""
duplicate_list_location_default = os.path.join("docs_to_remove_consolidated", "docs_to_remove_consolidated.parquet")
""" Default location of the list of duplicate documents marked for removal"""
operation_mode_default = "filter_duplicates"
""" Default value for operation mode, will filter out all the duplicate documents"""


class DataCleaningTransform(AbstractTableTransform):
    """
    This is the third transform of the fuzzy dedup pipeline. It takes as input
    the list of the documents to remove (identified as duplicates during the
    cluster analysis phase, and the original dataset. Each dataset file is
    imported into a table, and the documents that are in the documents to remove
    list are filtered out from that table. The output is a new dataset, which
    keeps the directory structure of the input dataset, but has all the fuzzy
    duplicates removed.

    Args:
        duplicate_location: location (local or s3) of the duplicate document list
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments
        defined by the companion runtime, ClusterAnalysisTransformRuntime.
        """
        super().__init__(config)
        self.logger = get_logger(__name__)
        self.document_id_column = config.get(document_id_column_key, document_id_column_default)
        self.duplicate_list_location = config.get(duplicate_list_location_key, duplicate_list_location_default)
        self.operation_mode = config.get(operation_mode_key, operation_mode_default)
        contents = config.get("df")
        self.docs_to_remove_df = pl.read_parquet(io.BytesIO(contents))
        self.logger.info(f"Got docs_to_remove_df with {len(self.docs_to_remove_df)} rows")
        self.docs_to_remove_df = self.docs_to_remove_df.rename({"docs_to_remove": self.document_id_column})

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        self.logger.info(f"Transforming table with {table.num_rows} rows from file {file_name}")
        input_df = pl.from_arrow(table)
        # handle the case when the doc_id columns in the input dataframe and the
        # docs_to_remove_df  have different types, i.e. one is int32 and the
        # other is int64
        input_doc_id_type = input_df[self.document_id_column].dtype
        if input_doc_id_type != self.docs_to_remove_df[self.document_id_column].dtype:
            self.docs_to_remove_df = self.docs_to_remove_df.select(
                pl.col(self.document_id_column).cast(input_doc_id_type)
            )
        if self.operation_mode == "filter_duplicates":
            result_df = input_df.join(self.docs_to_remove_df, on=self.document_id_column, how="anti")
        elif self.operation_mode == "filter_non_duplicates":
            result_df = input_df.join(self.docs_to_remove_df, on=self.document_id_column, how="inner")
        else:  # self.operation_mode == "annotation"
            duplicates_df = self.docs_to_remove_df.with_columns(pl.lit("d").alias("duplicate"))
            result_df = input_df.join(duplicates_df, on=self.document_id_column, how="left").with_columns(
                pl.col("duplicate").fill_null("")
            )
        result_table = result_df.to_arrow()
        metadata = {
            "input_files": 1,
            "input_docs": table.num_rows,
            "input_bytes": table.nbytes,
            "output_files": 1,
            "output_docs": result_table.num_rows,
            "output_bytes": result_table.nbytes,
            "filtered_docs": (table.num_rows - result_table.num_rows),
            "filtered_bytes": (table.nbytes - result_table.nbytes),
        }
        return [result_table], metadata


class DataCleaningTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self, transform_class: type[AbstractTableTransform] = DataCleaningTransform):
        super().__init__(
            name=short_name,
            transform_class=transform_class,
        )
        self.logger = get_logger(__name__, level="INFO")

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{document_id_column_cli_param}",
            type=str,
            default=document_id_column_default,
            help="name of the column storing the unique ID assigned to each document",
        )
        parser.add_argument(
            f"--{duplicate_list_location_cli_param}",
            type=str,
            default=duplicate_list_location_default,
            help="location of duplicate document list that are marked for removal",
        )
        parser.add_argument(
            f"--{operation_mode_cli_param}",
            choices=["filter_duplicates", "filter_non_duplicates", "annotate"],
            default=operation_mode_default,
            help="operation mode: filter out duplicates/non-duplicates, or annotate duplicate documents",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"{short_name} parameters are : {self.params}")
        return True
