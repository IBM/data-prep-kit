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
import re
from argparse import ArgumentParser, Namespace
from typing import Any, List, Tuple

import numpy as np
import polars as pl
import pyarrow as pa
from data_processing.transform import AbstractFolderTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger
from Murmur_MH import Murmur_MH


short_name = "fdlist"
cli_prefix = f"{short_name}_"

# configuration keys
subfolder_key = "docs_to_remove"
""" This key holds the name of the subfolder with the duplicate records"""
consolidated_filename_key = "consolidated_filename"
""" This key holds the name of the file with the consolidated list of duplicates"""
sort_output_key = "sort_output"
""" This key is used to sort"""

# command line arguments
subfolder_cli_param = f"{cli_prefix}{subfolder_key}"
""" The name of the subfolder with the duplicate records"""
consolidated_filename_cli_param = f"{cli_prefix}{consolidated_filename_key}"
""" The name of the file with the consolidated list of duplicates"""
sort_output_cli_param = f"{cli_prefix}{sort_output_key}"
""" Sort the output"""

captured_arg_keys = [
    subfolder_key,
    consolidated_filename_key,
    sort_output_key,
]

# defaults
subfolder_default = "docs_to_remove"
""" Default name of the subfolder with the duplicate records"""
consolidated_filename_default = os.path.join("docs_to_remove_consolidated", "docs_to_remove_consolidated.parquet")
""" Default name of the file with the consolidated list of duplicates"""
sort_output_default = False


class GetDuplicateListTransform(AbstractFolderTransform):
    """
    This is an intermediate step of the fuzzy dedup pipeline. It runs in a single
    location and consolidates in a single file all the duplicates found for each
    band segment.
    Args:
        subfolder: name of the subfolder with the duplicate records
        consolidated_filename: name of the file with the consolidated list of duplicates
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments
        defined by the companion runtime, ClusterAnalysisTransformRuntime.
        """
        super().__init__(config)
        self.subfolder = config.get(subfolder_key, subfolder_default)
        self.consolidated_filename = config.get(consolidated_filename_key, consolidated_filename_default)
        self.sort_output = config.get(sort_output_key, sort_output_default)
        self.data_access = config.get("data_access")
        self.logger = get_logger(__name__)

    def transform(self, folder_name: str) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        self.logger.info(f"Get Duplicate List for folder {folder_name}")
        metadata = {}
        input_folder = self.sanitize_folder_name(os.path.join(self.data_access.input_folder, folder_name))
        files, retries = self.data_access.get_folder_files(
            path=input_folder,
            extensions=[".parquet"],
            return_data=True,
        )
        if retries > 0:
            metadata |= {"data_access_retries": retries}
        output_folder = self.sanitize_folder_name(self.data_access.output_folder)
        output_path = os.path.join(output_folder, self.consolidated_filename)

        # consolidate into a single data frame band hashes computed by workers
        consolidated_dataframe, consolidation_stats = self.consolidate_docs_to_remove_files(files)
        self.logger.info(f"{len(consolidated_dataframe)} documents marked as duplicates")
        metadata |= consolidation_stats
        output_data = TransformUtils.convert_arrow_to_binary(consolidated_dataframe.to_arrow())
        return [(output_data, output_path)], metadata

    def sanitize_folder_name(self, folder_name: str) -> str:
        if "://" in folder_name:
            _, folder_name = folder_name.split("://")
        if folder_name[-1] != "/":
            folder_name = f"{folder_name}/"
        return folder_name

    def consolidate_docs_to_remove_files(self, files: dict[str, bytes]) -> tuple[pl.DataFrame, dict[str, Any]]:
        consolidated_dataframe = pl.DataFrame()
        total_input_rows = 0
        for fname, contents in files.items():
            df = pl.read_parquet(io.BytesIO(contents))
            total_input_rows += len(df)
            self.logger.debug(f"{fname} has {len(df)} rows")
            consolidated_dataframe = consolidated_dataframe.vstack(df)
        consolidated_dataframe = consolidated_dataframe.select("docs_to_remove").unique()

        consolidation_stats = {
            "input_files": len(files),
            "input_bytes": sum(len(v) for v in files.values()),
            "input_rows": total_input_rows,
            "consolidated_files": 1,
            "consolidated_bytes": consolidated_dataframe.to_arrow().nbytes,
            "consolidated_rows": len(consolidated_dataframe),
        }
        if self.sort_output:
            consolidated_dataframe = consolidated_dataframe.sort(by="docs_to_remove")

        return consolidated_dataframe, consolidation_stats


class GetDuplicateListTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=GetDuplicateListTransform,
            remove_from_metadata=[],
        )
        self.logger = get_logger(__name__, level="INFO")

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the GetDuplicateListTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{subfolder_cli_param}",
            type=str,
            default=subfolder_default,
            help="The name of the subfolder with the duplicate records",
        )
        parser.add_argument(
            f"--{consolidated_filename_cli_param}",
            type=str,
            default=consolidated_filename_default,
            help="The name of the file with the consolidated list of duplicates",
        )
        parser.add_argument(
            f"--{sort_output_cli_param}",
            type=bool,
            default=sort_output_default,
            help="Sort",
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
