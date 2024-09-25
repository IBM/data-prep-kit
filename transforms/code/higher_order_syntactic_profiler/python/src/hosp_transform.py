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

import time
from argparse import ArgumentParser, Namespace
from typing import Any
import csv

import pyarrow as pa
import pyarrow.parquet as pq
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider
from UAST import *


short_name = "hosp"
cli_prefix = f"{short_name}_"

metrics_list = "metrics_list"
hosp_metrics_cli_param = f"{cli_prefix}{metrics_list}"


def uast_read(jsonstring):
    uast = UAST()
    if jsonstring is not None and jsonstring != 'null':
        uast.load_from_json_string(jsonstring)
        return uast
    return None

def extract_ccr(uast):
    if uast is not None:
        total_comment_loc = 0
        for node_idx in uast.nodes:
            node = uast.get_node(node_idx)
            if node.node_type == 'uast_comment':
                total_comment_loc += node.metadata.get("loc_original_code", 0)
            elif node.node_type == 'uast_root':
                loc_snippet = node.metadata.get("loc_snippet", 0)
        if total_comment_loc > 0:
            return loc_snippet / total_comment_loc
        else:
            return None 
    return None


class HigherOrderSyntacticProfilerTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, SemanticProfilerTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of SemanticProfilerTransformConfiguration class
        super().__init__(config)
        self.metrics_list = config.get("metrics", ["CCR"])
        

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        if self.metrics_list is not None:
            for metric in self.metrics_list:
                if metric == "CCR":
                    self.logger.info(f"Generating {metric} values")
                    uasts = [uast_read(uast_json) for uast_json in table['UAST'].to_pylist()]
                    ccrs = [extract_ccr(uast) for uast in uasts]
                    new_table = table.append_column('CCR', pa.array(ccrs))
        self.logger.debug(f"Transformed one table with {len(new_table)} rows")
        metadata = {"nfiles": 1, "nrows": len(new_table)}
        return [new_table], metadata


class HigherOrderSyntacticProfilerTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=HigherOrderSyntacticProfilerTransform,
            # remove_from_metadata=[pwd_key],
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the SemanticProfilerTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, sp_, pii_, etc.)
        """
       
        # Add argument for a list of strings
        parser.add_argument(
            f"--{hosp_metrics_cli_param}",
            type=str,
            nargs='+',  # Accept one or more strings
            default=["CCR"],  # Set a default value as a list
            help="List of higher order syntactic profiling metrics (default: ['CCR'])",
        )


    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"hosp parameters are : {self.params}")
        return True
