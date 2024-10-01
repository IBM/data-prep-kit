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


from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider
from sp_helper import *


short_name = "sp"
cli_prefix = f"{short_name}_"

ikb_file = "ikb_file"
null_libs_file = "null_libs_file"

ikb_file_cli_param = f"{cli_prefix}{ikb_file}"
null_libs_file_cli_param = f"{cli_prefix}{null_libs_file}"



class SemanticProfilerTransform(AbstractTableTransform):
    """
    Implements the semantic profiler transform on a pyarrow table
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
        self.ikb_file = config.get("ikb_file", "../src/ikb/ikb_model.csv")
        self.null_libs_file = config.get("null_libs_file", "../src/ikb/null_libs.csv")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation takes a pyarrow table (ouput of the USBR transform) as input and obtains the
        semantic mapping of each datapoint from the Internal Knowledge Base. These semantic concepts are added as
        a new column into the input table and returned as output. The points for which no semantic mapping is found are
        written into the "null_libs.csv" file.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        ikb = knowledge_base(self.ikb_file, self.null_libs_file)
        ikb.load_ikb_trie()
        libraries = table.column('Library').to_pylist()
        language = table.column('Language').to_pylist()
        concepts = [concept_extractor(lib, lang, ikb) for lib, lang in zip(libraries, language)]
        new_col = pa.array(concepts)
        table = table.append_column('Concepts', new_col)
        ikb.write_null_files()
        # Add some sample metadata.
        self.logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata


class SemanticProfilerTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=SemanticProfilerTransform,
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

        parser.add_argument(
            f"--{ikb_file_cli_param}",
            type=str,
            default=None,
            help="Default IKB file",
        )

        parser.add_argument(
            f"--{null_libs_file_cli_param}",
            type=str,
            default=None,
            help="Default Null Libraries file",
        )


    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"sp parameters are : {self.params}")
        return True
