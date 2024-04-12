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

import pyarrow as pa
from data_processing.ray import DefaultTableTransformConfiguration, TransformLauncher
from data_processing.transform import AbstractTableTransform


class NOOPTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of NOOPTransformConfiguration class
        super().__init__(config)
        self.sleep = config.get("sleep", 1)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        if self.sleep is not None:
            print(f"Sleep for {self.sleep} seconds")
            time.sleep(self.sleep)
            print("Sleep completed - continue")
        # Add some sample metadata.
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata


class NOOPTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="NOOP", transform_class=NOOPTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--noop_sleep_sec",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.noop_sleep_sec < 0:
            print(
                f"Parameter noop_sleep_sec should be greater then or equal to 0, you specified {args.noop_sleep_sec}"
            )
            return False
        self.params["sleep"] = args.noop_sleep_sec
        print(f"noop parameters are : {self.params}")
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
    launcher.launch()
