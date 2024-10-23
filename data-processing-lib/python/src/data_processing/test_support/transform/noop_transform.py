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
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration, AbstractTransform
from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)

short_name = "noop"
cli_prefix = f"{short_name}_"
sleep_key = "sleep_sec"
pwd_key = "pwd"
sleep_cli_param = f"{cli_prefix}{sleep_key}"
pwd_cli_param = f"{cli_prefix}{pwd_key}"


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
        self.sleep = config.get("sleep_sec", 1)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        logger.debug(f"Transforming one table with {len(table)} rows")
        if self.sleep is not None:
            logger.info(f"Sleep for {self.sleep} seconds")
            time.sleep(self.sleep)
            logger.info("Sleep completed - continue")
        # Add some sample metadata.
        logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata


class NOOPTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self, clazz: type[AbstractTransform] = NOOPTransform):
        super().__init__(
            name=short_name,
            transform_class=clazz,
            remove_from_metadata=[pwd_key],
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{sleep_cli_param}",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
        # An example of a command line option that we don't want included
        # in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            f"--{pwd_cli_param}",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
            print(f"Parameter noop_sleep_sec should be non-negative. you specified {args.noop_sleep_sec}")
            return False

        self.params = self.params | captured
        logger.info(f"noop parameters are : {self.params}")
        return True


class NOOPPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=NOOPTransformConfiguration())


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(NOOPPythonTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
