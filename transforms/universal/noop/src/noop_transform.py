import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger


logger = get_logger(__name__)


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
        logger.debug(f"Transforming one table with {len(table)} rows")
        if self.sleep is not None:
            logger.info(f"Sleep for {self.sleep} seconds")
            time.sleep(self.sleep)
            logger.info("Sleep completed - continue")
        # Add some sample metadata.
        logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata


class NOOPTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
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
        # An example of a command line option that we don't want included in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            "--noop_pwd",
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
        if args.noop_sleep_sec <= 0:
            print(f"Parameter noop_sleep_sec should be greater then 0, you specified {args.noop_sleep_sec}")
            return False
        self.params["sleep"] = args.noop_sleep_sec
        self.params["pwd"] = args.noop_pwd
        print(f"noop parameters are : {self.params}")
        # Don't publish this in the metadata produced by the ray orchestrator.
        self.remove_from_metadata["pwd"]
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
