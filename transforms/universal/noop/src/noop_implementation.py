import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    AbstractTableTransformRuntimeFactory,
    DefaultTableTransformRuntime,
)
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
        self.sleep_msec = config.get("noop_sleep_msec", 1)

    def transform(self, table: pa.Table) -> list[pa.Table]:
        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        if self.sleep_msec is not None:
            print(f"Sleep for {self.sleep_msec} milliseconds")
            time.sleep(self.sleep_msec / 1000)
            print("Sleep completed - continue")
        return [table]


class NOOPTableTransformRuntimeFactory(AbstractTableTransformRuntimeFactory):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(runtime_class=DefaultTableTransformRuntime, transformer_class=NOOPTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--noop_sleep_msec",
            type=int,
            default=1,
            help="Sleep actor for a number of milliseconds while processing the data frame, before writing the file to COS",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        self.params["sleep"] = args.noop_sleep_msec
        print(f"noop parameters are : {self.params}")
        return True
