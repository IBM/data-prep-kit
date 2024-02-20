import argparse
import time
from argparse import ArgumentParser

import pyarrow

from data_processing.ray.transform_runtime import DefaultTableTransformRuntime
from data_processing.table_transform import AbstractTableTransform


class NOOPTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        super().__init__(config)
        self.sleep_msec = config.get("noop_sleep_msec", None)

    def mutate(self, table: pyarrow.Table) -> tuple[pyarrow.Table, dict]:
        """
        Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.
        """
        if self.sleep_msec is not None:
            print(f"Sleep for {self.sleep_msec} milliseconds")
            time.sleep(self.sleep_msec / 1000)
            print("Sleep completed - continue")
        rows = len(table)
        metadata = {"nrows": rows, "nfiles": 1}
        return table, metadata



class NOOPTransformRuntime(DefaultTableTransformRuntime):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """
    def __init__(self):
        super().__init__(NOOPTransform)

    def add_input_arguments(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--noop_sleep_msec",
            type=int,
            required=False,
            help="Sleep actor for a number of milliseconds while processing the data frame, before writing the file to COS",
        )

    def combine_metadata(self, m1: dict, m2: dict) -> dict:
        """
        Combine the metadata being produced across multiple calls to NOOPTransform.mutate()
        """
        m1_files = m1["nfiles"]
        m2_files = m2["nfiles"]
        m1_rows = m1["nrows"]
        m2_rows = m2["nrows"]
        combined = {"nrows": m1_rows + m2_rows, "nfiles": m1_files + m2_files}
        return combined


if __name__ == "__main__":
    # Not currently used, but shows how one might use the two classes above outside of ray.
    parser = argparse.ArgumentParser()
    runtime = NOOPTransformRuntime()
    runtime.add_input_arguments(parser)
    args = parser.parse_args()
    runtime.apply_input_arguments(args)
    transform = NOOPTransform(vars(args))
    print(f"transform={transform}")
