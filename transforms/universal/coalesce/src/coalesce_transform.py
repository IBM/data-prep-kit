from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import LOCAL_TO_DISK, MB, get_logger


logger = get_logger(__name__)


class CoalesceTransform(AbstractTableTransform):
    """
    Implements file coalescing small files into the larger ones.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        super().__init__(config)
        self.coalesce_target = LOCAL_TO_DISK * MB * config.get("coalesce_target_mb", 1.0)
        self.output_buffer = []

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Coalesce smaller files into the larger ones
        :param table: table
        :return: resulting table, if we reached coalescing threshold or nothing, if it is not reached
        """
        # Add table to the output buffer
        self.output_buffer.append((table, table.nbytes))
        # Check the size
        total_size = 0
        for (_, table_size) in self.output_buffer:
            total_size += table_size
        # Not enough data to output
        if total_size <= self.coalesce_target:
            return [], {}
        # Build table to return and return it
        return self._build_return_table()

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Flushing remaining
        :return: remaining table
        """
        if len(self.output_buffer) == 0:
            return [], {}
        return self._build_return_table()

    def _build_return_table(self) -> tuple[[pa.Table], dict[str, Any]]:
        """
        build return table and return it
        :return: an array of tables and metadata dictionary
        """
        # Build output table
        output_batches = []
        for (table, _) in self.output_buffer:
            output_batches.extend(table.to_batches())  # this is zero copy
        out_table = pa.Table.from_batches(batches=output_batches)
        # clear out buffer
        self.output_buffer = []
        return [out_table], {}


class CoalesceTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name="Coalesce", runtime_class=DefaultTableTransformRuntime, transform_class=CoalesceTransform
        )
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--coalesce_target_mb",
            type=float,
            default=0,
            help="Coalesce target (MB)",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.coalesce_target_mb <= 0:
            logger.info(f"Parameter coalesce_target should be greater then 0, you specified {args.coalesce_target_mb}")
            return False
        self.params["coalesce_target_mb"] = args.coalesce_target_mb
        logger.info(f"Coalesce parameters are : {self.params}")
        return True


if __name__ == "__main__":

    launcher = TransformLauncher(transform_runtime_config=CoalesceTransformConfiguration())
    launcher.launch()
