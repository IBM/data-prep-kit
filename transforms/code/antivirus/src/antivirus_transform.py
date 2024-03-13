from io import BytesIO
from argparse import ArgumentParser, Namespace
import traceback
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger
from data_processing.utils.transform_utils import TransformUtils


logger = get_logger(__name__)


class AntivirusTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, AntivirusTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of AntivirusTransformConfiguration class
        super().__init__(config)
        self.warning_issued = False
        self.input_column = config.get("input_column", "contents")
        self.output_column = config.get("output_column", "virus_detection")
        self.clamd_socket = config.get("clamd_socket", "/var/run/clamav/clamd.ctl")
        logger.info(f"Using unix socket: {self.clamd_socket}")

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        import clamd
        
        logger.debug(f"Transforming one table with {len(table)} rows")

        cd = clamd.ClamdUnixSocket(path=self.clamd_socket)
        
        def _scan(content: str) -> str | None:
            if content is None:
                return None
            (status, description) = cd.instream(BytesIO(content.encode()))['stream']
            if status == 'FOUND':
                logger.debug(f"Detected: {description}")
                return description or 'UNKNOWN'
            return None

        try:
            virus_detection = list(map(_scan, table[self.input_column].to_pylist()))
        except Exception as e:
            logger.error(f"Exception during the scan {e}: {traceback.print_exc()}")
            return None, None
        
        nrows = len(virus_detection)
        clean = virus_detection.count(None)
        infected = nrows - clean
        table = TransformUtils.add_column(table, self.output_column, virus_detection)
        # Add some sample metadata.
        logger.debug(f"Virus detection {infected} / {nrows} rows")
        metadata = {"clean": clean, "infected": infected}
        return [table], metadata


class AntivirusTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="Antivirus", transform_class=AntivirusTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the AntivirusTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--input_column",
            type=str,
            default="contents",
            help="input column name",
        )
        parser.add_argument(
            "--output_column",
            type=str,
            default="virus_detection",
            help="output column name",
        )
        parser.add_argument(
            "-s",
            "--clamd_socket",
            type=str,
            default="/var/run/clamav/clamd.ctl",
            help="local socket path for clamd"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if len(args.input_column) < 1:
            logger.error("Empty value is not allowed for input_column")
            return False
        if len(args.output_column) < 1:
            logger.error("Empty value is not allowed for output_column")
            return False
        self.params["input_column"] = args.input_column
        self.params["output_column"] = args.output_column
        self.params["clamd_socket"] = args.clamd_socket
        logger.info(f"antivirus parameters are : {self.params}")
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
