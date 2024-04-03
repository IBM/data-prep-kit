from io import BytesIO
from argparse import ArgumentParser, Namespace
import subprocess
import time
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

INPUT_COLUMN_KEY = "antivirus_input_column"
OUTPUT_COLUMN_KEY = "antivirus_output_column"
CLAMD_SOCKET_KEY = "antivirus_clamd_socket"
DEFAULT_INPUT_COLUMN = "contents"
DEFAULT_OUTPUT_COLUMN = "virus_detection"
DEFAULT_CLAMD_SOCKET = "/var/run/clamav/clamd.ctl"
CLAMD_TIMEOUT_SEC = 180

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
        self.input_column = config.get(INPUT_COLUMN_KEY, DEFAULT_INPUT_COLUMN)
        self.output_column = config.get(OUTPUT_COLUMN_KEY, DEFAULT_OUTPUT_COLUMN)
        self.clamd_socket = config.get(CLAMD_SOCKET_KEY, DEFAULT_CLAMD_SOCKET)
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

        def _scan_with_timeout(content: str) -> str | None:
            timeout = time.time() + CLAMD_TIMEOUT_SEC
            while True:
                try:
                    return _scan(content)
                except clamd.ConnectionError as err:
                    if time.time() < timeout:
                        logger.debug('Clamd is not ready. Retry after 5 seconds.')
                        time.sleep(5)
                    else:
                        logger.error(f"clamd didn't become ready in {CLAMD_TIMEOUT_SEC} seconds.")
                        raise err

        def _start_clamd_and_scan(content: str) -> str | None:
            try:
                return _scan(content)
            except clamd.ConnectionError:
                logger.info("Clamd process is not running. Start clamd process.")
                subprocess.Popen('clamd', shell=True)
                logger.info(f"Started clamd process. Retry to scan.")
                return _scan_with_timeout(content)

        virus_detection = pa.array(list(map(_start_clamd_and_scan, table[self.input_column].to_pylist())), type=pa.string())

        nrows = table.num_rows
        clean = virus_detection.null_count
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
            f"--{INPUT_COLUMN_KEY}",
            type=str,
            default=DEFAULT_INPUT_COLUMN,
            help="input column name",
        )
        parser.add_argument(
            f"--{OUTPUT_COLUMN_KEY}",
            type=str,
            default=DEFAULT_OUTPUT_COLUMN,
            help="output column name",
        )
        parser.add_argument(
            f"--{CLAMD_SOCKET_KEY}",
            type=str,
            default=DEFAULT_CLAMD_SOCKET,
            help="local socket path for clamd"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        self.params[INPUT_COLUMN_KEY] = args.antivirus_input_column
        self.params[OUTPUT_COLUMN_KEY] = args.antivirus_output_column
        self.params[CLAMD_SOCKET_KEY] = args.antivirus_clamd_socket
        logger.info(f"antivirus parameters are : {self.params}")
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
    logger.info("Launching antivirus transform")
    launcher.launch()
