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
        self.use_network_socket = config.get("use_network_socket", False)
        if self.use_network_socket:
            self.network_socket_host = config.get("network_socket_host", "localhost")
            self.network_socket_port = config.get("network_socket_port", 3310)
            logger.info(f"Using network scoket: {self.network_socket_host}:{self.network_socket_port}")
        else:
            logger.info("Using unix socket")

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        import clamd
        
        logger.debug(f"Transforming one table with {len(table)} rows")
        if self.output_column in table.column_names:
            if not self.warning_issued:
                logger.warn(f"Drop existing column {self.output_column}")
                self.warning_issued = True
            table = table.drop(self.output_column)

        if self.use_network_socket:
            cd = clamd.ClamdNetworkSocket(host=self.network_socket_host, port=self.network_socket_port)
        else:
            cd = clamd.ClamdUnixSocket()
        
        def _scan(content: str) -> str | None:
            if content is None:
                return None
            (status, description) = cd.instream(BytesIO(content.encode()))['stream']
            if status == 'FOUND':
                logger.debug(f"Detected: {description}")
                return description or 'UNKNOWN'
            return None

        try:
            virus_detection = pa.array(list(map(_scan, table[self.input_column].to_pylist())), type=pa.string())
        except Exception as e:
            logger.error(f"Exception during the scan {e}: {traceback.print_exc()}")
            return None, None
        
        nrows = len(virus_detection)
        detected = nrows - virus_detection.null_count
        table = table.append_column(self.output_column, virus_detection)
        # Add some sample metadata.
        logger.debug(f"Virus detection {detected} / {nrows} rows")
        metadata = {"nfiles": 1, "nrows": len(table), "detected": detected}
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
            "-n",
            "--network_socket",
            type=str,
            default="",
            help="If provided, use local network socket (default: use unix socket (/var/run/clamav/clamd.ctl))."
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
        use_network_socket = bool(args.network_socket)
        if use_network_socket:
            host_port = args.network_socket.split(":")
            if len(host_port) < 2 or not host_port[-1].isnumeric():
                logger.error(f"invalid netowrk socket: {args.network_socket}")
                return False
            self.params["use_network_socket"] = True
            self.params["network_socket_host"] = ":".join(host_port[:-1])
            self.params["network_socket_port"] = int(host_port[-1])
        logger.info(f"antivirus parameters are : {self.params}")
        return True


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
