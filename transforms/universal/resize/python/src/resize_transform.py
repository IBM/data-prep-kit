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
from data_processing.utils import (
    LOCAL_TO_DISK,
    MB,
    CLIArgumentProvider,
    UnrecoverableException,
    get_logger,
)


max_rows_per_table_key = "max_rows_per_table"
max_mbytes_per_table_key = "max_mbytes_per_table"
size_type_key = "size_type"
shortname = "resize"
cli_prefix = f"{shortname}_"
max_rows_per_table_cli_param = f"{cli_prefix}{max_rows_per_table_key}"
max_mbytes_per_table_cli_param = f"{cli_prefix}{max_mbytes_per_table_key}"
size_type_cli_param = f"{cli_prefix}{size_type_key}"
size_type_disk = "disk"
size_type_memory = "memory"
size_type_default = size_type_disk


class ResizeTransform(AbstractTableTransform):
    """
    Implements splitting large files into smaller ones.
    Two flavours of splitting are supported - based on the amount of documents and based on the size
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        super().__init__(config)
        self.max_rows_per_table = config.get(max_rows_per_table_key, 0)
        self.max_bytes_per_table = MB * config.get(max_mbytes_per_table_key, 0)
        disk_memory = config.get(size_type_key, size_type_default)
        if size_type_default in disk_memory:
            self.max_bytes_per_table *= LOCAL_TO_DISK

        self.logger.debug(f"max bytes = {self.max_bytes_per_table}")
        self.logger.debug(f"max rows = {self.max_rows_per_table}")
        self.buffer = None
        if self.max_rows_per_table <= 0 and self.max_bytes_per_table <= 0:
            raise ValueError("Neither max rows per table nor max table size are defined")
        if self.max_rows_per_table > 0 and self.max_bytes_per_table > 0:
            raise ValueError("Both max rows per table and max table size are defined. Only one should be present")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        split larger files into the smaller ones
        :param table: table
        :param file_name: name of the file
        :return: resulting set of tables
        """
        self.logger.debug(f"got new table with {table.num_rows} rows")
        if self.buffer is not None:
            try:
                self.logger.debug(
                    f"concatenating buffer with {self.buffer.num_rows} rows to table with {table.num_rows} rows"
                )
                # table = pa.concat_tables([self.buffer, table], unicode_promote_options="permissive")
                table = pa.concat_tables([self.buffer, table])
                self.buffer = None
                self.logger.debug(f"concatenated table has {table.num_rows} rows")
            except Exception as _:  # Can happen if schemas are different
                # Raise unrecoverable error to stop the execution
                self.logger.warning(f"table in {file_name} can't be merged with the buffer")
                self.logger.warning(f"incoming table columns {table.schema.names} ")
                self.logger.warning(f"buffer columns {self.buffer.schema.names}")
                raise UnrecoverableException()

        result = []
        start_row = 0
        if self.max_rows_per_table > 0:
            # split file with max documents
            n_rows = table.num_rows
            rows_left = n_rows
            while start_row < n_rows and rows_left >= self.max_rows_per_table:
                length = n_rows - start_row
                if length > self.max_rows_per_table:
                    length = self.max_rows_per_table
                a_slice = table.slice(offset=start_row, length=length)
                self.logger.debug(f"created table slice with {a_slice.num_rows} rows, starting with row {start_row}")
                result.append(a_slice)
                start_row = start_row + self.max_rows_per_table
                rows_left = rows_left - self.max_rows_per_table
        else:
            # split based on size
            current_size = 0.0
            if table.nbytes >= self.max_bytes_per_table:
                for n in range(table.num_rows):
                    current_size += table.slice(offset=n, length=1).nbytes
                    if current_size >= self.max_bytes_per_table:
                        self.logger.debug(f"capturing slice, current_size={current_size}")
                        # Reached the size
                        a_slice = table.slice(offset=start_row, length=(n - start_row))
                        result.append(a_slice)
                        start_row = n
                        current_size = 0.0
        if start_row < table.num_rows:
            # buffer remaining chunk for next call
            self.logger.debug(f"Buffering table starting at row {start_row}")
            self.buffer = table.slice(offset=start_row, length=(table.num_rows - start_row))
            self.logger.debug(f"buffered table has {self.buffer.num_rows} rows")
        self.logger.debug(f"returning {len(result)} tables")
        return result, {}

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        result = []
        if self.buffer is not None:
            self.logger.debug(f"flushing buffered table with {self.buffer.num_rows} rows of size {self.buffer.nbytes}")
            result.append(self.buffer)
            self.buffer = None
        else:
            self.logger.debug(f"Empty buffer. nothing to flush.")
        return result, {}


class ResizeTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name=shortname, transform_class=ResizeTransform)
        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the resizeTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{max_rows_per_table_cli_param}",
            type=int,
            default=-1,
            help="Max number of rows per table",
        )
        parser.add_argument(
            f"--{max_mbytes_per_table_cli_param}",
            type=float,
            default=-1,
            help=f"Max table size (MB). Size is measured according to the --{size_type_cli_param} parameter",
        )
        parser.add_argument(
            f"--{size_type_cli_param}",
            type=str,
            required=False,
            default=size_type_default,
            choices=[size_type_disk, size_type_memory],
            help=f"Determines how memory is measured when using the --{max_mbytes_per_table_cli_param} option."
            "\n'memory' measures the in-process memory footprint and \n'disk' makes an estimate of the resulting parquet file size.",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        # dargs = vars(args)
        if self.params.get(max_rows_per_table_key) <= 0 and self.params.get(max_mbytes_per_table_key) <= 0:
            self.logger.info("Neither max documents per table nor max table size are defined")
            return False
        if self.params.get(max_rows_per_table_key) > 0 and self.params.get(max_mbytes_per_table_key) > 0:
            self.logger.info("Both max documents per table and max table size are defined. Only one should be present")
            return False
        self.logger.info(f"Split file parameters are : {self.params}")
        return True
