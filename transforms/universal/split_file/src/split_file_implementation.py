from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import MB, LOCAL_TO_DISK


class SplitFileTransform(AbstractTableTransform):
    """
    Implements splitting large files into smaller ones.
    Two flavours of splitting are supported - based on the amount of documents and based on the size
    """
    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        super().__init__(config)
        self.max_documents_table = config.get("max_documents_table", 1.0)
        self.max_table_size = LOCAL_TO_DISK * MB * config.get("max_table_size", 1.0)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        split larger files into the smaller ones
        :param table: table
        :return: resulting set of tables
        """
        result = []
        start_row = 0
        if self.max_documents_table > 0:
            # split file with max documents
            n_rows = table.num_rows
            while start_row < n_rows:
                length = n_rows - start_row
                if length > self.max_documents_table:
                    length = self.max_documents_table
                result.append(table.slice(offset=start_row, length=length))
                start_row = start_row + self.max_documents_table
        else:
            # split based on size
            current_size = 0.0
            for n in range(table.num_rows):
                current_size += table.slice(offset=n, length=1).nbytes
                if current_size >= self.max_table_size:
                    # Reached the size
                    result.append(table.slice(offset=start_row, length=(n - start_row)))
                    start_row = n
                    current_size = 0.0
            if start_row < table.num_rows:
                # process remaining chunk
                result.append(table.slice(offset=start_row, length=(table.num_rows - start_row)))
        return result, {}


class SplitFileTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="SplitFile", runtime_class=DefaultTableTransformRuntime,
                         transform_class=SplitFileTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--max_documents_table",
            type=int,
            default=-1,
            help="Max documents per table",
        )
        parser.add_argument(
            "--max_table_size",
            type=int,
            default=-1,
            help="Max table size (MB)",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.max_documents_table <= 0 and args.max_table_size <= 0:
            print("Neither max documents per table nor max table size are defined")
            return False
        if args.max_documents_table > 0 and args.max_table_size > 0:
            print("Both max documents per table and max table size are defined. Only one should be present")
            return False
        self.params["max_documents_table"] = args.max_documents_table
        self.params["max_table_size"] = args.max_table_size
        print(f"Split file parameters are : {self.params}")
        return True
