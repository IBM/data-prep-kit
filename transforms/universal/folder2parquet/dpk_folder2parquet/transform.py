# SPDX-License-Identifier: Apache-2.0
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

import io
import os
import zipfile
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import (
    CLIArgumentProvider,
    TransformUtils,
    UnrecoverableException,
    get_dpk_logger,
    str2bool,
)


logger = get_dpk_logger()

shortname = "f2p"
cli_prefix = f"{shortname}_"
input_folder_cli_param = "f2p_input_folder"
fewer_parquets_cli_param = "f2p_fewer_parquets"
content_column_cli_param = "f2p_content_column"
file_path_column_cli_param = "f2p_file_path"
document_uuid_column_cli_param = "f2p_document_uuid"
file_types_to_use_cli_param = "f2p_file_types_to_use"

content_column_default = "binary_contents"
fewer_parquets_default = False
document_uuid_default = "document_uuid"
file_path_default = "file_path"
file_types_to_use_default = "*"


class Folder2ParquetTransform(AbstractTableTransform):
    """
    Implements creating parquet tables that store the contents of the files of an input folder.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        super().__init__(config)
        logger.info(config)
        data_access = config.get("data_access")
        input_folder = None
        if data_access and hasattr(data_access, "get_input_folder"):
            try:
                input_folder = data_access.get_input_folder()
            except Exception:
                input_folder = None

        if input_folder is None:
            input_folder = config.get(input_folder_cli_param)

        self.input_folder = input_folder
        self.relative_path = None
        self.buffer = None
        self.fewer_parquets = config.get(fewer_parquets_cli_param, fewer_parquets_default)
        self.content_column = config.get(content_column_cli_param, content_column_default)
        self.file_path = config.get(file_path_column_cli_param, file_path_default)
        self.document_uuid = config.get(document_uuid_column_cli_param, document_uuid_default)
        extensions_raw = config.get(file_types_to_use_cli_param, file_types_to_use_default)
        if isinstance(extensions_raw, str):
            self.file_types_to_use = {ext.strip().lower() for ext in extensions_raw.split(",")}
        else:
            self.file_types_to_use = {ext.lower() for ext in extensions_raw}

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Processes a local folder and returns a PyArrow Table.
        :return: resulting set of tables
        """
        for file_path in Path(self.input_folder).rglob("*"):
            if file_path.is_file() and not file_path.name.startswith("."):
                if (
                    "*" in self.file_types_to_use
                    or file_path.suffix.lower() in self.file_types_to_use
                ):
                    try:
                        with open(file_path, "rb") as f:
                            content = f.read()
                        # transform_binary will buffer the files
                        self.transform_binary(str(file_path), content)
                        logger.info(f"Buffered: {file_path.name}")
                    except Exception as e:
                        logger.error(f"Failed to process {file_path}: {e}")
                else:
                    logger.debug(f"Skipping {file_path.name} (extension not included)")

        return self.flush()

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into one or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param byte_array: contents of the input file to be transformed.
        :param file_name: the file name of the file containing the given byte_array.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        self.relative_path = file_name.replace(self.input_folder, "").lstrip(os.sep)

        def _new_row(file_path, byte_array):
            import uuid

            return pa.Table.from_pylist(
                [{self.file_path: file_path, self.document_uuid: str(uuid.uuid4()), self.content_column: byte_array}]
            )

        if TransformUtils.get_file_extension(file_name)[1] == ".zip":
            logger.info(f"Iterating through zip file {file_name=} .")
            table = None
            with zipfile.ZipFile(io.BytesIO(byte_array)) as opened_zip:
                zip_namelist = opened_zip.namelist()
                for archive_doc_filename in zip_namelist:
                    logger.info("Processing " f"{file_name}/{archive_doc_filename} ")
                    with opened_zip.open(archive_doc_filename) as file:
                        try:
                            # Read the content of the file
                            content_bytes = file.read()
                            if table is None:
                                table = _new_row(f"{self.relative_path}/{archive_doc_filename}", content_bytes)
                            else:
                                table = pa.concat_tables(
                                    [table, _new_row(f"{self.relative_path}/{archive_doc_filename}", content_bytes)]
                                )
                        except Exception as e:
                            logger.error(f" skipping {archive_doc_filename} in {file_name} due to {str(e)}")
                return [(TransformUtils.convert_arrow_to_binary(table=table), ".parquet")], {}

        else:
            try:
                if self.buffer is not None:
                    logger.debug(f"Added new row {file_name} to existing  buffer with {self.buffer.num_rows}")
                    self.buffer = pa.concat_tables([self.buffer, _new_row(self.relative_path, byte_array)])
                else:
                    logger.debug(f"Starting buffer with {file_name}")
                    self.buffer = _new_row(self.relative_path, byte_array)
                ## Wait for flush when folder change before writing new parquet file
            except Exception as _:  # Can happen if schemas are different
                # Raise unrecoverable error to stop the execution
                logger.warning(f"table in {file_name} can't be merged with the buffer")
                logger.warning(f"buffer columns {self.buffer.schema.names}")
                raise UnrecoverableException()
            return [], {}

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        result = []
        if self.buffer is not None:
            logger.debug(f"flushing buffered table with {self.buffer.num_rows} rows of size {self.buffer.nbytes}")
            result.append(self.buffer)
            self.buffer = None
        else:
            logger.debug(f"Empty buffer. nothing to flush.")
        return result, {"parquet_file": [{self.relative_path: x.num_rows} for x in result]}

    def enforce_folder_boundary(self):
        """
        This is supporting method for transformers, that implement buffering of tables, and triggers
        a call to flush the buffer prior to switching to a new folder if so required
        :return: true if the runtime should call the flush method before processing the next folder
        """
        return not self.fewer_parquets


class Folder2ParquetTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name=shortname, transform_class=Folder2ParquetTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the Folder2ParquetTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{content_column_cli_param}",
            type=str,
            default=content_column_default,
            help="name of the column containing document",
        )
        parser.add_argument(
            f"--{fewer_parquets_cli_param}",
            type=lambda x: bool(str2bool(x)),
            default=fewer_parquets_default,
            help="name of the column containing document id",
        )

        parser.add_argument(
            f"--{file_path_column_cli_param}",
            type=str,
            default=file_path_default,
            help="name of the column containing file name",
        )
        parser.add_argument(
            f"--{document_uuid_column_cli_param}",
            type=str,
            default=document_uuid_default,
            help="name of the column containing document uuid",
        )
        parser.add_argument(
            f"--{file_types_to_use_cli_param}",
            type=str,
            default=file_types_to_use_default,
            help="Comma-separated list of file extensions to include (e.g., .txt,.pdf)",
        )
        return

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, True)
        self.params = self.params | captured
        logger.info(f"Ingest file parameters are : {self.params}")
        return True
