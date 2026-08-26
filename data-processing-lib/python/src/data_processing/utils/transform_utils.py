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

import hashlib
import io
import os
import string
import sys
from typing import Any, List, Dict

import mmh3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.json as pj
import zipfile

from data_processing.exceptions import DPKException
from data_processing.utils import (
    ExecutionStatus, Metrics, TransformConstants, DocsStructure
)

from data_processing.utils import get_dpk_logger
logger = get_dpk_logger()

RANDOM_SEED = 42
LOCAL_TO_DISK = 2


class TransformUtils:
    """
    Class implementing support methods for filter implementation
    """

    @staticmethod
    def deep_get_size(ob) -> int:
        """
        Getting the complete size of the Python object. Based on
        https://www.askpython.com/python/built-in-methods/variables-memory-size-in-python
        Supports Python structures: list, tuple and set
            :param ob: object
            :return: object size
        """
        size = sys.getsizeof(ob)
        if isinstance(ob, (list, tuple, set)):
            for element in ob:
                size += TransformUtils.deep_get_size(element)
        if isinstance(ob, dict):
            for k, v in ob.items():
                size += TransformUtils.deep_get_size(k)
                size += TransformUtils.deep_get_size(v)
        return size

    @staticmethod
    def normalize_string(doc: str) -> str:
        """
        Normalize string
        :param doc: string to normalize
        :return: normalized string
        """
        return doc.replace(" ", "").replace("\n", "").lower().translate(str.maketrans("", "", string.punctuation))

    @staticmethod
    def str_to_hash(val: str) -> str:
        """
        compute string hash
        :param val: string
        :return: hash value
        """
        return hashlib.sha256(val.encode("utf-8")).hexdigest()

    @staticmethod
    def str_to_int(s: str) -> int:
        """
        Convert string to int using mmh3 hashing. Ensures predictable result by setting seed
        :param s: string
        :return: int hash
        """
        return mmh3.hash(s, seed=RANDOM_SEED, signed=False)

    @staticmethod
    def decode_content(content_bytes: bytes, encoding: str = "utf-8") -> str:
        """
        Decode the given bytes content using the specified encoding.
        :param content_bytes: The bytes content to decode
        :param encoding:The encoding to use while decoding the content. Default is 'utf-8'
        :return: str: The decoded content as a string if successful,
                      otherwise empty string if an error occurs during decoding.
        """
        try:
            content_string = content_bytes.decode(encoding)
            return content_string
        except Exception:
            return ""

    @staticmethod
    def get_file_extension(file_path) -> list[str]:
        """
        Get the file's root and extension from the given file path.
        :param file_path : The path of the file.
        :return: str: The file extension including the dot ('.') if present, otherwise an empty string.
        """
        return os.path.splitext(file_path)

    @staticmethod
    def get_file_basename(file_path) -> str:
        """
        Get the file's base name from the given file path.
        :param file_path : The path of the file.
        :return: str: file base name.
        """
        return os.path.basename(file_path)

    @staticmethod
    def validate_columns(table: pa.Table, required: list[str], transform_name: str = None) -> None:
        """
        Check if required columns exist in the table
        :param table: table
        :param required: list of required columns
        :return: None
        """
        columns = table.schema.names
        result = True
        for r in required:
            if r not in columns:
                result = False
                break
        if not result:
            message=f"Not all required columns for {transform_name} transform are present in the table - required {required}, present {table}"
            raise DPKException(message)


    @staticmethod
    def convert_ndjson_to_arrow(data: bytes) -> pa.Table:
        """
        Convert ndjson byte array to table
        :param data: byte array
        :return: table or None if the conversion failed
        """
        try:
            table = pj.read_json(io.BytesIO(data))
        except Exception as e:
            logger.warning(f"Could not convert bytes from ndjson to pyarrow- Retrying with bigger block size: {type(e)}-{e}")
            try:
                block_size=10 * 1024 * 1024
                logger.debug(f"Retrying with block size {block_size:,} ")                
                read_options = pj.ReadOptions(block_size=10 * 1024 * 1024) 
                table = pj.read_json(io.BytesIO(data), read_options=read_options)
            except Exception as e:  
                logger.error(f"Could not convert bytes from ndjson to pyarrow: {type(e)}-{e}")                
                table = None
        return table
    

    @staticmethod
    def convert_zip_to_arrow(data: bytes) -> pa.Table:
        """
        Convert zip file byte array to table. Currently only supports ndjson zipped files
        :param data: byte array
        :return: table or None if the conversion failed
        """
        table = None
        with zipfile.ZipFile(io.BytesIO(data)) as opened_zip:
            zip_namelist = opened_zip.namelist()
            for archive_filename in zip_namelist:
                logger.debug(f"Processing archive {archive_filename} with extention {TransformUtils.get_file_extension(archive_filename)[1]}")
                with opened_zip.open(archive_filename) as file:
                    try:
                        # Read the content of the file
                        content_bytes = file.read()
                        if TransformUtils.get_file_extension(archive_filename)[1] in [".ndjson", ".jsonl"]:
                            x = TransformUtils.convert_ndjson_to_arrow(content_bytes)
                            table =x if table is None else pa.concat_tables([table, x])
                    except Exception as e:
                        logger.error(f"Failed to read/convert {archive_filename}: {e}")
                        return None
        return table
                    

    @staticmethod
    def convert_binary_to_arrow(data: bytes, schema: pa.schema = None) -> pa.Table:
        """
        Convert byte array to table
        :param data: byte array
        :param schema: optional Arrow table schema used for reading table, default None
        :return: table or None if the conversion failed
        """
        from data_processing.utils import get_dpk_logger

        logger = get_dpk_logger()
        try:
            reader = pa.BufferReader(data)
            table = pq.read_table(reader, schema=schema)
            return table
        except Exception as e:
            logger.warning(f"Could not convert bytes to pyarrow: {e}")

        # We have seen this exception before when using pyarrow, but polars does not throw it.
        # "Nested data conversions not implemented for chunked array outputs"
        # See issue 816 https://github.com/data-prep-kit/data-prep-kit/issues/816.
        logger.info(f"Attempting read of pyarrow Table using polars")
        try:
            import polars

            df = polars.read_parquet(io.BytesIO(data))
            table = df.to_arrow()
        except Exception as e:
            logger.error(f"Could not convert bytes to pyarrow using polars: {e}. Skipping.")
            table = None
        return table

    @staticmethod
    def convert_arrow_to_binary(table: pa.Table) -> bytes:
        """
        Convert Arrow table to byte array
        :param table: Arrow table
        :return: byte array or None if conversion fails
        """
        from data_processing.utils import get_dpk_logger

        logger = get_dpk_logger()
        try:
            # convert table to bytes
            writer = pa.BufferOutputStream()
            # Update default snappy compression to ZSTD.
            # See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
            pq.write_table(table=table, where=writer, compression="ZSTD")
            return bytes(writer.getvalue())
        except Exception as e:
            logger.error(f"Failed to convert arrow table to byte array, exception {e}. Skipping it")
            return None

    @staticmethod
    def add_column(table: pa.Table, name: str, content: list[Any]) -> pa.Table:
        """
        Add column to the table
        :param table: original table
        :param name: column name
        :param content: content of the column
        :return: updated table, containing new column
        """
        # check if column already exist and drop it
        if name in table.schema.names:
            table = table.drop(columns=[name])
        # append column
        return table.append_column(field_=name, column=[content])

    @staticmethod
    def verify_no_duplicate_columns(table: pa.Table, file: str) -> bool:
        """
        Verify that resulting table does not have duplicate columns
        :param table: table
        :param file: file where we are saving the table
        :return: True, if there are no duplicates, False otherwise
        """
        from data_processing.utils import get_dpk_logger

        logger = get_dpk_logger()
        columns_list = table.schema.names
        columns_set = set(columns_list)
        if len(columns_set) != len(columns_list):
            logger.warning(f"Resulting table for file {file} contains duplicate columns {columns_list}. Skipping")
            return False
        return True

    @staticmethod
    def clean_path(path: str) -> str:
        """
        Clean path parameters:
            Removes white spaces from the input/output paths
            Removes schema prefix (s3://, http:// https://), if exists
            Adds the "/" character at the end, if it doesn't exist
            Removes URL encoding
        :param path: path to clean up
        :return: clean path
        """
        path = path.strip()
        if path == "":
            return path
        from urllib.parse import unquote, urlparse, urlunparse

        # Parse the URL
        parsed_url = urlparse(path)
        if parsed_url.scheme in ["http", "https"]:
            # Remove host
            parsed_url = parsed_url._replace(netloc="")
            parsed_url = parsed_url._replace(path=parsed_url.path[1:])

        # Remove the schema
        parsed_url = parsed_url._replace(scheme="")

        # Reconstruct the URL without the schema
        url_without_schema = urlunparse(parsed_url)

        # Remove //
        if url_without_schema[:2] == "//":
            url_without_schema = url_without_schema.replace("//", "", 1)

        return_path = unquote(url_without_schema)
        if return_path[-1] != "/":
            return_path += "/"
        return return_path

    @staticmethod
    def merge_status(old_stat: ExecutionStatus, new_stat: ExecutionStatus) -> ExecutionStatus:
        """
        Merge two JobStatus values by returning the one with the lower numeric code (i.e., higher severity).
        If the old status is more severe, it is returned; otherwise, the new status is returned.
        """
        status_codes = {
            ExecutionStatus.FAILED: 1,
            ExecutionStatus.COMPLETED_WITH_ERRORS: 2,
            ExecutionStatus.COMPLETED_WITH_WARNINGS: 3,
            ExecutionStatus.CANCELED: 4,
            ExecutionStatus.CANCELING: 5,
            ExecutionStatus.PAUSED: 6,
            ExecutionStatus.RESUMING: 7,
            ExecutionStatus.RUNNING: 8,
            ExecutionStatus.STARTING: 9,
            ExecutionStatus.QUEUED: 10,
            ExecutionStatus.COMPLETED: 1000
        }
        if status_codes[old_stat] < status_codes[new_stat]:
            return old_stat
        else:
            return new_stat

    @staticmethod
    def get_feature(name, description, type, available_for_filter=False, available_for_vector_db=False,
                    mandatory_for_vector_db=False):
        return {
            TransformConstants.NAME: name,
            TransformConstants.DESCRIPTION: description,
            TransformConstants.TYPE: type,
            TransformConstants.AVAILABLE_FOR_FILTER: available_for_filter,
            TransformConstants.AVAILABLE_FOR_VECTOR_DB: available_for_vector_db,
            TransformConstants.MANDATORY_FOR_VECTOR_DB: mandatory_for_vector_db
        }

    @staticmethod
    def find_skipped_docs(
            input_table: pa.Table,
            output_table: pa.Table,
            reason: str
    ) -> Dict[str, Any]:
        """
        Compares two PyArrow tables to find documents that are in the input but not
        in the output, and returns them in a structured format.

        Args:
            input_table: The PyArrow Table with the initial set of documents.
                         Must contain both the ID and Name columns.
            output_table: The PyArrow Table with the processed documents.
            reason: A string explaining why these documents were skipped.
            id_column_name: The name of the column containing the document IDs.
            name_column_name: The name of the column containing the document names.

        Returns:
            A dictionary containing:
            A dictionary containing:
            - 'skipped_docs': A list of DocsStructure dictionaries for skipped items.
            - 'skipped_docs_count': The integer count of skipped items.
        """
        # 1. Get the set of IDs from the output table for fast lookup.
        output_ids_set = set(output_table.column(TransformConstants.ID).to_pylist())

        # 2. Get the ID and Name columns from the input table.
        input_ids = input_table.column(TransformConstants.ID).to_pylist()
        input_names = input_table.column(TransformConstants.NAME).to_pylist()

        # 3. Iterate through the input data and build the list of skipped docs.
        skipped_docs_list: List[DocsStructure] = []
        for doc_id, doc_name in zip(input_ids, input_names):
            # If the ID from the input is NOT in the output set, it was skipped.
            if doc_id not in output_ids_set:
                skipped_docs_list.append({
                    "id": doc_id,
                    "name": doc_name,
                    "reason": reason
                })

        return {
            Metrics.External.SKIPPED_DOCS: skipped_docs_list,
            Metrics.External.SKIPPED_DOCS_COUNT: len(skipped_docs_list)
        }
