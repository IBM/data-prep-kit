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

from typing import Any, TypeVar

import pyarrow as pa
from data_processing.transform import AbstractBinaryTransform
from data_processing.utils import TransformUtils, get_logger


logger = get_logger(__name__)


class AbstractTableTransform(AbstractBinaryTransform[pa.Table]):
    """
    Extends AbstractBinaryTransform to expect the byte arrays from to contain a pyarrow Table.
    Sub-classes are expected to implement transform() on the parsed Table instances.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        super().__init__(config)

    def transform_binary(self, byte_array: bytes, ext: str) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :param byte_array: input file
        :param ext: file extension
        :return: a tuple of a list of 0 or more converted file and a dictionary of statistics that will be
                 propagated to metadata
        """
        # validate extension
        if ext != ".parquet":
            logger.warning(f"Get wrong file type {ext}")
            return [], {"wrong file type": 1}
        # convert to table
        table = TransformUtils.convert_binary_to_arrow(data=byte_array)
        if table is None:
            logger.warning("Transformation of file to table failed")
            return [], {"failed_reads": 1}
        # Ensure that table is not empty
        if table.num_rows == 0:
            logger.warning(f"table is empty, skipping processing")
            return [], {"skipped empty tables": 1}
        # transform table
        out_tables, stats = self.transform(table=table)
        # Add number of rows to stats
        stats = stats | {"source_doc_count": table.num_rows}
        # convert tables to files
        return self._check_and_convert_tables(
            out_tables=out_tables, stats=stats | {"source_doc_count": table.num_rows}
        )

    # def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
    #     """
    #     Converts input table into an output table.
    #     If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
    #     :param table: input table
    #     :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
    #     propagated to metadata
    #     """
    #     raise NotImplemented()

    def flush_binary(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics. The majority of transformers
        should use default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :return: a tuple of a list of 0 or more converted file and a dictionary of statistics that will be
                 propagated to metadata
        """
        out_tables, stats = self.flush()
        return self._check_and_convert_tables(out_tables=out_tables, stats=stats)

    def flush(self) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of tables, for example coalesce.
        These transformers can have buffers containing tables that were not written to the output. Flush is
        the hook for them to return back locally stored tables and their statistics. The majority of transformers
        should use default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed when running in Ray.
        :return: a tuple of a list of 0 or more converted tables and a dictionary of statistics that will be
        propagated to metadata
        """
        return [], {}

    @staticmethod
    def _check_and_convert_tables(
        out_tables: list[pa.Table], stats: dict[str, Any]
    ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:

        out_files = [tuple[bytes, str]] * len(out_tables)
        out_docs = 0
        for i in range(len(out_tables)):
            if not TransformUtils.verify_no_duplicate_columns(table=out_tables[i], file=""):
                logger.warning("Transformer created file with the duplicate columns")
                return [], {"duplicate columns result": 1}
            out_binary = TransformUtils.convert_arrow_to_binary(table=out_tables[i])
            if out_binary is None:
                logger.warning("Failed to convert table to binary")
                return [], {"failed_writes": 1}
            out_docs += out_tables[i].num_rows
            out_files[i] = (out_binary, ".parquet")
        return out_files, stats | {"result_doc_count": out_docs}
