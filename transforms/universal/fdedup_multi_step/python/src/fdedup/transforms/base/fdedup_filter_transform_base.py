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
    RANDOM_SEED,
    CLIArgumentProvider,
    TransformUtils,
)
from data_processing.utils import UnrecoverableException

# configuration parameters
short_name = "fdedup_filter"
cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
int_column_name_key = "doc_id_column"
cluster_column_name_key = "cluster_column"
removed_docs_column_name_key = "removed_docs_column"
doc_id_snapshot_directory_key = "docid_snapshot_directory"
doc_id_cache_key = "doc_id_cache"

filter_doc_column_name_cli_param = f"{cli_prefix}{doc_column_name_key}"
filter_int_column_name_cli_param = f"{cli_prefix}{int_column_name_key}"
filter_cluster_column_name_cli_param = f"--{cli_prefix}{cluster_column_name_key}"
filter_removed_docs_column_name_cli_param = f"--{cli_prefix}{removed_docs_column_name_key}"
filter_doc_id_snapshot_directory_cli_param = f"--{cli_prefix}{doc_id_snapshot_directory_key}"


class FdedupFilterTransformBase(AbstractTableTransform):
    """
    Implements fuzzy dedup data preprocessor (building tables, minhashes and buckets).
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters, with the following keys
            doc_column - name of doc column
            doc_id_int_column - name of int doc id column
            doc_id_cache - doc id cache
        """
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        super().__init__(config)
        self.doc_column = config.get(doc_column_name_key, "contents")
        self.doc_id_column = config.get(int_column_name_key, "int_document_id")
        self.cluster_column = config.get(cluster_column_name_key, "cluster")
        self.removed_column = config.get(removed_docs_column_name_key, "removed")
        self.doc_id_cache = config.get(doc_id_cache_key, None)
        if self.doc_id_cache is None:
            raise UnrecoverableException("Doc_id cache is not provided")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Preprocessing table content.
        :param table: table
        :param file_name - name of currently processed file
        :return: resulting table, statistics
        """
        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column, self.doc_id_column])
        # inner variables
        ids = table.column(self.doc_id_column)
        unique = self._get_unique_ids(ids)
        # Filter out table
        mask = []
        clusters = []
        removed =[]
        # Actual filtering
        for n in range(table.num_rows):
            doc_id = ids[n].as_py()
            if doc_id in unique:
                mask.append(True)
                clusters.append(unique.pop(doc_id))
            else:
                mask.append(False)
                removed.append(doc_id)
        # build out table
        out_table = TransformUtils.add_column(table=table.filter(mask), name=self.cluster_column, content=clusters)
        # populate removed columns
        if out_table.num_rows > 0:
            # we can only add removed if the file is not empty
            removed_column = [[]] * out_table.num_rows
            removed_column[0] = removed
            out_table = TransformUtils.add_column(table=out_table, name=self.removed_column, content=removed_column)
        # build execution statistics
        stats = {"source_documents": table.num_rows, "result_documents": out_table.num_rows}
        return [out_table], stats

    def _get_unique_ids(self, ids: list[int]) -> dict[int, int]:
        """
        Get unique IDs
        :param ids: table ids
        :return: unique ids and clusters
        """
        raise NotImplementedError


class FdedupFilterTransformConfigurationBase(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self, transform_class: type[AbstractTableTransform]):
        super().__init__(
            name=short_name,
            transform_class=transform_class,
        )
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument(
            f"--{filter_doc_column_name_cli_param}",
            type=str,
            default="contents",
            help="document column name")
        parser.add_argument(
            f"--{filter_int_column_name_cli_param}",
            type=str,
            default="int_document_id",
            help="integer document id column name"
        )
        parser.add_argument(
            f"--{filter_cluster_column_name_cli_param}",
            type=str,
            default="cluster",
            help="cluster column name"
        )
        parser.add_argument(
            f"--{filter_removed_docs_column_name_cli_param}",
            type=str,
            default="removed",
            help="removed documents column name"
        )
        parser.add_argument(
            f"--{filter_doc_id_snapshot_directory_cli_param}",
            type=str,
            default=None,
            help="ID snapshot directory"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True
