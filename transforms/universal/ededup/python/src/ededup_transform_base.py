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

import pickle
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.data_access import SnapshotUtils
from data_processing.transform import (
    AbstractTableTransform,
    TransformConfiguration,
)
from data_processing.utils import (
    GB,
    CLIArgumentProvider,
    TransformUtils,
    UnrecoverableException,
    get_logger,
    str2bool,
)


REQUEST_LEN = 8192
short_name = "ededup"
cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
int_column_name_key = "doc_id_column"
use_snapshot_key = "use_snapshot"
snapshot_directory_key = "snapshot_directory"
doc_column_name_cli_param = f"{cli_prefix}{doc_column_name_key}"
int_column_name_cli_param = f"{cli_prefix}{int_column_name_key}"
use_snapshot_cli_param = f"{cli_prefix}{use_snapshot_key}"
snapshot_directory_cli_param = f"--{cli_prefix}{snapshot_directory_key}"

class HashFilter:
    """
    Implements hash
    """

    def __init__(self, params: dict[str, Any]):
        """
        initialize set of local hashes
        """
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id", 1)
        data_access_factory = params.get("data_access_factory", None)
        if data_access_factory is None:
            self.data_access = None
            self.hashes = set()
        else:
            self.data_access = data_access_factory.create_data_access()
            snapshot = params.get("snapshot", None)
            if snapshot is None:
                self.hashes = set()
            else:
                try:
                    b_hashes, _ = self.data_access.get_file(snapshot)
                    self.hashes = pickle.loads(b_hashes)
                except Exception as e:
                    self.logger.warning(f"Failed to load hashes collector {self.actor_id} with exception {e}")
                    raise UnrecoverableException("failed to load hashes")

    def add_hashes(self, hashes: set[str]) -> None:
        """
        Adding hashes
        :param hashes: set of hashes to add
        :return: None
        """
        self.hashes.update(hashes)

    def get_unique(self, ha: list[str]) -> list[str]:
        """
        Get list of unique hashes
        :param ha: new set of hashes
        :return: list of unique ones
        """
        unique = []
        for h in ha:
            if h not in self.hashes:
                # If a hash does not exist, add it to unique and add to the local set
                self.hashes.add(h)
                unique.append(h)
        return unique

    def get_hash_size(self) -> tuple[int, float]:
        """
        Get size of created hashes for statistics
        :return: size of the local set and its memory footprint
        """
        return len(self.hashes), TransformUtils.deep_get_size(self.hashes) / GB

    def snapshot(self) -> None:
        """
        Snapshot content
        :return: None
        """
        try:
            # pickle content
            b_doc = pickle.dumps(self.hashes)
            # Save it
            self.data_access.save_file(
                f"{SnapshotUtils.get_snapshot_folder(self.data_access)}hash_collector_{self.actor_id}", b_doc
            )
        except Exception as e:
            self.logger.warning(f"Failed to snapshot doc collector {self.actor_id} with exception {e}")
            raise e


class EdedupTransformBase(AbstractTableTransform):
    """
    Implements dedup table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
        """
        super().__init__(config)
        self.doc_column = config.get(doc_column_name_key, "contents")
        self.doc_id_column = config.get(int_column_name_key, "document_id")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        De duping table content.
        :param table: table
        :param file_name: file name
        :return: resulting table, statistics
        """
        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column, self.doc_id_column])
        # Inner variables
        docs = table[self.doc_column]
        doc_ids = table[self.doc_id_column]
        hashes = set()
        unique = []
        hd = {}
        # Compute unique hashes for the table
        for n in range(table.num_rows):
            doc = docs[n].as_py()
            doc_id = doc_ids[n].as_py()
            # Compute doc hash
            h = TransformUtils.str_to_hash(TransformUtils.normalize_string(str(doc)))
            if h not in hashes:  # Processing this hash for the first time
                hashes.add(h)  # Remember it locally
                hd[h] = doc_id
                if len(hd) >= REQUEST_LEN:  # time to check remotely
                    unique = unique + self._process_cached_hashes(hd=hd)
                    hd = {}
        if len(hd) > 0:  # Process remaining hashes
            unique = unique + self._process_cached_hashes(hd=hd)

        # Remove duplicates
        unique_set = set(unique)
        mask = [False] * table.num_rows
        removed = []
        index = 0
        for id in table[self.doc_id_column]:
            str_id = str(id)
            if str_id in unique_set:
                mask[index] = True
                unique_set.remove(str_id)
            else:
                removed.append(str_id)
            index += 1
        # Create output table
        out_table = table.filter(mask)
        # populate removed columns
        if out_table.num_rows > 0:
            # we can only add removed if the file is not empty
            removed_column = [[]] * out_table.num_rows
            removed_column[0] = removed
            out_table = TransformUtils.add_column(table=out_table, name="removed", content=removed_column)
        # report statistics
        stats = {"source_documents": table.num_rows, "result_documents": out_table.num_rows}
        return [out_table], stats

    def _process_cached_hashes(self, hd: dict[str, str]) -> list[str]:
        """
        check hashes uniqueness with the distributed cache of hashes
        :param hd: dictionary of hash to document
        :return: unique documents
        """
        raise NotImplementedError


class EdedupTransformConfigurationBase(TransformConfiguration):
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
            f"--{doc_column_name_cli_param}",
            type=str,
            default="contents",
            help="name of the column containing document")
        parser.add_argument(
            f"--{int_column_name_cli_param}",
            type=str,
            default="document_id",
            help="name of the column containing document id"
        )
        parser.add_argument(
            f"--{use_snapshot_cli_param}",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="flag to continue from snapshot",
        )
        # by default, snapshot file is from the output directory. This parameter can overwrite
        # default location by explicitly defining the snapshot directory
        parser.add_argument(
            f"--{snapshot_directory_cli_param}", type=str, default=None, help="location of snapshot files"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"exact dedup params are {self.params}")
        return True
