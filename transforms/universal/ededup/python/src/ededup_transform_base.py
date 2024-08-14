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
from data_processing.utils import GB, CLIArgumentProvider, TransformUtils


REQUEST_LEN = 8192
short_name = "ededup"
cli_prefix = f"{short_name}_"


class HashFilter:
    """
    Implements hash
    """

    def __init__(self, params: dict[str, Any]):
        """
        initialize set of local hashes
        """
        self.hashes = set()

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
        self.doc_column = config.get("doc_column", "contents")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        De duping table content.
        :param table: table
        :param file_name: file name
        :return: resulting table, statistics
        """
        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column])
        # Inner variables
        hashes = set()
        unique = []
        hd = {}
        # Compute unique hashes for the table
        for text in table[self.doc_column]:
            # Compute doc hash
            h = TransformUtils.str_to_hash(TransformUtils.normalize_string(str(text)))
            if h not in hashes:  # Processing this hash for the first time
                hashes.add(h)  # Remember it locally
                hd[h] = str(text)
                if len(hd) >= REQUEST_LEN:  # time to check remotely
                    unique = unique + self._process_cached_hashes(hd=hd)
                    hd = {}
        if len(hd) > 0:  # Process remaining hashes
            unique = unique + self._process_cached_hashes(hd=hd)

        # Remove duplicates
        unique_set = set(unique)
        mask = [False] * table.num_rows
        index = 0
        for text in table[self.doc_column]:
            str_text = str(text)
            if str_text in unique_set:
                mask[index] = True
                unique_set.remove(str_text)
            index += 1
        # Create output table
        out_table = table.filter(mask)
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
        parser.add_argument(f"--{cli_prefix}doc_column", type=str, default="contents", help="key for accessing data")

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
