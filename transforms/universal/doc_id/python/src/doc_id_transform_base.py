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
from data_processing.utils import CLIArgumentProvider, TransformUtils, UnrecoverableException


class IDGenerator():
    """
    A class maintaining unique integer ids
    """

    def __init__(self, start: int=0):
        """
        Initialization
        :param start: starting id number
        """
        self.id = start

    def get_ids(self, n_rows: int) -> int:
        """
        Give out a new portion of integer ids
        :param n_rows: number of required Ids
        :return: starting value of blocks of ids
        """
        start_id = self.id
        self.id = self.id + n_rows
        return start_id

    def get_current(self) -> int:
        """
        Give out a new portion of integer ids
        :return: current value for ID
        """
        return self.id


short_name = "doc_id"
cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
hash_column_name_key = "hash_column"
int_column_name_key = "int_column"
start_id_key = "start_id"
id_generator_key = "id_generator"

doc_column_name_cli_param = f"{cli_prefix}{doc_column_name_key}"
hash_column_name_cli_param = f"{cli_prefix}{hash_column_name_key}"
int_column_name_cli_param = f"{cli_prefix}{int_column_name_key}"
start_id_cli_param = f"{cli_prefix}{start_id_key}"

doc_column_name_default = "contents"


class DocIDTransformBase(AbstractTableTransform):
    """
    Implements schema modification of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)
        self.doc_column = config.get(doc_column_name_key, doc_column_name_default)
        self.hash_column = config.get(hash_column_name_key, None)
        self.int_column = config.get(int_column_name_key, None)
        if self.hash_column is None and self.int_column is None:
            raise UnrecoverableException("At least one of hash or integer column names must be specified.")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        TransformUtils.validate_columns(table=table, required=[self.doc_column])

        if self.hash_column is not None:
            # add doc id column
            docs = table[self.doc_column]
            doc_ids = [""] * table.num_rows
            for n in range(table.num_rows):
                doc_ids[n] = TransformUtils.str_to_hash(docs[n].as_py())
            table = TransformUtils.add_column(table=table, name=self.hash_column, content=doc_ids)
        if self.int_column is not None:
            # add integer document id
            sid = self._get_starting_id(table.num_rows)
            int_doc_ids = list(range(sid, table.num_rows + sid))
            table = TransformUtils.add_column(table=table, name=self.int_column, content=int_doc_ids)
        return [table], {}

    def _get_starting_id(self, n_rows: int) -> int:
        """
        Get starting Id
        :param n_rows - number of rows in the table
        :return: starting id for the table
        """
        raise NotImplementedError


class DocIDTransformConfigurationBase(TransformConfiguration):

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
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{doc_column_name_cli_param}",
            type=str,
            default=doc_column_name_default,
            help="doc column name"
        )
        parser.add_argument(
            f"--{hash_column_name_cli_param}",
            type=str,
            default=None,
            help="Compute document hash and place in the given named column",
        )
        parser.add_argument(
            f"--{int_column_name_cli_param}",
            type=str,
            default=None,
            help="Compute unique integer id and place in the given named column",
        )
        parser.add_argument(
            f"--{start_id_cli_param}",
            type=int,
            default=0,
            help="starting integer id",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(hash_column_name_key) is None and captured.get(int_column_name_key) is None:
            self.logger.info("One of hash or int id column names must be specified.")
            return False

        self.params = self.params | captured
        self.logger.info(f"Doc id parameters are : {self.params}")
        return True