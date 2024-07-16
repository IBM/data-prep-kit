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
import ray
from data_processing.data_access import DataAccessFactoryBase
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle


@ray.remote(num_cpus=0.25, scheduling_strategy="SPREAD")
class IDGenerator(object):
    """
    An actor maintaining unique integer ids
    """

    def __init__(self):
        """
        Initialization
        """
        self.id = 1

    def get_ids(self, n_rows: int) -> int:
        """
        Give out a new portion of integer ids
        :param n_rows: number of required Ids
        :return: starting value of blocks of ids
        """
        start_id = self.id
        self.id = self.id + n_rows
        return start_id


short_name = "doc_id"
cli_prefix = f"{short_name}_"
doc_column_name_key = "doc_column"
hash_column_name_key = "hash_column"
int_column_name_key = "int_column"
_id_generator_key = "_id_generator"

doc_column_name_cli_param = f"{cli_prefix}{doc_column_name_key}"
hash_column_name_cli_param = f"{cli_prefix}{hash_column_name_key}"
int_column_name_cli_param = f"{cli_prefix}{int_column_name_key}"

doc_column_name_default = "contents"


class DocIDTransform(AbstractTableTransform):
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
        self.id_generator = config.get(_id_generator_key, None)
        if self.hash_column is None and self.int_column is None:
            raise RuntimeError("At least one of hash or integer column names must be specified.")
        if self.id_generator is None and self.int_column is not None:
            raise RuntimeError(
                "Integer id generation requested, but there is no id generating actor defined (are we running Ray?)."
            )

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
            sid = ray.get(self.id_generator.get_ids.remote(table.num_rows))
            int_doc_ids = list(range(sid, table.num_rows + sid))
            table = TransformUtils.add_column(table=table, name=self.int_column, content=int_doc_ids)
        return [table], {}


class DocIDRuntime(DefaultRayTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            hash_column - name of doc id column to create
            int_column - name of integer doc id column to create
        """
        super().__init__(params)

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - statistics actor reference
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        # create id generator
        return {_id_generator_key: IDGenerator.remote()} | self.params


class DocIDTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=DocIDTransform,
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
            f"--{doc_column_name_cli_param}", type=str, default=doc_column_name_default, help="doc column name"
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


class DocIDPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=DocIDTransformConfiguration())


class DocIDRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=DocIDTransformConfiguration(), runtime_class=DocIDRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(DocIDRayTransformConfiguration())
    launcher.launch()
