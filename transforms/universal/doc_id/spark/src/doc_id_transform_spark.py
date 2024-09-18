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
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics
from data_processing.utils import CLIArgumentProvider, TransformUtils
from data_processing_spark.runtime.spark import SparkTransformLauncher
from data_processing_spark.runtime.spark import SparkTransformRuntimeConfiguration, DefaultSparkTransformRuntime


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
    Spark specific DocID transformer implementation
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
        # here we compute starting index for partition as a partition index times max 32 bit integer
        self.start_index = config.get("partition_index", 0) * 2147483647
        self.logger.debug(f"starting index {self.start_index}")
        if self.hash_column is None and self.int_column is None:
            raise RuntimeError("At least one of hash or integer column names must be specified.")

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
            int_doc_ids = list(range(self.start_index, table.num_rows + self.start_index))
            self.logger.debug(f"int ids {int_doc_ids}")
            self.start_index += table.num_rows
            table = TransformUtils.add_column(table=table, name=self.int_column, content=int_doc_ids)
        return [table], {}


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


class DocIDSparkTransformRuntime(DefaultSparkTransformRuntime):

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTansformConfiguration.
        """
        super().__init__(params)

        def get_transform_config(
                self, partition: int, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics
        ) -> dict[str, Any]:
            """
            Get the dictionary of configuration that will be provided to the transform's initializer.
            This is the opportunity for this runtime to create a new set of configuration based on the
            config/params provided to this instance's initializer.  This may include the addition
            of new configuration data such as ray shared memory, new actors, etc, that might be needed and
            expected by the transform in its initializer and/or transform() methods.
            :param data_access_factory - data access factory class being used by the RayOrchestrator.
            :param statistics - reference to statistics actor
            :return: dictionary of transform init params
            """
            return self.params | {"partition_index": partition}



class DocIDSparkTransformConfiguration(SparkTransformRuntimeConfiguration):
    """
    Implements the SparkTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=DocIDTransformConfiguration(), runtime_class=DocIDSparkTransformRuntime)


if __name__ == "__main__":
    launcher = SparkTransformLauncher(DocIDSparkTransformConfiguration())
    launcher.launch()
