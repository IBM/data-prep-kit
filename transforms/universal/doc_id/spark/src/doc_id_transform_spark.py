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

import ast
import json
from argparse import ArgumentParser, Namespace
from typing import Any

from data_processing.transform import TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing_spark.runtime.spark.runtime_config import (
    SparkTransformRuntimeConfiguration,
)
from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id


logger = get_logger(__name__)


short_name = "doc_id"
cli_prefix = short_name + "_"

doc_id_column_name_key = "column_name"
""" name of the column that holds the generated document id """

doc_id_column_name_cli_param = f"{cli_prefix}{doc_id_column_name_key}"
""" name of the column that holds the generated document id """

captured_arg_keys = [doc_id_column_name_key]
""" The set of keys captured from the command line """

# defaults
doc_id_column_name_default = "doc_id"
""" The default name of the column that holds the generated document id """


class DocIDTransform(AbstractSparkTransform):
    """
    Implements Spark document ID generation - assign each row in a Spark DataFrame
    a unique integer ID
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, FilterTransformRuntime.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of FilterTransformConfiguration class
        super().__init__(config)
        self.column_name = config.get(doc_id_column_name_key, doc_id_column_name_default)

    def transform(self, data: DataFrame) -> tuple[list[DataFrame], dict[str, Any]]:
        """
        This implementation filters the input Spark dataframe using a SQL
        statement and returns the filtered table and execution stats
        :param data: input Spark DataFrame
        :return: list of output Spark DataFrames and custom statistics
        """
        # initialize the metadata dictionary
        total_docs = data.count()
        total_columns = len(data.columns)
        metadata = {
            "total_docs_count": total_docs,
            "total_columns_count": total_columns,
        }

        doc_id_df = data.withColumn(self.column_name, monotonically_increasing_id())

        # add global filter stats to metadata
        metadata["docs_after_doc_id"] = doc_id_df.count()
        metadata["columns_after_doc_id"] = len(doc_id_df.columns)

        return [doc_id_df], metadata


class DocIDTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=DocIDTransform,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the DocIDTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """

        parser.add_argument(
            f"--{doc_id_column_name_cli_param}",
            type=str,
            required=False,
            default="doc_id",
            help="name of the column that holds the generated document ids",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        # Capture the args that are specific to this transform
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        return True


class DocIDSparkRuntimeConfiguration(SparkTransformRuntimeConfiguration):
    """
    Implements the SparkTransformConfiguration for DocID as required by the
    SparkTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(transform_config=DocIDTransformConfiguration())


if __name__ == "__main__":
    launcher = SparkTransformLauncher(DocIDSparkRuntimeConfiguration())
    logger.info("Launching doc_id transform")
    launcher.launch()
