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
from doc_id_transform_base import IDGenerator, DocIDTransformBase, DocIDTransformConfigurationBase


class DocIDPythonTransform(DocIDTransformBase):
    """
    Implements schema modification of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)

    def get_starting_id(self, n_rows: int) -> int:
        """
        Get starting Id
        :param n_rows - number of rows in the table
        :return: starting id for the table
        """
        self.id_generator.get_ids(n_rows=n_rows)


class DocIDPythonTransformConfiguration(TransformConfiguration):

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

