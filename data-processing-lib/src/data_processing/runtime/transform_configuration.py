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

from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider


class TransformRuntimeConfiguration(CLIArgumentProvider):
    def __init__(self, base_configuration: TransformConfiguration):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        self.base_configuration = base_configuration

    def add_input_params(self, parser: ArgumentParser) -> None:
        self.base_configuration.add_input_params(parser)

    def apply_input_params(self, args: Namespace) -> bool:
        return self.base_configuration.apply_input_params(args)

    def get_input_params(self) -> dict[str, Any]:
        return self.base_configuration.get_input_params()

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Get the class extending AbstractTableTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime get_transform_config() method.
        :return: class extending AbstractTableTransform
        """
        return self.base_configuration.get_transform_class()

    def get_name(self):
        return self.base_configuration.get_name()

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Get transform metadata. Before returning remove all parameters key accumulated in
        self.remove_from metadata. This allows transform developer to mark any input parameters
        that should not make it to the metadata. This can be parameters containing sensitive
        information, access keys, secrets, passwords, etc
        :return parameters for metadata:
        """
        return self.base_configuration.get_transform_metadata()

    def get_transform_params(self) -> dict[str, Any]:
        """
         Get transform parameters
        :return: transform parameters
        """
        return self.base_configuration.get_transform_params()
