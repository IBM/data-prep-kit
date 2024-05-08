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
from typing import Any
from argparse import ArgumentParser, Namespace
from data_processing.transform import AbstractTableTransform, LauncherConfiguration
from data_processing.utils import CLIArgumentProvider


class PythonLauncherConfiguration(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer runtime
    It holds the following:
        1) The type of the concrete AbstractTransform class, that is created by a the worker with a
            dictionary of parameters to perform that table transformations.
    Sub-classes may extend this class to override the following:
        1) add_input_params() to add CLI argument definitions used in creating TransformRuntime.
    """

    def __init__(
            self,
            name: str,
            transform_class: type[AbstractTableTransform],
            launcher_configuration: LauncherConfiguration,
            remove_from_metadata: list[str] = [],
    ):
        """
        Initialization
        :param launcher_configuration: base transform configuration class
        :param transform_class: implementation of the Filter
        :return:
        """
        self.name = name
        self.transform_class = transform_class
        # These are expected to be updated later by the sub-class in apply_input_params().
        self.params = {}
        self.remove_from_metadata = remove_from_metadata
        self.base = launcher_configuration

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add input parameters. Delegates to the base class
        :param parser: parser
        :return: None
        """
        return self.base.add_input_params(parser=parser)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply input parameters. Delegate to base class
        :param args: arguments
        :return: True, if parameters are valid, False otherwise
        """
        is_valid = self.base.apply_input_params(args=args)
        if is_valid:
            self.params = self.base.params
        return is_valid

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Get the class extending AbstractTableTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime get_transform_config() method.
        :return: class extending AbstractTableTransform
        """
        return self.transform_class

    def get_name(self):
        return self.name

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Get transform metadata. Before returning remove all parameters key accumulated in
        self.remove_from metadata. This allows transform developer to mark any input parameters
        that should not make it to the metadata. This can be parameters containing sensitive
        information, access keys, secrets, passwords, etc
        :return parameters for metadata:
        """
        # get input parameters
        parameters = self.get_input_params()
        # remove everything that user marked as to be removed
        for key in self.remove_from_metadata:
            del self.params[key]
        return parameters

    def get_transform_params(self) -> dict[str, Any]:
        """
         Get transform parameters
        :return: transform parameters
        """
        return self.params
