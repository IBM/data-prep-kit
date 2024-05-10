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
import argparse
from argparse import ArgumentParser
from typing import Any

from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider


class TransformConfiguration(CLIArgumentProvider):
    """
    This is a base transform configuration class defining transform's input/output parameter
    """

    def __init__(self, name: str, transform_class: AbstractTableTransform, remove_from_metadata: list[str] = []):
        """
        Initialization
        """
        self.name = name
        self.transform_class = transform_class
        self.remove_from_metadata = remove_from_metadata
        self.params = {}


class TransformConfigurationProxy(TransformConfiguration):
    def __init__(self, proxied_transform_config: TransformConfiguration):
        self.proxied_transform_config = proxied_transform_config
        # Python probably has a better way of doing this using the proxied transform config
        self.name = proxied_transform_config.name
        self.transform_class = proxied_transform_config.transform_class
        self.remove_from_metadata = proxied_transform_config.remove_from_metadata
        self.params = {}

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        self.proxied_transform_config.add_input_params(parser)

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        is_valid = self.proxied_transform_config.apply_input_params(args)
        if is_valid:
            self.params = self.proxied_transform_config.params
        return is_valid

    def get_input_params(self) -> dict[str, Any]:
        return self.params


def get_transform_config(
    transform_configuration: TransformConfiguration, argv: list[str], parser: ArgumentParser = None
):
    """
    Create a transform configuration dictionary  using the given Configuration class and dictionary of
    values that should be treated as command line options.
    Example:

        config = self._get_transform_config(YourTransformConfiguration(), ...)\n
        transform = YourTransform(config)   \n

    :param transform_configuration: The configuration class used to define and apply input parameters.
        and the values are the command line values.
    :param parser: optional parser to use.  If not provided one is created internally.  if provided and argv
        contains args that will be parsed by the parser, then they will be in the returned dictionary.
    :param argv: list of parameters string
    :return:  the configuration dictionary as produced by the given transform configuration after all args
        have been defined and applied.
    """
    # Create and add our arguments to the parser
    if parser is None:
        parser = ArgumentParser()
    transform_configuration.add_input_params(parser)

    # Create the command line and parse it.
    if "python" in argv[0]:
        argv = argv[1:0]
    args = parser.parse_args(argv)
    dargs = vars(args)

    transform_configuration.apply_input_params(args)
    config = transform_configuration.get_input_params()  # This is the transform configuration, for this test.
    return dargs | config
