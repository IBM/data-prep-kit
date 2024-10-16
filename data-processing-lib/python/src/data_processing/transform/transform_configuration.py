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

from argparse import ArgumentParser
from typing import Any

from data_processing.transform import AbstractTransform
from data_processing.utils import CLIArgumentProvider


class TransformConfiguration(CLIArgumentProvider):
    """
    This is a base transform configuration class defining transform's input/output parameter
    """

    def __init__(
        self, name: str, transform_class: type[AbstractTransform], remove_from_metadata: list[str] = []
    ):
        """
        Initialization
        :param name: transformer name
        :param transform_class: transform implementation class
        :param remove_from_metadata - list of parameters to remove from metadata
        """
        self.name = name
        self.transform_class = transform_class
        self.remove_from_metadata = remove_from_metadata
        self.params = {}

    def get_transform_class(self) -> type[AbstractTransform]:
        """
        Get the class extending AbstractBinaryTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime get_transform_config() method.
        :return: class extending AbstractBinaryTransform
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
