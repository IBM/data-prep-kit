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
from typing import Any

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import AbstractTableTransform
from data_processing.utils import CLIArgumentProvider
from ray.actor import ActorHandle


class DefaultTableTransformRuntime:
    """
    Transformer runtime used by processor to to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTansformConfiguration.
        """
        self.params = params

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc, that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        return self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        return stats


class DefaultTableTransformConfiguration(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer running in the ray environment.
    It holds the following:
        1) The type of the concrete AbstractTransform class, that is created by a the ray worker with a
            dictionary of parameters to perform that table transformations.
        2) The type of the of DefaultTableTransformRuntime that supports operation of the transform
            on the ray orchestrator side.  It is create with an initializer that takes the dictionary
            of CLI arguments, optionally defined in this class.
    Sub-classes may extend this class to override the following:
        1) add_input_params() to add CLI argument definitions used in creating both the AbstractTransform
            and the DefaultTableTransformRuntime.
    """

    def __init__(
        self,
        name: str,
        transform_class: type[AbstractTableTransform],
        runtime_class: type[DefaultTableTransformRuntime] = DefaultTableTransformRuntime,
    ):
        """
        Initialization
        :param transform_class: implementation of the Filter
        :param runtime_class: implementation of the Filter runtime
        :return:
        """
        self.name = name
        self.runtime_class = runtime_class
        self.transform_class = transform_class
        # These are expected to be updated later by the sub-class in apply_input_params().
        self.params = {}
        self.remove_from_metadata = []

    def create_transform_runtime(self) -> DefaultTableTransformRuntime:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.params)

    def get_transform_class(self) -> type[AbstractTableTransform]:
        """
        Get the class extending AbstractTableTransform which implements a specific transformation.
        The class will generally be instantiated with a dictionary of configuration produced by
        the associated TransformRuntime's get_transform_config() method.
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
        :return:
        """
        # get input parameters
        parameters = self.get_input_params()
        # remove everything that user marked as to be removed
        for key in self.remove_from_metadata:
            del self.params[key]
        return parameters


def get_transform_config(
    transform_configuration: DefaultTableTransformConfiguration, argv: str, parser: argparse.ArgumentParser = None
):
    """
    Create a transform configuration dictionary  using the given Configuration class and dictionary of
    values that should be treated as command line options.
    Example:

        config = self._get_transform_config(YourTransformConfiguration(), ...)\n
        transform = YourTransform(config)   \n

    :param transform_configuration: The configuration class used to define and apply input parameters.
    :param command_line: dictionary of key/value pairs where the keys are the option names (.e.g. --block_list_foo)
        and the values are the command line values.
    :param parser: optional parser to use.  If not provided one is created internally.  if provided and argv
        contains args that will be parsed by the parser, then they will be in the returned dictionary.
    :return:  the configuration dictionary as produced by the given transform configuration after all args
        have been defined and applied.
    """
    # Create and add our arguments to the parser
    if parser is None:
        parser = argparse.ArgumentParser()
    transform_configuration.add_input_params(parser)

    # Create the command line and parse it.
    if "python" in argv[0]:
        argv = argv[1:0]
    args = parser.parse_args(argv)
    dargs = vars(args)

    transform_configuration.apply_input_params(args)
    config = transform_configuration.get_input_params()  # This is the transform configuration, for this test.
    return dargs | config
