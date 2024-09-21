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

from data_processing.transform import TransformConfiguration
from data_processing.transform.pure_python import PythonPipelineTransform
from data_processing.utils import get_logger

logger = get_logger(__name__)


class PipelineTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(
            name="pipeline",
            transform_class=PythonPipelineTransform,
        )
        self.params = config

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        for t in self.params["transforms"]:
            t.transform_config.add_input_params(parser=parser)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        res = True
        for t in self.params["transforms"]:
            res = res and t.transform_config.apply_input_params(args=args)
        return res

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        params = {}
        for t in self.params["transforms"]:
            params |= t.transform_config.get_input_params()
        return params

    def get_transform_metadata(self) -> dict[str, Any]:
        """
        Get transform metadata. Before returning remove all parameters key accumulated in
        self.remove_from metadata. This allows transform developer to mark any input parameters
        that should not make it to the metadata. This can be parameters containing sensitive
        information, access keys, secrets, passwords, etc.
        :return parameters for metadata:
        """
        params = {}
        for t in self.params["transforms"]:
            params |= t.transform_config.get_transform_metadata()
        return params
