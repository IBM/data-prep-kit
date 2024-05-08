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

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import (
    AbstractTableTransform,
    LauncherConfiguration,
)
from data_processing.pure_python import PythonLauncherConfiguration
from ray.actor import ActorHandle


class DefaultTableTransformRuntimeRay:
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


class RayLauncherConfiguration(PythonLauncherConfiguration):
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
        launcher_configuration: LauncherConfiguration,
        remove_from_metadata: list[str] = [],
        runtime_class: type[DefaultTableTransformRuntimeRay] = DefaultTableTransformRuntimeRay,
    ):
        super().__init__(
            name=name,
            transform_class=transform_class,
            launcher_configuration=launcher_configuration,
            remove_from_metadata=remove_from_metadata,
        )
        """
        Initialization
        :param transform_class: implementation of the transform
        :param runtime_class: implementation of the transform runtime
        :param base: base transform configuration class
        :param name: transform name
        :param remove_from_metadata: list of parameters to remove from metadata
        :return:
        """
        self.runtime_class = runtime_class

    def create_transform_runtime(self) -> DefaultTableTransformRuntimeRay:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.params)
