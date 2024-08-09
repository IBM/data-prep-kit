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
from data_processing.runtime import AbstractTransformFileProcessor
from data_processing.runtime.pure_python import PythonTransformRuntimeConfiguration
from data_processing.transform import TransformStatistics


class PythonTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the worker class processing of a single file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: TransformStatistics,
        runtime_configuration: PythonTransformRuntimeConfiguration,
    ):
        """
        Init method
        :param data_access_factory - data access factory
        :param statistics - reference to statistics class
        :param runtime_configuration: transform configuration class
        """
        # invoke superclass
        super().__init__(
            data_access_factory=data_access_factory,
            transform_parameters=dict(runtime_configuration.get_transform_params()),
        )
        self.transform_params["statistics"] = statistics
        # Create local processor
        self.transform = runtime_configuration.get_transform_class()(self.transform_params)
        # Create statistics
        self.stats = statistics

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        self.stats.add_stats(stats)
