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
from data_processing.transform import AbstractPipelineTransform
from data_processing.transform import TransformRuntimeConfiguration, BaseTransformRuntime


class PythonPipelineTransform(AbstractPipelineTransform):
    """
    Transform that executes a set of base transforms sequentially. Data is passed between
    participating transforms in memory
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initializes pipeline execution for the list of transforms
        :param config - configuration parameters
        :param transforms - list of transforms in the pipeline. Note that transforms will
        be executed
        """
        super().__init__(config)

    def _get_transform_params(self, runtime: BaseTransformRuntime) -> dict[str, Any]:
        """
        get transform parameters
        :param runtime - runtime
        :return: transform params
        """
        return runtime.get_transform_config(data_access_factory=self.data_access_factory,
                                            statistics=self.statistics, files=[])

    def _compute_execution_stats(self, runtime: BaseTransformRuntime, st: dict[str, Any]) -> None:
        """
        get transform parameters
        :param runtime - runtime
        :param st - statistics
        :return: None
        """
        self.statistics.add_stats(st)
        runtime.compute_execution_stats(stats=self.statistics)
        return
