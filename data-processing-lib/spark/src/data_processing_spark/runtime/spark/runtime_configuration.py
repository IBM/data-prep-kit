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

from data_processing.transform import TransformConfiguration, TransformRuntimeConfiguration
from data_processing_spark.runtime.spark import DefaultSparkTransformRuntime


class SparkTransformRuntimeConfiguration(TransformRuntimeConfiguration):
    def __init__(
        self,
        transform_config: TransformConfiguration,
        runtime_class: type[DefaultSparkTransformRuntime] = DefaultSparkTransformRuntime,
    ):
        """
        Initialization
        :param transform_config - base configuration class
        :param runtime_class: implementation of the transform runtime
        """
        super().__init__(transform_config=transform_config, runtime_class=runtime_class)
