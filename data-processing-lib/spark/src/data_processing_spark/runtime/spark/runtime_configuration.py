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
from data_processing.runtime import TransformRuntimeConfiguration
from data_processing.transform import TransformConfiguration
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
        super().__init__(transform_config=transform_config)
        self.runtime_class = runtime_class

    def get_bcast_params(self, data_access_factory: DataAccessFactoryBase) -> dict[str, Any]:
        """Allows retrieving and broadcasting to all the workers very large
        configuration parameters, like the list of document IDs to remove for
        fuzzy dedup, or the list of blocked web domains for block listing.  This
        function is called by the spark runtime after spark initialization, and
        before spark_context.parallelize()
        :param data_access_factory - creates data_access object to download the large config parameter
        """
        return {}

    def create_transform_runtime(self) -> DefaultSparkTransformRuntime:
        """
        Create transform runtime with the parameters captured during apply_input_params()
        :return: transform runtime object
        """
        return self.runtime_class(self.transform_config.get_transform_params())
