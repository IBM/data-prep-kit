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

from data_processing.data_access import DataAccessFactoryBase, DataAccess
from data_processing.transform import TransformStatistics


class DefaultSparkTransformRuntime:
    """
    Transformer runtime used by processor to to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a TableTansformConfiguration.
        """
        self.params = params

    def get_folders(self, data_access: DataAccess) -> list[str]:
        """
        Get folders to process
        :param data_access: data access
        :return: list of folders to process
        """
        raise NotImplemented()

    def get_transform_config(
        self, partition: int, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.
        :param partition - the partition assigned to this worker, needed by transforms like doc_id
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :return: dictionary of transform init params
        """
        return self.params

    def get_bcast_params(self, data_access_factory: DataAccessFactoryBase) -> dict[str, Any]:
        """Allows retrieving and broadcasting to all the workers very large
        configuration parameters, like the list of document IDs to remove for
        fuzzy dedup, or the list of blocked web domains for block listing. This
        function is called by the spark runtime after spark initialization, and
        before spark_context.parallelize()
        :param data_access_factory - creates data_access object to download the large config parameter
        """
        return {}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        This method does not return a value; the job execution statistics are generally reported
        as metadata by the Spark Orchestrator.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        """
        pass
