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
from data_processing.transform import TransformStatistics
from data_processing_spark.runtime.spark import SparkTransformRuntimeConfiguration
from data_processing.utils import UnrecoverableException


class SparkTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the actual work/actor processing of a single file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        runtime_configuration: SparkTransformRuntimeConfiguration,
        statistics: TransformStatistics,
        is_folder: bool,
    ):
        """
        Init method
        """
        super().__init__(
            data_access_factory=data_access_factory,
            transform_parameters=runtime_configuration.get_transform_params(),
            is_folder=is_folder,
        )
        # Add data access ant statistics to the processor parameters
        self.runtime_configuration = runtime_configuration
        self.transform = None
        # set up statistics
        self.transform_params["statistics"] = statistics
        self.stats = statistics

    def create_transform(self, transform_parameters: dict[str, Any]):
        """
        Create transform
        :param transform_parameters - transform parameters
        :return: None
        """
        # Create local processor
        try:
            self.transform = self.runtime_configuration.get_transform_class()(transform_parameters)
        except Exception as e:
            self.logger.error(f"Exception creating transform  {e}")
            raise UnrecoverableException("failed creating transform")

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publish statistics (to the local dictionary)
        :param stats: statistics dictionary
        :return: None
        """
        self.stats.add_stats(stats)
