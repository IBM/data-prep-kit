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
from data_processing.transform import AbstractTransform, TransformStatistics
from data_processing.utils import UnrecoverableException


class PythonTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the worker class processing of a single file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: TransformStatistics,
        transform_params: dict[str, Any],
        transform_class: type[AbstractTransform],
        is_folder: bool,
    ):
        """
        Init method
        :param data_access_factory - data access factory
        :param statistics - reference to statistics class
        :param transform_params - transform parameters
        :param transform_class: transform class
        :param is_folder: folder transform flag
        """
        # invoke superclass
        super().__init__(
            data_access_factory=data_access_factory,
            transform_parameters=dict(transform_params),
            is_folder=is_folder,
        )
        self.transform_params["statistics"] = statistics
        # Create local processor
        try:
            self.transform = transform_class(self.transform_params)
        except Exception as e:
            self.logger.error(f"Exception creating transform  {e}")
            raise UnrecoverableException("failed creating transform")
        # Create statistics
        self.stats = statistics

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        self.stats.add_stats(stats)


class PythonPoolTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the worker class processing of a single file
    """

    def __init__(
        self,
        data_access_factory: DataAccessFactoryBase,
        transform_params: dict[str, Any],
        transform_class: type[AbstractTransform],
        is_folder: bool
    ):
        """
        Init method
        :param data_access_factory - data access factory
        :param transform_params - transform parameters
        :param transform_class: transform class
        :param is_folder: folder tranform flag
        """
        super().__init__(
            data_access_factory=data_access_factory,
            transform_parameters=dict(transform_params),
            is_folder=is_folder,
        )
        # Add data access and statistics to the processor parameters
        self.transform_params["data_access"] = self.data_access
        self.transform_class = transform_class
        self.transform = None

    def process_file(self, f_name: str) -> dict[str, Any]:
        # re initialize statistics
        self.stats = {}
        if self.transform is None:
            # create transform. Make sure to do this locally
            try:
                self.transform = self.transform_class(self.transform_params)
            except Exception as e:
                self.logger.error(f"Exception creating transform  {e}")
                raise UnrecoverableException("failed creating transform")
        # Invoke superclass method
        super().process_file(f_name=f_name)
        # return collected statistics
        return self.stats

    def flush(self) -> dict[str, Any]:
        # re initialize statistics
        self.stats = {}
        # Invoke superclass method
        super().flush()
        # return collected statistics
        return self.stats

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publish statistics (to the local dictionary)
        :param stats: statistics dictionary
        :return: None
        """
        for key, val in stats.items():
            # for all key/values
            self.stats[key] = self.stats.get(key, 0) + val
