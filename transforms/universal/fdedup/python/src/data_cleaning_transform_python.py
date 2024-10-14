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

import os
from typing import Any

from data_cleaning_transform import DataCleaningTransformConfiguration
from data_processing.data_access import DataAccessFactoryBase
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    DefaultPythonTransformRuntime,
    PythonTransformRuntimeConfiguration,
)
from data_processing.transform import TransformStatistics
from data_processing.utils import get_logger


logger = get_logger(__name__)


class DataCleaningPythonRuntime(DefaultPythonTransformRuntime):
    """
    Data cleaning runtime support for Python
    """

    def __init__(self, params: dict[str, Any]):
        super().__init__(params=params)
        self.logger = get_logger(__name__)

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics, files: list[str]
    ) -> dict[str, Any]:
        """
        Download the table of duplicate document ids that will be provided to the
        filtering/annotation method. This is the opportunity for this runtime to
        create a new set of configuration based on the config/params provided to
        this instance's initializer. This may include the addition of new
        configuration data such as ray shared memory, new actors, etc., that
        might be needed and expected by the transform in its initializer and/or
        transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        data_access = data_access_factory.create_data_access()
        duplicate_list_location = os.path.abspath(
            os.path.join(data_access.output_folder, "..", self.params["duplicate_list_location"])
        )
        if duplicate_list_location.startswith("s3://"):
            _, duplicate_list_location = duplicate_list_location.split("://")
        self.duplicate_list, retries = data_access.get_file(duplicate_list_location)
        return self.params | {"df": self.duplicate_list}


class DataCleaningPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for fuzzy dedup data cleaning step
    as required by the PythonTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        :param: transform_configuration - transform configuration class
        :param: runtime_class - name of the runtime configuration class
        """
        super().__init__(
            transform_config=DataCleaningTransformConfiguration(),
            runtime_class=DataCleaningPythonRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(DataCleaningTransformConfiguration())
    logger.info("Launching fuzzy dedup data cleaning transform")
    launcher.launch()
