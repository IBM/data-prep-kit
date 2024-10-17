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
import time
from typing import Any

from data_processing.data_access import DataAccess
from data_processing.runtime.pure_python import (
    DefaultPythonTransformRuntime,
    PythonTransformLauncher,
    PythonTransformRuntimeConfiguration,
)
from data_processing.utils import get_logger
from get_duplicate_list_transform import (
    GetDuplicateListTransformConfiguration,
    subfolder_key,
)


logger = get_logger(__name__)


class GetDuplicateListPythonRuntime(DefaultPythonTransformRuntime):
    """
    Get duplicate list runtime support for Python
    """

    def __init__(self, params: dict[str, Any]):
        super().__init__(params=params)
        self.logger = get_logger(__name__)

    def get_folders(self, data_access: DataAccess) -> list[str]:
        """
        Return the set of folders that will be processed by this transform
        :param data_access - data access object
        :return: list of folder paths
        """
        return [self.params[subfolder_key]]


class GetDuplicateListPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for Fuzzy Dedup GetDuplicateList
    as required by the PythonTransformLauncher.
    """

    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(
            transform_config=GetDuplicateListTransformConfiguration(),
            runtime_class=GetDuplicateListPythonRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(runtime_config=GetDuplicateListPythonTransformConfiguration())
    logger.info("Launching fuzzy dedup get duplicate list python transform")
    launcher.launch()
