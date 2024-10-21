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

import time

from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.runtime.pure_python import (
    PythonTransformExecutionConfiguration,
    PythonTransformRuntimeConfiguration,
    orchestrate,
)
from data_processing.runtime.transform_launcher import AbstractTransformLauncher
from data_processing.utils import get_logger


logger = get_logger(__name__)


class PythonTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: PythonTransformRuntimeConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        super().__init__(runtime_config, data_access_factory)
        self.execution_config = PythonTransformExecutionConfiguration(name=runtime_config.get_name())

    def _submit_for_execution(self) -> int:
        """
        Submit for execution
        :return:
        """
        res = 1
        start = time.time()
        try:
            logger.debug("Starting orchestrator")
            res = orchestrate(
                data_access_factory=self.data_access_factory,
                runtime_config=self.runtime_config,
                execution_config=self.execution_config,
            )
            logger.debug("Completed orchestrator")
        except Exception as e:
            logger.info(f"Exception running orchestration\n{e}")
        finally:
            logger.info(f"Completed execution in {round((time.time() - start)/60., 3)} min, execution result {res}")
            return res
