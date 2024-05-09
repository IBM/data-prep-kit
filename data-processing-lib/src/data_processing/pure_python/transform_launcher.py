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

import argparse
import time
import sys

from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.pure_python import orchestrate
from data_processing.transform import (
    TransformExecutionConfiguration,
)
from data_processing.pure_python import PythonLauncherConfiguration
from data_processing.utils import get_logger, str2bool


logger = get_logger(__name__)


class PythonTransformLauncher:
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        transform_runtime_config: PythonLauncherConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param transform_runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        self.name = transform_runtime_config.get_name()
        self.transform_runtime_config = transform_runtime_config
        self.data_access_factory = data_access_factory
        self.execution_config = TransformExecutionConfiguration(name=transform_runtime_config.get_name())

    def __get_parameters(self) -> bool:
        """
        This method creates arg parser, fill it with the parameters
        and does parameters validation
        :return: True id validation passe or False, if not
        """
        parser = argparse.ArgumentParser(
            description=f"Driver for {self.name} processing",
            # RawText is used to allow better formatting of ast-based arguments
            # See uses of ParamsUtils.dict_to_str()
            formatter_class=argparse.RawTextHelpFormatter,
        )
        parser.add_argument(
            "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="running ray local flag"
        )
        # add additional arguments
        self.transform_runtime_config.add_input_params(parser=parser)
        self.data_access_factory.add_input_params(parser=parser)
        self.execution_config.add_input_params(parser=parser)
        args = parser.parse_args()
        self.run_locally = args.run_locally
        return (
            self.transform_runtime_config.apply_input_params(args=args)
            and self.execution_config.apply_input_params(args=args)
            and self.data_access_factory.apply_input_params(args=args)
        )

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
                transform_config=self.transform_runtime_config,
                execution_config=self.execution_config,
            )
            logger.debug("Completed orchestrator")
        except Exception as e:
            logger.info(f"Exception running orchestration\n{e}")
        finally:
            logger.info(f"Completed execution in {(time.time() - start)/60.} min, execution result {res}")
            return res

    def launch(self) -> int:
        """
        Execute method orchestrates driver invocation
        :return:
        """
        if self.__get_parameters():
            res = self._submit_for_execution()
        else:
            res = 1
        if not self.run_locally:
            if res == 1:
                # if we are running in kfp exit to signal kfp that we failed
                sys.exit(1)
        return res
