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
import sys
import time

import ray
from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.runtime.transform_launcher import AbstractTransformLauncher
from data_processing.utils import get_logger, str2bool
from data_processing_ray.runtime.ray import (
    RayTransformExecutionConfiguration,
    RayTransformRuntimeConfiguration,
    orchestrate,
)


logger = get_logger(__name__)


class RayTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: RayTransformRuntimeConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        super().__init__(runtime_config, data_access_factory)
        self.execution_config = RayTransformExecutionConfiguration(name=self.name)

    def _get_arguments(self, parser: argparse.ArgumentParser) -> argparse.Namespace:
        """
        Parse input parameters
        :param parser: parser
        :return: list of arguments
        """
        parser.add_argument(
            "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="running ray local flag"
        )
        return super()._get_arguments(parser)

    def _get_parameters(self, args: argparse.Namespace) -> bool:
        """
        This method creates arg parser, fill it with the parameters
        and does parameters validation
        :return: True id validation passe or False, if not
        """
        result = super()._get_parameters(args)
        self.run_locally = args.run_locally
        if self.run_locally:
            logger.info("Running locally")
        else:
            logger.info("connecting to existing cluster")
        return result

    def _submit_for_execution(self) -> int:
        """
        Submit for Ray execution
        :return:
        """
        res = 1
        start = time.time()
        try:
            if self.run_locally:
                # Will create a local Ray cluster
                logger.debug("running locally creating Ray cluster")
                # enable metrics for local Ray
                ray.init(_metrics_export_port=8088)
            else:
                # connect to the existing cluster
                logger.info("Connecting to the existing Ray cluster")
                ray.init(f"ray://localhost:10001", ignore_reinit_error=True)
            logger.debug("Starting orchestrator")
            res = ray.get(
                orchestrate.remote(
                    preprocessing_params=self.execution_config,
                    data_access_factory=self.data_access_factory,
                    runtime_config=self.runtime_config,
                )
            )
            logger.debug("Completed orchestrator")
            time.sleep(10)
        except Exception as e:
            logger.info(f"Exception running ray remote orchestration\n{e}")
        finally:
            logger.info(f"Completed execution in {round((time.time() - start)/60., 3)} min, execution result {res}")
            ray.shutdown()
            return res

    def launch(self) -> int:
        """
        Execute method orchestrates driver invocation
        :return: launch result
        """
        res = super().launch()
        if not self.run_locally and res > 0:
            # if we are running in kfp exit to signal kfp that we failed
            sys.exit(1)
        return res
