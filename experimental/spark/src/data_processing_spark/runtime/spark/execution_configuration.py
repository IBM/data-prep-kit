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

from data_processing.runtime import TransformExecutionConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)


cli_prefix = "runtime_"


class SparkTransformExecutionConfiguration(TransformExecutionConfiguration):
    """
    A class specifying and validating Spark orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        super().__init__(name=name, print_params=False)

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        if not TransformExecutionConfiguration.apply_input_params(self, args=args):
            return False
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        # store parameters locally
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "spark",
            "job id": captured["job_id"],
        }
        # if the user did not define actor max_restarts set it up for fault tolerance
        logger.info(f"job details {self.job_details}")
        return True
