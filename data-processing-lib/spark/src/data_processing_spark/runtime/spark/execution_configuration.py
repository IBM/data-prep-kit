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
from typing import Any

from data_processing.runtime import TransformExecutionConfiguration, runtime_cli_prefix
from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)


class SparkTransformExecutionConfiguration(TransformExecutionConfiguration):
    """
    A class specifying and validating Spark orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        super().__init__(name=name, print_params=False)
        self.parallelization = -1

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return: None
        """
        """
        This determines how many partitions the RDD should be divided into. See 
        https://sparktpoint.com/how-to-create-rdd-using-parallelize/ for the explanation
        of this parameter
        If you specify a positive value of the parameter, Spark will attempt to evenly 
        distribute the data from seq into that many partitions. For example, if you have 
        a collection of 100 elements and you specify numSlices as 4, Spark will try 
        to create 4 partitions with approximately 25 elements in each partition.
        If you don’t specify this parameter, Spark will use a default value, which is 
        typically determined based on the cluster configuration or the available resources
        (number of workers).    
        """
        parser.add_argument(f"--{runtime_cli_prefix}parallelization", type=int, default=-1, help="parallelization.")
        return TransformExecutionConfiguration.add_input_params(self, parser=parser)

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        if not TransformExecutionConfiguration.apply_input_params(self, args=args):
            return False
        captured = CLIArgumentProvider.capture_parameters(args, runtime_cli_prefix, False)
        # store parameters locally
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "spark",
            "job id": captured["job_id"],
        }
        self.parallelization = captured["parallelization"]
        # if the user did not define actor max_restarts set it up for fault tolerance
        logger.info(f"job details {self.job_details}")
        logger.info(f"RDD parallelization {self.parallelization}")
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return: dictionary of parameters
        """
        return {
            "RDD parallelization": self.parallelization,
        }
