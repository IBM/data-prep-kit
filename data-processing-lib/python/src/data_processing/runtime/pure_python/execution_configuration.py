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

from data_processing.runtime import TransformExecutionConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger


logger = get_logger(__name__)


cli_prefix = "runtime_"


class PythonTransformExecutionConfiguration(TransformExecutionConfiguration):
    """
    A class specifying and validating Python orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        super().__init__(name=name, print_params=False)
        self.num_processors = 0

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return:
        """
        parser.add_argument(f"--{cli_prefix}num_processors", type=int, default=0, help="size of multiprocessing pool")

        return TransformExecutionConfiguration.add_input_params(self, parser=parser)

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
        self.num_processors = captured["num_processors"]
        # print them
        if self.num_processors > 0:
            # we are using multiprocessing
            logger.info(f"using multiprocessing, num processors {self.num_processors}")
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return: dictionary of parameters
        """
        return {"num_processors": self.num_processors}
