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
import ast

from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger


logger = get_logger(__name__)


runtime_cli_prefix = "runtime_"


class TransformExecutionConfiguration(CLIArgumentProvider):
    """
    A class specifying and validating transform execution configuration
    """

    def __init__(self, name: str, print_params: bool = True):
        """
        Initialization
        :param name: job name
        :param print_params: flag to print parameters
        """
        self.pipeline_id = ""
        self.job_details = {}
        self.code_location = {}
        self.name = name
        self.print_params = print_params

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return:
        """
        parser.add_argument(f"--{runtime_cli_prefix}pipeline_id", type=str, default="pipeline_id", help="pipeline id")
        parser.add_argument(f"--{runtime_cli_prefix}job_id", type=str, default="job_id", help="job id")

        help_example_dict = {
            "github": ["https://github.com/somerepo", "Github repository URL."],
            "commit_hash": ["1324", "github commit hash"],
            "path": ["transforms/universal/code", "Path within the repository"],
        }
        parser.add_argument(
            f"--{runtime_cli_prefix}code_location",
            type=ast.literal_eval,
            default=None,
            help="AST string containing code location\n" + ParamsUtils.get_ast_help_text(help_example_dict),
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, runtime_cli_prefix, False)
        # store parameters locally
        self.pipeline_id = captured["pipeline_id"]
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "pure python",
            "job id": captured["job_id"],
        }
        self.code_location = captured["code_location"]
        # print parameters
        logger.info(f"pipeline id {self.pipeline_id}")
        if self.print_params:
            logger.info(f"job details {self.job_details}")
        logger.info(f"code location {self.code_location}")
        return True
