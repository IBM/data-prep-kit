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
from typing import Any

from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger


logger = get_logger(__name__)


cli_prefix = "runtime_"


class TransformOrchestratorConfiguration(CLIArgumentProvider):
    """
    A class specifying and validating Ray orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        self.worker_options = {}
        self.n_workers = 1
        self.creation_delay = 0
        self.pipeline_id = ""
        self.job_details = {}
        self.code_location = {}
        self.name = name

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return:
        """
        parser.add_argument(f"--{cli_prefix}num_workers", type=int, default=1, help="number of workers")

        help_example_dict = {
            "num_cpus": ["8", "Required number of CPUs."],
            "num_gpus": ["1", "Required number of GPUs"],
            "resources": [
                '{"special_hardware": 1, "custom_label": 1}',
                """The complete list can be found at
           https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options
           and contains accelerator_type, memory, name, num_cpus, num_gpus, object_store_memory, placement_group,
           placement_group_bundle_index, placement_group_capture_child_tasks, resources, runtime_env,
           scheduling_strategy, _metadata, concurrency_groups, lifetime, max_concurrency, max_restarts,
           max_task_retries, max_pending_calls, namespace, get_if_exists""",
            ],
        }
        parser.add_argument(
            f"--{cli_prefix}worker_options",
            type=ast.literal_eval,
            default="{'num_cpus': 0.8}",
            help="AST string defining worker resource requirements.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument(f"--{cli_prefix}pipeline_id", type=str, default="pipeline_id", help="pipeline id")
        parser.add_argument(f"--{cli_prefix}job_id", type=str, default="job_id", help="job id")
        parser.add_argument(f"--{cli_prefix}creation_delay", type=int, default=0, help="delay between actor' creation")

        help_example_dict = {
            "github": ["https://github.com/somerepo", "Github repository URL."],
            "commit_hash": ["13241231asdfaed", "github commit hash"],
            "path": ["transforms/universal/ededup", "Path within the repository"],
        }
        parser.add_argument(
            f"--{cli_prefix}code_location",
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
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        # store parameters locally
        self.worker_options = captured["worker_options"]
        self.n_workers = captured["num_workers"]
        self.creation_delay = captured["creation_delay"]
        self.pipeline_id = captured["pipeline_id"]
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "ray",
            "job id": captured["job_id"],
        }
        self.code_location = captured["code_location"]

        # print them
        logger.info(f"number of workers {self.n_workers} worker options {self.worker_options}")
        logger.info(f"pipeline id {self.pipeline_id}; number workers {self.n_workers}")
        logger.info(f"job details {self.job_details}")
        logger.info(f"code location {self.code_location}")
        logger.info(f"actor creation delay {self.creation_delay}")
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return: dictionary of parameters
        """
        return {
            "number of workers": self.n_workers,
            "worker options": self.worker_options,
            "actor creation delay": self.creation_delay,
        }
