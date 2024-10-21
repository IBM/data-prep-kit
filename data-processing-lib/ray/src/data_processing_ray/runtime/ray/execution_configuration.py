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

from data_processing.runtime import TransformExecutionConfiguration
from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger


logger = get_logger(__name__)


cli_prefix = "runtime_"


class RayTransformExecutionConfiguration(TransformExecutionConfiguration):
    """
    A class specifying and validating Ray orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        super().__init__(name=name, print_params=False)
        self.worker_options = {}
        self.n_workers = 1
        self.creation_delay = 0

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
        parser.add_argument(f"--{cli_prefix}creation_delay", type=int, default=0, help="delay between actor' creation")
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
        self.worker_options = captured["worker_options"]
        self.n_workers = captured["num_workers"]
        self.creation_delay = captured["creation_delay"]
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "ray",
            "job id": captured["job_id"],
        }
        # if the user did not define actor max_restarts set it up for fault tolerance
        if "max_restarts" not in self.worker_options:
            self.worker_options["max_restarts"] = -1

        # print them
        logger.info(f"number of workers {self.n_workers} worker options {self.worker_options}")
        logger.info(f"actor creation delay {self.creation_delay}")
        logger.info(f"job details {self.job_details}")
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
