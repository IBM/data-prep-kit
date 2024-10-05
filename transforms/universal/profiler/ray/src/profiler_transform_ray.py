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

from argparse import ArgumentParser, Namespace
from typing import Any

import ray
from data_processing.data_access import DataAccessFactoryBase
from data_processing.utils import CLIArgumentProvider, TransformUtils, UnrecoverableException
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle
from profiler_transform_base import (
    DataAggregator,
    ProfilerTransformBase,
    ProfilerTransformConfigurationBase,
    cli_prefix
)


class ProfilerTransform(ProfilerTransformBase):
    """
    Implements Aggregator table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
            aggregators - list of aggregator actors, references
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of AggregateTableTransformConfiguration class
        super().__init__(config)
        self.aggregators = config.get("aggregators", [])
        if len(self.aggregators) == 0:
            raise UnrecoverableException("No aggregators are available")

    def _submit_to_cache(self, words: dict[str, str]) -> None:
        """
        Submits
        :param words: dictionary of word occurrences in document
        :return: unique documents
        """
        # split words by aggregators
        aggregator_words = [{} for _ in range(len(self.aggregators))]
        for k, v in words.items():
            aggr = TransformUtils.str_to_int(k) % len(self.aggregators)
            aggregator_words[aggr][k] = v

        # Build requests to individual aggregators
        requests = [aggregator_words[i] for i in range(len(self.aggregators))]

        # Submit requests to appropriate aggregator actors. Note, its completely asynchronous
        for i in range(len(self.aggregators)):
            req = requests[i]
            if len(req) > 0:  # Only submit if the length is greater then 0
                self.aggregators[i].add_words.remote(req)
        return


class ProfilerRuntime(DefaultRayTransformRuntime):
    """
    Aggregator runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create profiler runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            aggregator_cpu - cpus per hash instance
            num_aggregators - number of aggregators
        """
        super().__init__(params)
        self.aggregators = []
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for transform execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        # aggregator parameters
        params = {"data_access_factory": data_access_factory}
        # create aggregators
        self.aggregators = RayUtils.create_actors(
            clazz=ray.remote(DataAggregator),
            params=params,
            actor_options={"num_cpus": self.params.get("aggregator_cpu", 0.5)},
            n_actors=self.params.get("num_aggregators", 1),
        )
        return {"aggregators": self.aggregators} | self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        # Save aggregated info
        remote_replies = [aggr.save_data.remote() for aggr in self.aggregators]
        retries = 0
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            for r in ready:
                res, ret = ray.get(r)
                retries += ret
                if res is None:
                    self.logger.warning("Failed to write aggregation file")
            remote_replies = not_ready

        # Get aggregator's stats
        sum_aggregators = 0
        sum_aggregator_mem = 0
        remote_replies = [aggr.get_size.remote() for aggr in self.aggregators]
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            for r in ready:
                h_size, h_memory = ray.get(r)
                sum_aggregators = sum_aggregators + h_size
                sum_aggregator_mem = sum_aggregator_mem + h_memory
            remote_replies = not_ready
        if retries > 0:
            stats["data access retries"] = stats.get("data access retries", 0) + retries
        return {"unique words": sum_aggregators, "words memory, GB": sum_aggregator_mem} | stats


class ProfilerRayTransformConfiguration(ProfilerTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            transform_class=ProfilerTransform,
            print_config=False,
        )

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        super().add_input_params(parser)
        parser.add_argument(
            f"--{cli_prefix}aggregator_cpu",
            type=float,
            default=0.5,
            help="number of CPUs per aggregator"
        )
        parser.add_argument(
            f"--{cli_prefix}num_aggregators",
            type=int,
            default=0,
            help="number of aggregator actors to use"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        super().apply_input_params(args)
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        if self.params["num_aggregators"] <= 0:
            self.logger.info(
                f"Number of aggregators should be greater then zero, provided {self.params['num_aggregators']}"
            )
            return False
        self.logger.info(f"profiler params are {self.params}")
        return True


class ProfilerRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=ProfilerRayTransformConfiguration(), runtime_class=ProfilerRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(ProfilerRayTransformRuntimeConfiguration())
    launcher.launch()
