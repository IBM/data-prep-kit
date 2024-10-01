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

from typing import Any

from data_processing.data_access import DataAccessFactoryBase
from data_processing.utils import UnrecoverableException
from data_processing.transform import TransformStatistics
from data_processing_spark.runtime.spark import SparkTransformLauncher
from data_processing_spark.runtime.spark import SparkTransformRuntimeConfiguration, DefaultSparkTransformRuntime
from profiler_transform_base import ProfilerTransformBase, ProfilerTransformConfigurationBase, DataAggregator

class ProfilerSparkTransform(ProfilerTransformBase):
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
        self.aggregator = config.get("aggregator", None)
        if self.aggregator is None:
            raise UnrecoverableException("Aggregator is not defined")

    def _submit_to_cache(self, words: dict[str, str]) -> None:
        """
        Submits
        :param words: dictionary of word occurrences in document
        :return: unique documents
        """
        self.aggregator.add_words(words)
        return


class ProfilerRuntime(DefaultSparkTransformRuntime):
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
        from data_processing.utils import get_logger
        super().__init__(params=params)
        self.aggregator = None
        self.logger = get_logger(__name__)

    def get_transform_config(
            self, partition: int, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc, that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :return: dictionary of transform init params
        """
        self.aggregator = DataAggregator({"data_access_factory": data_access_factory})
        return self.params | {"aggregator": self.aggregator}


    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # Save aggregated info
        if self.aggregator is not None:
            words, size = self.aggregator.get_size()
            stats.add_stats({"unique words": words, "words memory, GB": size})
            # save execution result
            self.aggregator.save_data()


class ProfilerSparkTransformRuntimeConfiguration(SparkTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=ProfilerTransformConfigurationBase(transform_class=ProfilerSparkTransform),
                         runtime_class=ProfilerRuntime)


if __name__ == "__main__":
    launcher = SparkTransformLauncher(ProfilerSparkTransformRuntimeConfiguration())
    launcher.launch()
