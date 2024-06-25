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

import csv
import io
import uuid
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
import ray
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import GB, CLIArgumentProvider, TransformUtils
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ray.actor import ActorHandle


REQUEST_LEN = 8192

short_name = "profiler"
cli_prefix = f"{short_name}_"


@ray.remote(scheduling_strategy="SPREAD")
class DataAggregator:
    """
    Implements an element of distributed cache of data
    """

    def __init__(self, params: dict[str, Any]):
        """
        initialize set of local aggregators
        :param params - dictionary of input parameters.
            data_access - data access factory
        """
        self.words = {}
        self.id = str(uuid.uuid4())
        data_access_factory = params.get("data_access")
        self.data_access = data_access_factory.create_data_access()

    def add_words(self, words: dict[str, int]) -> None:
        """
        Add words to cache
        :param words: new words dictionary
        :return: None
        """
        # merge dictionaries updating values
        for k, v in words.items():
            self.words[k] = self.words.get(k, 0) + v

    def get_size(self) -> tuple[int, float]:
        """
        Get size of created aggregators for statistics
        :return: size of the local set and its memory footprint
        """
        return len(self.words), TransformUtils.deep_get_size(self.words) / GB

    def save_data(self) -> tuple[dict[str, Any], int]:
        """
        Save data
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        if len(self.words) == 0:
            # No data
            return {}, 0
        output_folder = self.data_access.get_output_folder()
        if not output_folder.endswith("/"):
            output_folder += "/"
        """
        output_path = f"{output_folder}{self.id}.parquet"
        # convert to parquet
        words = pa.array(self.words.keys())
        counts = pa.array(self.words.values())
        names = ["words", "counts"]
        table = pa.Table.from_arrays(arrays=[words, counts], names=names)
        # save table
        return self.data_access.save_table(path=output_path, table=table)
        """
        # convert dictionary to CSV
        s = io.StringIO()
        dict_writer = csv.DictWriter(s, ["word", "count"], extrasaction="ignore")
        for i, datum in enumerate(self.words.items()):
            row = {"word": datum[0], "count": datum[1]}
            dict_writer.writerow(row)
        # reset cursor to the beginning of the StringIO stream
        s.seek(0)
        output_path = f"{output_folder}{self.id}.csv"
        return self.data_access.save_file(path=output_path, data=s.read().encode("utf-8"))


class ProfilerTransform(AbstractTableTransform):
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
        self.doc_column = config.get("doc_column", "contents")
        self.aggregators = config.get("aggregators", [])
        if len(self.aggregators) == 0:
            raise RuntimeError("No aggregators are available")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Building aggregations.
        :param table: table
        :param file_name: name of the file to process
        :return: resulting table, statistics
        """
        from base_tokenizer import tokenize
        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column])
        # Inner variables
        words = {}
        # Compute words count
        for text in table[self.doc_column]:
            # Compute doc hash
            tokens = tokenize(text=str(text))
            for token in tokens:
                words[token] = words.get(token, 0) + 1
        # submit word counts to cache
        self._submit_to_cache(words=words)
        # return
        return [], {}

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
        Create filter runtime
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
        params = {"data_access": data_access_factory}
        # create aggregators
        self.aggregators = RayUtils.create_actors(
            clazz=DataAggregator,
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
        return {"number of words": sum_aggregators, "word memory, GB": sum_aggregator_mem} | stats


class ProfilerTableTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=ProfilerTransform,
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument(
            f"--{cli_prefix}aggregator_cpu", type=float, default=0.5, help="number of CPUs per aggregator"
        )
        parser.add_argument(
            f"--{cli_prefix}num_aggregators", type=int, default=0, help="number of aggregator actors to use"
        )
        parser.add_argument(f"--{cli_prefix}doc_column", type=str, default="contents", help="key for accessing data")

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        if self.params["num_aggregators"] <= 0:
            self.logger.info(
                f"Number of aggregators should be greater then zero, provided {self.params['num_aggregators']}"
            )
            return False
        self.logger.info(f"profiler params are {self.params}")
        return True


class ProfilerRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=ProfilerTableTransformConfiguration(), runtime_class=ProfilerRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(ProfilerRayTransformConfiguration())
    launcher.launch()
