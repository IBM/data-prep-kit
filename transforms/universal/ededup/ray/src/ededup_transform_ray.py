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

import pickle
from argparse import ArgumentParser, Namespace
from typing import Any

import ray
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing.utils import TransformUtils, UnrecoverableException
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from ededup_transform_base import (
    EdedupTransformBase,
    EdedupTransformConfigurationBase,
    HashFilter,
    cli_prefix,
)
from ray.actor import ActorHandle
from ededup_transform_base import use_snapshot_key


hash_cpu_key = "hash_cpu"
num_hashes_key = "num_hashes"
hash_cpu_cli_params = f"{cli_prefix}{hash_cpu_key}"
num_hashes_cli_params = f"{cli_prefix}{num_hashes_key}"


class EdedupRayTransform(EdedupTransformBase):
    """
    Implements dedup table transformer.
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of the doc column
            hashes - list of hash actors, references
        """
        super().__init__(config)
        self.hashes = config.get("hashes", [])

    def _process_cached_hashes(self, hd: dict[str, str]) -> list[str]:
        """
        check hashes uniqueness with the distributed cache of hashes
        :param hd: dictionary of hash to document
        :return: unique documents
        """
        # Build requests - We are building requests for individual hash actors
        request = [[] for _ in range(len(self.hashes))]

        for h in hd.keys():
            request[TransformUtils.str_to_int(h) % len(self.hashes)].append(h)

        # Submit requests to appropriate hash actors
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.hashes[i].get_unique.remote(req))
            i = i + 1
        # Process replies
        unique = []
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            # Populate uniques for completed replies
            for red in ray.get(ready):
                for uh in red:
                    unique.append(hd[uh])
            # Continue waiting for not completed replies
            remote_replies = not_ready
        return unique


class EdedupRayRuntime(DefaultRayTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the doc column
            hash_cpu - cpus per hash instance
            num_hashes - number of hashes
        """
        from data_processing.utils import get_logger

        super().__init__(params)
        self.filters = []
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
        # create hashes
        n_filters = self.params.get(num_hashes_key, 1)
        self.filters = [None] * n_filters
        for i in range(n_filters):
            self.filters[i] = (
                ray.remote(HashFilter)
                .options(num_cpus=self.params.get(hash_cpu_key, 0.5))
                .remote({"id": i, "data_access_factory": data_access_factory})
            )
        if self.params.get(use_snapshot_key, False):
            self._load_snapshots(data_access_factory=data_access_factory, statistics=statistics)
        return {"hashes": self.filters} | self.params

    def _load_snapshots(self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle) -> None:
        """
        Load snapshots
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :return: None
        """
        # we are using snapshots. Note here that the amount of files might be different
        # from the current amount of hashes
        snapshot_path = self.params.get("snapshot_directory", None)
        if snapshot_path is None or len(snapshot_path) == 0:
            snapshot_path = f"{SnapshotUtils.get_snapshot_folder(data_access_factory.create_data_access())}"
        data_access = data_access_factory.create_data_access()
        # get snapshot files
        files, retries = data_access.get_folder_files(path=snapshot_path)
        if retries > 0:
            statistics.add_stats.remote({"data access retries": retries})
        self.logger.info(f"Found the following snapshot files {files.keys()}")
        # process snapshot files
        for file in files.values():
            # convert the file
            try:
                snaps = pickle.loads(file)
            except Exception as e:
                self.logger.warning(f"Failed to load hashes with exception {e}")
                raise UnrecoverableException("failed to load hashes")
            request = [[] for _ in range(len(self.filters))]
            for h in snaps:
                request[TransformUtils.str_to_int(h) % len(self.filters)].append(h)
            # Submit requests to appropriate hash actors
            remote_replies = []
            i = 0
            for req in request:
                if len(req) > 0:  # Only submit if the length is greater then 0
                    remote_replies.append(self.filters[i].add_hashes.remote(req))
                i = i + 1
            # Process replies
            while remote_replies:
                # Wait for replies
                ready, not_ready = ray.wait(remote_replies)
                # Continue waiting for not completed replies
                remote_replies = not_ready

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        # Get filters stats
        sum_hash = 0
        sum_hash_mem = 0
        remote_replies = [f.get_hash_size.remote() for f in self.filters]
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            for r in ready:
                h_size, h_memory = ray.get(r)
                sum_hash = sum_hash + h_size
                sum_hash_mem = sum_hash_mem + h_memory
            remote_replies = not_ready
        dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 1))
        # snapshot execution results
        remote_replies = [f.snapshot.remote() for f in self.filters]
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            remote_replies = not_ready
        return {"number of hashes": sum_hash, "hash memory, GB": sum_hash_mem, "de duplication %": dedup_prst} | stats


class EdedupRayTransformConfiguration(EdedupTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(transform_class=EdedupRayTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        super().add_input_params(parser)
        parser.add_argument(f"--{hash_cpu_cli_params}", type=float, default=0.5, help="number of CPUs per hash")
        parser.add_argument(f"--{num_hashes_cli_params}", type=int, default=0, help="number of hash actors to use")

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        super().apply_input_params(args)
        if self.params[num_hashes_key] <= 0:
            self.logger.info(f"Number of hashes should be greater then zero, provided {self.params['num_hashes']}")
            return False
        return True


class EdedupRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=EdedupRayTransformConfiguration(), runtime_class=EdedupRayRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(EdedupRayTransformRuntimeConfiguration())
    launcher.launch()
