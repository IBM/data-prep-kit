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
from argparse import ArgumentParser, Namespace
import numpy as np
import ray
import pickle
from ray.actor import ActorHandle
from data_processing.utils import UnrecoverableException, RANDOM_SEED
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from fdedup.utils import BucketsHash, DocsMinHash, MurmurMH, fuzzy_optimal_param
from fdedup.transforms.base import (FdedupPreprocessorTransformBase,
                                    FdedupPreprocessorTransformConfigurationBase,
                                    preprocessor_cli_prefix,
                                    buckets_cache_key, minhashes_cache_key, mn_min_hash_key,
                                    threshold_key, num_permutations_key, minhash_snapshot_directory_key,
                                    buckets_snapshot_directory_key,
                                    )

bucket_cpu_key = "bucket_cpu"
minhash_cpu_key = "minhash_cpu"
num_buckets_key = "num_buckets"
num_minhash_key = "num_minhashes"

preprocessor_bucket_cpu_cli_param = f"{preprocessor_cli_prefix}{bucket_cpu_key}"
preprocessor_minhash_cpu_cli_param = f"{preprocessor_cli_prefix}{minhash_cpu_key}"
preprocessor_num_buckets_cli_param = f"{preprocessor_cli_prefix}{num_buckets_key}"
preprocessor_num_minhash_cli_param = f"{preprocessor_cli_prefix}{num_minhash_key}"


class FdedupRayPreprocessorTransform(FdedupPreprocessorTransformBase):
    """
    Fdedup preprocessor Python version
    """
    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters, with the following keys
            doc_column - name of doc column
            doc_id_int_column - name of int doc id column
            word_shingle_size - word shingle size
            mn_min_hash - MurmurMH class
            num_bands - number of bands
            length_band - band length
            buckets - bucket class
            minhashes - minhash class
            delimiter - delimiter
        """
        # superclass initialization
        super().__init__(config)
        self.buckets = config.get(buckets_cache_key, None)
        if self.buckets is None:
            raise UnrecoverableException("backets cache is not defined")
        self.minhashes = config.get(minhashes_cache_key, None)
        if self.minhashes is None:
            raise UnrecoverableException("minhashes cache is not defined")

    def _submit_buckets_minhashes(
            self, buckets: dict[int, list[int]], minhashes: list[tuple[int, int, np.array]]
    ) -> None:
        """
        Submit buckets to hash
        :param buckets: buckets
        :param minhashes: minhashes
        :return: None
        """
        # buckets requests
        request = [[] for _ in range(len(self.buckets))]
        for key, value in buckets.items():
            request[key % len(self.buckets)].append((key, value))
        # Submit requests to appropriate bucket collectors
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.buckets[i].add_buckets.remote(req))
            i += 1
        # Minhash requests
        request = [[] for _ in range(len(self.minhashes))]
        for minh in minhashes:
            request[minh[0] % len(self.minhashes)].append(minh)
        # Submit requests to appropriate minhash collectors
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.minhashes[i].add_minhashes.remote(req))
            i += 1
        # wait for completion
        RayUtils.wait_for_execution_completion(logger=self.logger, replies=remote_replies)


class FdedupRayPreprocessorRuntime(DefaultRayTransformRuntime):
    """
    fuzzy dedup preprocessor runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        super().__init__(params=params)
        self.buckets = None
        self.minhashes = None
        self.logger = get_logger(__name__)
        self.threshold = params.get(threshold_key, 0.8)
        self.num_permutations = params.get(num_permutations_key, 64)
        self.minhash_snapshot = params.get(minhash_snapshot_directory_key, None)
        self.buckets_snapshot = params.get(buckets_snapshot_directory_key, None)
        self.n_buckets = params.get(num_buckets_key, 1)
        self.n_minhash = params.get(num_minhash_key, 1)
        self.bucket_cpu = params.get(bucket_cpu_key, .5)
        self.minhash_cpu = params.get(minhash_cpu_key, .5)

    def get_transform_config(
            self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc., that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        # compute fuzzy dedup parameters
        num_buckets, length_bucket = fuzzy_optimal_param(
            threshold=self.threshold,
            num_perm=self.num_permutations,
            false_positive_weight=0.5,
            false_negative_weight=0.5,
        )
        self.logger.info(f"Fuzzy: num buckets {num_buckets}, bucket length {length_bucket}")
        mn_min_hash = MurmurMH(num_perm=self.num_permutations, seed=RANDOM_SEED)
        data_access = data_access_factory.create_data_access()
        # create minhashes
        self.minhashes = [None] * self.n_minhash
        for i in range(self.n_minhash):
            self.minhashes[i] = ray.remote(DocsMinHash).options(**{"num_cpus": self.minhash_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None}
            )
        if self.minhash_snapshot is not None and len(self.minhash_snapshot) > 0:
            # get snapshot files. Note here that the amount of files might differ from the
            # the amount of minhashes.
            files, retries = data_access.get_folder_files(path=self.minhash_snapshot)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            for file in files.keys():
                # load the file
                try:
                    m_hashes, _ = data_access.get_file(file)
                    minhashes = list(pickle.loads(m_hashes).getitems())
                except Exception as e:
                    self.logger.warning(f"Failed to load minhashes from file {file} with exception {e}")
                    raise UnrecoverableException("failed to load minhashes")
                # Add snapshotted minhashes
                request = [[] for _ in range(self.n_minhash)]
                for mh in minhashes:
                    request[mh[0] % self.n_minhash].append(mh)
                    # Submit requests to appropriate hash actors
                    remote_replies = []
                    i = 0
                    for req in request:
                        if len(req) > 0:  # Only submit if the length is greater then 0
                            remote_replies.append(self.minhashes[i].add_minhashes.remote(req))
                        i = i + 1
                    # Process replies
                    while remote_replies:
                        ready, not_ready = ray.wait(remote_replies)
                        remote_replies = not_ready
        self.logger.info(f"Created {len(self.minhashes)} minhash collectors")
        # create buckets
        self.buckets = [None] * self.n_buckets
        for i in range(self.n_buckets):
            self.buckets[i] = ray.remote(BucketsHash).options(**{"num_cpus": self.bucket_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None}
            )
        if self.buckets_snapshot is not None and len(self.buckets_snapshot) > 0:
            # get snapshot files. Note here that the amount of files might differ from the
            # the amount of buckets.
            files, retries = data_access.get_folder_files(
                path=f"{SnapshotUtils.get_snapshot_folder(data_access)}buckets")
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            for file in files.keys():
                # load the file
                try:
                    b_hashes, _ = data_access.get_file(file)
                    buckets = list(pickle.loads(b_hashes).get_items())
                except Exception as e:
                    self.logger.warning(f"Failed to load buckets from file {file} with exception {e}")
                    raise UnrecoverableException("failed to load buckets")
                # Add snapshotted buckets
                request = [[] for _ in range(self.n_buckets)]
                for b in buckets:
                    request[b[0] % self.n_buckets].append(b)
                    # Submit requests to appropriate hash actors
                    remote_replies = []
                    i = 0
                    for req in request:
                        if len(req) > 0:  # Only submit if the length is greater then 0
                            remote_replies.append(self.buckets[i].add_buckets.remote(req))
                        i = i + 1
                    # Process replies
                    while remote_replies:
                        ready, not_ready = ray.wait(remote_replies)
                        remote_replies = not_ready
        self.logger.info(f"Created {len(self.buckets)} bucket collectors")
        return self.params | {mn_min_hash_key: mn_min_hash, minhashes_cache_key: self.minhashes,
                              buckets_cache_key: self.buckets}

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute minhash usage
        sum_mh = 0
        sum_mh_mem = 0
        replies = [collector.get_size.remote() for collector in self.minhashes]
        while replies:
            ready, not_ready = ray.wait(replies)
            m_amount, m_memory = ray.get(ready)[0]
            sum_mh += m_amount
            sum_mh_mem += m_memory
            replies = not_ready
        # compute buckets usage
        sum_buckets = 0
        sum_buckets_mem = 0
        replies = [collector.get_size.remote() for collector in self.buckets]
        while replies:
            ready, not_ready = ray.wait(replies)
            b_amount, b_memory = ray.get(ready)[0]
            sum_buckets += b_amount
            sum_buckets_mem += b_memory
            replies = not_ready
        # create snapshots
        replies = [collector.snapshot.remote() for collector in self.minhashes]
        while replies:
            ready, not_ready = ray.wait(replies)
            replies = not_ready
        replies = [collector.snapshot.remote() for collector in self.buckets]
        while replies:
            ready, not_ready = ray.wait(replies)
            replies = not_ready
        # return updated statistics
        return {"number of buckets": sum_buckets, "bucket memory, GB": sum_buckets_mem,
                "number of minhashes": sum_mh, "minhashes memory, GB": sum_mh_mem} | stats


class FdedupPreprocessorTransformConfiguration(FdedupPreprocessorTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(transform_class=FdedupRayPreprocessorTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        super().add_input_params(parser)
        parser.add_argument(
            f"--{preprocessor_bucket_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per bucket hash"
        )
        parser.add_argument(
            f"--{preprocessor_minhash_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per minhash hash"
        )
        parser.add_argument(
            f"--{preprocessor_num_minhash_cli_param}",
            type=int,
            default=1,
            help="number of minhash caches to use"
        )
        parser.add_argument(
            f"--{preprocessor_num_buckets_cli_param}",
            type=int,
            default=1, help="number of bucket hashes to use"
        )

    def apply_input_params(self, args: Namespace) -> bool:

        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup preprocessing params are {self.params}")
        return True


class FdedupPreprocessorRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):

    def __init__(self):
        super().__init__(
            transform_config=FdedupPreprocessorTransformConfiguration(),
            runtime_class=FdedupRayPreprocessorRuntime,
        )


if __name__ == "__main__":
    launcher = RayTransformLauncher(FdedupPreprocessorRayTransformRuntimeConfiguration())
    launcher.launch()
