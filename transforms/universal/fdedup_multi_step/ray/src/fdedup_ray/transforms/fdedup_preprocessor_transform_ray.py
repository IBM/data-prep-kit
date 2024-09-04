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
from fdedup.utils import MurmurMH
from fdedup.transforms.base import (FdedupPreprocessorTransformBase,
                                    FdedupPreprocessorTransformConfigurationBase,
                                    preprocessor_cli_prefix, num_bands_key, length_band_key,
                                    buckets_cache_key, minhashes_cache_key, mn_min_hash_key,
                                    threshold_key, num_permutations_key, minhash_snapshot_directory_key,
                                    buckets_snapshot_directory_key, doc_id_snapshot_directory_key, doc_id_cache_key
                                    )

bucket_cpu_key = "bucket_cpu"
minhash_cpu_key = "minhash_cpu"
doc_id_cpu_key = "doc_id_cpu"
num_buckets_key = "num_buckets"
num_minhash_key = "num_minhashes"
num_doc_id_key = "num_doc_id"

preprocessor_bucket_cpu_cli_param = f"{preprocessor_cli_prefix}{bucket_cpu_key}"
preprocessor_doc_id_cpu_cli_param = f"{preprocessor_cli_prefix}{doc_id_cpu_key}"
preprocessor_minhash_cpu_cli_param = f"{preprocessor_cli_prefix}{minhash_cpu_key}"
preprocessor_num_buckets_cli_param = f"{preprocessor_cli_prefix}{num_buckets_key}"
preprocessor_num_doc_id_cli_param = f"{preprocessor_cli_prefix}{num_doc_id_key}"
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
            buckets - bucket cache
            minhashes - minhash cache
            docid - docid cache
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
        self.docid = config.get(doc_id_cache_key, None)
        if self.docid is None:
            raise UnrecoverableException("doc id cache is not defined")

    def _submit_buckets_minhashes(
            self, buckets: dict[int, list[int]], minhashes: list[tuple[int, int, np.array]]
    ) -> None:
        """
        Submit buckets to hash
        :param buckets: buckets
        :param minhashes: minhashes
        :return: None
        """
        from fdedup_ray.utils import FdedupSupportRay
        # remove local duplicates
        buckets, minhashes, docs, removed = (
            self._remove_local_duplicates(buckets=buckets, minhashes=minhashes))
        # buckets requests
        FdedupSupportRay.update_buckets(buckets=buckets, bucket_actors=self.buckets, logger=self.logger)
        # Minhash requests
        FdedupSupportRay.update_minhashes(minhashes=minhashes, minhash_actors=self.minhashes, logger=self.logger)
        # doc id requests
        FdedupSupportRay.update_doc_ids(docs=docs, removed=removed, doc_actors=self.docid, logger=self.logger)


class FdedupRayPreprocessorRuntime(DefaultRayTransformRuntime):
    """
    fuzzy dedup preprocessor runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        super().__init__(params=params)
        self.buckets = None
        self.docid = None
        self.minhashes = None
        self.logger = get_logger(__name__)
        self.threshold = params.get(threshold_key, 0.8)
        self.num_permutations = params.get(num_permutations_key, 64)
        self.minhash_snapshot = params.get(minhash_snapshot_directory_key, None)
        self.buckets_snapshot = params.get(buckets_snapshot_directory_key, None)
        self.docid_snapshot = params.get(doc_id_snapshot_directory_key, None)
        self.n_buckets = params.get(num_buckets_key, 1)
        self.n_docid = params.get(num_doc_id_key, 1)
        self.n_minhash = params.get(num_minhash_key, 1)
        self.bucket_cpu = params.get(bucket_cpu_key, .5)
        self.docid_cpu = params.get(doc_id_cpu_key, .5)
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
        from fdedup.utils import FdedupSupport
        from fdedup_ray.utils import FdedupSupportRay
        # compute fuzzy dedup parameters
        num_buckets, length_bucket = FdedupSupport.fuzzy_optimal_param(
            threshold=self.threshold,
            num_perm=self.num_permutations,
            false_positive_weight=0.5,
            false_negative_weight=0.5,
        )
        self.logger.info(f"Fuzzy: num buckets {num_buckets}, bucket length {length_bucket}")
        mn_min_hash = MurmurMH(num_perm=self.num_permutations, seed=RANDOM_SEED)
        # create minhashes
        self.minhashes = FdedupSupportRay.create_minhashes(
            data_access_factory=data_access_factory, n_actors=self.n_minhash, actor_cpu=self.minhash_cpu,
            directory=self.minhash_snapshot, logger=self.logger, statistics=statistics)
        # create doc ids
        self.docid = FdedupSupportRay.create_doc_ids(
            data_access_factory=data_access_factory, n_actors=self.n_docid, actor_cpu=self.docid_cpu,
            directory=self.docid_snapshot, logger=self.logger, statistics=statistics)
        # create buckets
        self.buckets = FdedupSupportRay.create_buckets(
            data_access_factory=data_access_factory, n_actors=self.n_buckets, actor_cpu=self.bucket_cpu,
            directory=self.buckets_snapshot, logger=self.logger, statistics=statistics)

        return self.params | {num_bands_key: num_buckets, length_band_key: length_bucket,
                              mn_min_hash_key: mn_min_hash, minhashes_cache_key: self.minhashes,
                              doc_id_cache_key: self.docid, buckets_cache_key: self.buckets}

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Update/augment the given stats object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        from fdedup_ray.utils import FdedupSupportRay
        # compute minhash usage
        sum_mh, sum_mh_mem = FdedupSupportRay.get_minhash_stats(self.minhashes)
        # compute buckets usage
        sum_buckets, sum_buckets_mem = FdedupSupportRay.get_bucket_stats(self.buckets)
        # compute doc id usage
        sum_docs, sum_docs_memory, sum_removed, sum_removed_memory = FdedupSupportRay.get_doc_stats(self.docid)
        # create snapshots
        FdedupSupportRay.snapshot_caches(bucket_actors=self.buckets, doc_actors=self.docid,
                                         minhash_actors=self.minhashes, logger=self.logger)
        # return updated statistics
        return {"number of buckets": sum_buckets, "bucket memory, GB": sum_buckets_mem,
                "number of minhashes": sum_mh, "minhashes memory, GB": sum_mh_mem,
                "number of docs": sum_docs, "docs_memory, GB": sum_docs_memory,
                "number of removed": sum_removed, "removed_memory, GB": sum_removed_memory} | stats


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
            f"--{preprocessor_doc_id_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per doc id hash"
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
            f"--{preprocessor_num_doc_id_cli_param}",
            type=int,
            default=1,
            help="number of doc id caches to use"
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
