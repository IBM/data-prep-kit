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
from argparse import Namespace
import numpy as np
from data_processing.utils import UnrecoverableException, RANDOM_SEED
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics
from data_processing.runtime.pure_python import (DefaultPythonTransformRuntime,
                                                 PythonTransformLauncher,
                                                 PythonTransformRuntimeConfiguration
                                                 )
from fdedup.utils import BucketsHash, DocsMinHash, MurmurMH, fuzzy_optimal_param
from fdedup.transforms.base import (FdedupPreprocessorTransformBase,
                                    FdedupPreprocessorTransformConfigurationBase,
                                    buckets_cache_key, minhashes_cache_key, mn_min_hash_key,
                                    threshold_key, num_permutations_key,
                                    buckets_snapshot_directory_key, minhash_snapshot_directory_key)


class FdedupPreprocessorTransform(FdedupPreprocessorTransformBase):
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
            length_band band length
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
        self.minhashes.add_minhashes(updates=minhashes)
        self.buckets.add_buckets(bck=list(buckets.items()))


class FdedupPreprocessorRuntime(DefaultPythonTransformRuntime):
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
        self.minhash_directory = params.get(minhash_snapshot_directory_key, None)
        self.buckets_directory = params.get(buckets_snapshot_directory_key, None)

    def get_transform_config(
            self, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics, files: list[str]
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
        if self.minhash_directory is None or len(self.minhash_directory) == 0:
            mh_path = None
        else:
            # restarting from snapshot
            mh_path = self.minhash_directory
        if self.buckets_directory is None or len(self.buckets_directory) == 0:
           b_path = None
        else:
            # restarting from snapshot
            b_path = self.buckets_directory
        self.minhashes = DocsMinHash({"id": 0, "data_access": data_access_factory, "snapshot": mh_path})
        self.buckets = BucketsHash({"id": 0, "data_access": data_access_factory, "snapshot": b_path})
        return self.params | {mn_min_hash_key: mn_min_hash, minhashes_cache_key: self.minhashes,
                              buckets_cache_key: self.buckets}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        b_size, b_memory = self.buckets.get_size()
        m_size, m_memory = self.minhashes.get_size()
        stats.add_stats({"number of buckets": b_size, "bucket memory, GB": b_memory,
                         "number of minhashes": m_size, "minhashes memory, GB": m_memory})
        self.buckets.snapshot()
        self.minhashes.snapshot()


class FdedupPreprocessorTransformConfiguration(FdedupPreprocessorTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(transform_class=FdedupPreprocessorTransform)

    def apply_input_params(self, args: Namespace) -> bool:
        if args.runtime_num_processors > 0:
            self.logger.info(
                f"fdedup does not support multiprocessing. Runtime_num_processors should be 0, "
                f"current {args.runtime_num_processors}"
            )
            return False
        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup preprocessing params are {self.params}")
        return True


class FdedupPreprocessorPythonTransformRuntimeConfiguration(PythonTransformRuntimeConfiguration):

    def __init__(self):
        super().__init__(
            transform_config=FdedupPreprocessorTransformConfiguration(),
            runtime_class=FdedupPreprocessorRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(FdedupPreprocessorPythonTransformRuntimeConfiguration())
    launcher.launch()
