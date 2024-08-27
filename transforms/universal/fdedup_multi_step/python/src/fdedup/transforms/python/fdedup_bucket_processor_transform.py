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

from argparse import Namespace
from typing import Any, Union

from data_processing.utils import UnrecoverableException, RANDOM_SEED
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing.transform import TransformStatistics
from data_processing.runtime.pure_python import (DefaultPythonTransformRuntime,
                                                 PythonTransformLauncher,
                                                 PythonTransformRuntimeConfiguration
                                                 )
from fdedup.utils import DocsMinHash, MurmurMH, DocCollector, BucketsHash, BucketsHashProcessor
from fdedup.transforms.base import (FdedupBucketProcessorTransformBase,
                                    FdedupBucketProcessorTransformConfigurationBase,
                                    threshold_key, num_permutations_key, minhash_snapshot_directory_key)

# configuration parameters
processor_key = "processor"


class PythonBucketsHashProcessor(BucketsHashProcessor):
    """
    Python specific bucket hash processor
    """
    def __init__(self, params: dict[str, Any]):
        """
        Init method
        :param params - dictionary of parameters containing the following keys
            docs_collector - pointer to the docs collector
            minhash_collector - pointer to the minhash collector
            mn_min_hash - MurmurMH class
            threshold - threshold
            statistics - pointer to statistics
            print_interval - print interval
        """
        super().__init__(params)

    def _submit_generated_docs(self, docs: dict[int, tuple[int, int]], removed: set[int]) -> None:
        """
        Submit generated documents
        :param docs: docs to submit
        :param removed: removed documents
        :return: None
        """
        self.docs_collector.add_documents((list(docs.items()), removed))

    def _get_minhashes_docs(self, doc_ids: list[int]) -> dict[int, tuple[int, list[int]]]:
        """
        Get minhashes for documents by submitting requests to an appropriate doc collectors
        :param doc_ids: doc ids
        :return: doc ids with hashes
        """
        return self.minhash_collector.get_minhashes(doc_ids=doc_ids)


class FdedupBucketProcessorTransform(FdedupBucketProcessorTransformBase):
    """
    Implements fuzzy dedup Bucket Processor (removing duplicates).
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters , with the following keys
            processor - bucket processor
        """
        super().__init__(config)
        self.processor = config.get(processor_key, None)
        if self.processor is None:
            self.logger.error("processor is not defined")
            raise UnrecoverableException("processor is not defined")

    def _submit_bucket_processing(self, buckets: list[Union[tuple[int, int], tuple[int, list[int]]]]) -> None:
        """
        Submit buckets for processing. We are doing this to achieve better processing parallelism
        :param buckets: buckets
        :return: None
        """
        self.processor.process_buckets(buckets)


class FdedupBucketProcessorRuntime(DefaultPythonTransformRuntime):
    """
    fuzzy dedup bucket processor runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        super().__init__(params=params)
        self.buckets = None
        self.minhashes = None
        self.doc_collector = None
        self.bucket_processor = None
        self.logger = get_logger(__name__)
        self.threshold = params.get(threshold_key, 0.8)
        self.num_permutations = params.get(num_permutations_key, 64)

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
        mn_min_hash = MurmurMH(num_perm=self.num_permutations, seed=RANDOM_SEED)
        data_access = data_access_factory.create_data_access()
        # create support classes
        snapshot_path = self.params.get(minhash_snapshot_directory_key, None)
        if snapshot_path is None or len(snapshot_path) == 0:
            mh_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}minhash/minhash_collector_0"
        else:
            mh_path = f"{snapshot_path}/minhash_collector_0"
        self.minhashes = DocsMinHash({"id": 0, "data_access": data_access_factory, "snapshot": mh_path})
        self.buckets = BucketsHash({"id": 0, "data_access": data_access_factory, "snapshot": None})
        self.doc_collector = DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": None})
        self.bucket_processor = PythonBucketsHashProcessor({"threshold": self.threshold,
                                                            "mn_min_hash": mn_min_hash,
                                                            "docs_collector": self.doc_collector,
                                                            "minhash_collector": self.minhashes,
                                                            "statistics": statistics,
                                                            "print_interval": 10})
        return self.params | {processor_key: self.bucket_processor}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        d_size, d_memory, r_size, r_memory = self.doc_collector.get_size()
        stats.add_stats({"number of documents": d_size, "documents memory, GB": d_memory,
                         "number of removed": r_size, "removed memory, GB": r_memory})
        docs, removed = self.doc_collector.get_content()
        self.minhashes.remove_minhashes(removed)
        # build buckets
        buckets = {}
        for key, value in docs.items():
            buckets[value[1]] =  buckets.get(value[1], []) + [key]
        self.buckets.add_buckets(list(buckets.items()))
        self.doc_collector.snapshot()
        self.minhashes.snapshot()
        self.buckets.snapshot()


class FdedupBucketProcessorTransformConfiguration(FdedupBucketProcessorTransformConfigurationBase):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            transform_class=FdedupBucketProcessorTransform,
        )
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.runtime_num_processors > 0:
            self.logger.info(
                f"fdedup does not support multiprocessing. Runtime_num_processors should be 0, "
                f"current {args.runtime_num_processors}"
            )
            return False
        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup buckets processor params are {self.params}")
        return True


class FdedupBucketProcessorPythonTransformRuntimeConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=FdedupBucketProcessorTransformConfiguration(),
            runtime_class=FdedupBucketProcessorRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(FdedupBucketProcessorPythonTransformRuntimeConfiguration())
    launcher.launch()
