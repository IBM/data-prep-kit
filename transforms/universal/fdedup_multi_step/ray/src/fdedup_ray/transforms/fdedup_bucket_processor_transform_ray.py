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

from argparse import Namespace, ArgumentParser
from typing import Any
import time
import numpy as np
import ray
from ray.util import ActorPool
from ray.actor import ActorHandle

from data_processing.utils import UnrecoverableException
from data_processing.data_access import DataAccessFactoryBase
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from fdedup.utils import BucketsHash, BucketsHashProcessor
from fdedup.transforms.base import (FdedupBucketProcessorTransformBase,
                                    FdedupBucketProcessorTransformConfigurationBase,
                                    bucket_processor_cli_prefix, buckets_cache_key,
                                    threshold_key, num_permutations_key,
                                    minhash_snapshot_directory_key, doc_id_snapshot_directory_key)
from fdedup.transforms.python import processor_key
from fdedup_ray.transforms import (bucket_cpu_key, minhash_cpu_key, doc_id_cpu_key,
                                   num_buckets_key, num_doc_id_key, num_minhash_key)

# configuration parameters
bucket_processor_cpu_key = "processor_cpu"
num_bucket_processors_key = "num_processors"
bucket_processor_bucket_cpu_cli_param = f"{bucket_processor_cli_prefix}{bucket_cpu_key}"
bucket_processor_minhash_cpu_cli_param = f"{bucket_processor_cli_prefix}{minhash_cpu_key}"
bucket_processor_docid_cpu_cli_param = f"{bucket_processor_cli_prefix}{doc_id_cpu_key}"
bucket_processor_processor_cpu_cli_param = f"{bucket_processor_cli_prefix}{bucket_processor_cpu_key}"
bucket_processor_num_buckets_cli_param = f"{bucket_processor_cli_prefix}{num_buckets_key}"
bucket_processor_num_minhash_cli_param = f"{bucket_processor_cli_prefix}{num_minhash_key}"
bucket_processor_num_docid_cli_param = f"{bucket_processor_cli_prefix}{num_doc_id_key}"
bucket_processor_num_processors_cli_param = f"{bucket_processor_cli_prefix}{num_bucket_processors_key}"


@ray.remote(scheduling_strategy="SPREAD")
class BucketsHashProcessorInvoker:
    """
    Bucket hash processing coordinator (singleton)
    """

    def __init__(self, bucket_processors: list[ActorHandle]) -> None:
        from data_processing.utils import get_logger
        from ray.util.metrics import Counter
        self.n_processors = len(bucket_processors)
        self.pool = ActorPool(bucket_processors)
        self.submitted = 0
        self.processed = 0
        self.logger = get_logger(__name__)
        self.start = time.time()
        self.submitted_counter = Counter("bucket_processing_requests", "Amount of buckets processing requests")
        self.completed_counter = Counter("buckets_processing_completion", "Amount of bucket processing completed")
        self.failed_counter = Counter("buckets_processing_failures", "Amount of bucket processing failures")

    def submit_for_processing(self, buckets: list[tuple[int, list[int]]]) -> None:
        # Get completed results
        if self.submitted < self.n_processors:  # still have room
            self.pool.submit(lambda a, v: a.process_buckets.remote(v), buckets)
            self.submitted_counter.inc()
            self.logger.debug("Submitted bucket processing request")
            self.submitted += 1
            return
        else:
            while True:
                # we can have several workers fail here
                try:
                    self.pool.get_next_unordered()
                    break
                except Exception as e:
                    self.logger.error(f"Failed to process request worker exception {e}")
                    self.failed_counter.inc()
                    self.processed += 1
            self.processed += 1
            self.completed_counter.inc()
            if self.processed % 100 == 0:
                self.logger.info(f"processed {self.processed} buckets in {round((time.time() - self.start)/60, 3)} min")
            self.logger.debug("Completed bucket processing request")
            self.pool.submit(lambda a, v: a.process_buckets.remote(v), buckets)
            self.submitted += 1
            self.logger.debug("Submitted bucket processing request")
            return

    def wait_for_completion(self) -> None:
        self.logger.info(f"Waiting bucket processing completion. Submitted requests {self.submitted}")
        while self.pool.has_next():
            try:
                self.pool.get_next_unordered()
            except Exception as e:
                self.logger.error(f"Failed to process request worker exception {e}")
                self.failed_counter.inc()
            self.processed += 1
            self.completed_counter.inc()
            if self.processed % 100 == 0:
                self.logger.info(f"processed {self.processed} buckets in {round((time.time() - self.start)/60, 3)} min")


@ray.remote(scheduling_strategy="SPREAD")
class RayBucketsHashProcessor(BucketsHashProcessor):
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
        """
        super().__init__(params)

    def _submit_generated_docs(self, docs: dict[int, tuple[int, int]], removed: set[int]) -> None:
        """
        Submit generated documents
        :param docs: docs to submit
        :param removed: removed documents
        :return: None
        """
        from fdedup_ray.utils import FdedupSupportRay
        # Remove doc ids that are already removed
        for did in removed:
            docs.pop(did, None)
        # update cache
        FdedupSupportRay.update_doc_ids(docs=docs, removed=removed, doc_actors=self.docs_collector, logger=self.logger)

    def _get_minhashes_docs(self, doc_ids: list[int]) -> dict[int, tuple[int, np.array]]:
        """
        Get minhashes for documents by submitting requests to an appropriate doc collectors
        :param doc_ids: doc ids
        :return: doc ids with hashes
        """
        request = [[] for _ in range(len(self.minhash_collector))]
        for value in doc_ids:
            request[value % len(self.minhash_collector)].append(value)
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.minhash_collector[i].get_minhashes.remote(req))
            i += 1
        # Process replies
        hashes = {}
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            reply = ray.get(ready)[0]
            for r in reply:
                hashes[r[0]] = (r[1], r[2])
            remote_replies = not_ready
        return hashes


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
        self.buckets = config.get(buckets_cache_key, None)
        if self.buckets is None:
            raise UnrecoverableException("buckets cache is not defined")

    def _submit_bucket_processing(self, buckets: list[tuple[int, list[int]]]) -> None:
        """
        Submit buckets for processing. We are doing this to achieve better processing parallelism
        :param buckets: buckets
        :return: None
        """
        ray.get(self.processor.submit_for_processing.remote(buckets))

    def _save_buckets(self, file_name: str, buckets: dict[int, list[int]]) -> None:
        """
        save buckets
        :param buckets: buckets
        :return: None
        """
        i = int(file_name[file_name.rfind("_") + 1:])
        self.buckets[i].add_buckets.remote(list(buckets.items()))


class FdedupBucketProcessorRuntime(DefaultRayTransformRuntime):
    """
    fuzzy dedup bucket processor runtime support
    """

    def __init__(self, params: dict[str, Any]):
        from data_processing.utils import get_logger
        super().__init__(params=params)
        self.buckets = None
        self.minhashes = None
        self.doc_collectors = None
        self.bucket_processors = None
        self.invoker = None
        self.logger = get_logger(__name__)
        self.threshold = params.get(threshold_key, 0.8)
        self.num_permutations = params.get(num_permutations_key, 64)
        self.n_buckets = params.get(num_buckets_key, 1)
        self.n_minhash = params.get(num_minhash_key, 1)
        self.n_docid = params.get(num_doc_id_key, 1)
        self.n_processors = params.get(num_bucket_processors_key, 1)
        self.bucket_cpu = params.get(bucket_cpu_key, .5)
        self.minhash_cpu = params.get(minhash_cpu_key, .5)
        self.docid_cpu = params.get(doc_id_cpu_key, .5)
        self.processor_cpu = params.get(bucket_processor_cpu_key, .8)

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
        from fdedup_ray.utils import FdedupSupportRay
        data_access = data_access_factory.create_data_access()
        # create minhashes
        self.minhashes = FdedupSupportRay.create_minhashes_current(
            data_access_factory=data_access_factory, n_actors=self.n_minhash, actor_cpu=self.minhash_cpu,
            directory=self.params.get(minhash_snapshot_directory_key, None), statistics=statistics,
            logger=self.logger)
        # create buckets
        self.buckets = [None] * self.n_buckets
        for i in range(self.n_buckets):
            self.buckets[i] = ray.remote(BucketsHash).options(**{"num_cpus": self.bucket_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None})
        self.logger.info(f"Created {len(self.buckets)} bucket collectors")
        # doc collectors
        self.doc_collectors = FdedupSupportRay.create_doc_id_current(
            data_access_factory=data_access_factory, n_actors=self.n_docid, actor_cpu=self.docid_cpu,
            directory=self.params.get(doc_id_snapshot_directory_key, None), statistics=statistics, logger=self.logger)
        # processors
        processor_config = {"threshold": self.threshold,
                            "docs_collector": self.doc_collectors,
                            "minhash_collector": self.minhashes,
                            "statistics": statistics}
        self.bucket_processors = [
            RayBucketsHashProcessor.options(**{"num_cpus": self.processor_cpu}).remote(processor_config)
            for _ in range(self.n_processors)
        ]
        self.logger.info(f"Created {len(self.bucket_processors)} bucket processors")
        # invoker
        self.invoker = BucketsHashProcessorInvoker.options(**{"num_cpus": .5}).remote(self.bucket_processors)
        self.logger.info("Created processor invoker")
        return self.params | {processor_key: self.invoker, buckets_cache_key: self.buckets}

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
        sum_docs, sum_docs_memory, sum_removed, sum_removed_memory = FdedupSupportRay.get_doc_stats(self.doc_collectors)
        # create snapshots
        FdedupSupportRay.snapshot_caches(bucket_actors=self.buckets, doc_actors=self.doc_collectors,
                                         minhash_actors=self.minhashes, logger=self.logger)
        # return updated statistics
        return {"number of buckets": sum_buckets, "bucket memory, GB": sum_buckets_mem,
                "number of minhashes": sum_mh, "minhashes memory, GB": sum_mh_mem,
                "number of docs": sum_docs, "docs_memory, GB": sum_docs_memory,
                "number of removed": sum_removed, "removed_memory, GB": sum_removed_memory} | stats


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

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        super().add_input_params(parser)
        parser.add_argument(
            f"--{bucket_processor_bucket_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per bucket hash"
        )
        parser.add_argument(
            f"--{bucket_processor_minhash_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per minhash hash"
        )
        parser.add_argument(
            f"--{bucket_processor_docid_cpu_cli_param}",
            type=float,
            default=0.5,
            help="number of CPUs per docid hash"
        )
        parser.add_argument(
            f"--{bucket_processor_processor_cpu_cli_param}",
            type=float,
            default=0.8,
            help="number of CPUs per bucket processor"
        )
        parser.add_argument(
            f"--{bucket_processor_num_minhash_cli_param}",
            type=int,
            default=1,
            help="number of minhash caches to use"
        )
        parser.add_argument(
            f"--{bucket_processor_num_buckets_cli_param}",
            type=int,
            default=1, help="number of bucket hashes to use"
        )
        parser.add_argument(
            f"--{bucket_processor_num_docid_cli_param}",
            type=int,
            default=1, help="number of docid hashes to use"
        )
        parser.add_argument(
            f"--{bucket_processor_num_processors_cli_param}",
            type=int,
            default=2, help="number of bucket processors to use"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        super().apply_input_params(args=args)
        self.logger.info(f"fuzzy dedup buckets processor params are {self.params}")
        return True


class FdedupBucketProcessorRayTransformRuntimeConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=FdedupBucketProcessorTransformConfiguration(),
            runtime_class=FdedupBucketProcessorRuntime,
        )


if __name__ == "__main__":
    launcher = RayTransformLauncher(FdedupBucketProcessorRayTransformRuntimeConfiguration())
    launcher.launch()
