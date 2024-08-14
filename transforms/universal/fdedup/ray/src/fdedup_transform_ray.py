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

import random
import time
from argparse import ArgumentParser, Namespace
from typing import Any

import mmh3
import numpy as np
import pyarrow as pa
import ray
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import (
    RANDOM_SEED,
    CLIArgumentProvider,
    TransformUtils,
    str2bool,
)
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformFileProcessor,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from fdedup_support import (
    REQUEST_LEN,
    BucketsHash,
    BucketsHashProcessor,
    BucketsHashProcessorInvoker,
    DocCollector,
    DocsMinHash,
    MurmurMH,
    fuzzy_optimal_param,
)
from ray.actor import ActorHandle
from ray.util import ActorPool


short_name = "fdedup"
cli_prefix = f"{short_name}_"


class FdedupTransform(AbstractTableTransform):
    """
    Implements fuzzy dedup data preprocessor (building tables and minhashes).
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        :param config: initialization parameters, with the following keys
            doc_column - name of doc column
            doc_id_int_column - name of int doc id column
            word_shingle_size - word shingle size
            mn_min_hash - MurmurMH class
            num_bands - number of bands
            length_band band length
            remote_buckets - bucket actors
            remote_minhashes - minhash actors
            delimiter - delimiter
            random_delay_limit - random delay limit
        """
        super().__init__(config)
        self.doc_column = config.get("doc_column", "")
        self.doc_id_column = config.get("doc_id_int_column", "")
        self.word_shingle_size = config.get("word_shingle_size", 1)
        self.delimiter = config.get("delimiter", " ")
        self.mn_min_hash = config.get("mn_min_hash", None)
        self.num_bands = config.get("num_bands", 1)
        self.length_band = config.get("length_band", 1)
        self.buckets = config.get("remote_buckets", [])
        self.minhashes = config.get("remote_minhashes", [])
        self.random_delay_limit = config.get("random_delay_limit", 10)

    def _generate_minhashes(self, shingles: list[str]) -> np.array:
        """
        Generate minhashes
        :param shingles:
        :return: generated minhashes
        """
        min_hashes = self.mn_min_hash.minhash(len(shingles), shingles)
        num_min_hashes = len(min_hashes)
        assert self.num_bands * self.length_band <= num_min_hashes, (
            f"num_bans*band_len must be <= num min hashes, was num_bands={self.num_bands}, "
            f"bands_len={self.length_band}, num_min hashes={num_min_hashes}"
        )
        return min_hashes

    def _generate_buckets(self, min_hashes: np.array) -> list[int]:
        """
        Generate buckets
        :param min_hashes: array of minhashes
        :return:
        """
        return [
            mmh3.hash64(min_hashes[i * self.length_band : (i + 1) * self.length_band], seed=RANDOM_SEED, signed=False)[
                0
            ]
            for i in range(self.num_bands)
        ]

    def _submit_buckets_minhashes(
        self, buckets: dict[int, list[int]], minhashes: list[tuple[int, int, np.array]]
    ) -> None:
        """
        Submit buckets to hash
        :param buckets: buckets
        :param minhashes: minhashes
        :return: None
        """
        # bucket requests
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
        # Minhashes
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

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Preprocessing table content.
        :param table: table
        :param file_name - name of currently processed file
        :return: resulting table, statistics
        """
        from compute_shingles import compute_shingles

        def flush(limit: int) -> None:
            """
            flushing buckets and minhashes to dedicated actors
            :param limit: number of buckets to flush
            :return: None
            """
            if len(buckets) >= limit:  # time to submit
                nonlocal num_buckets
                nonlocal num_minhashes
                self._submit_buckets_minhashes(buckets, minhashes)
                num_buckets = num_buckets + len(buckets)
                num_minhashes = num_minhashes + len(minhashes)
                buckets.clear()
                minhashes.clear()

        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column, self.doc_id_column])
        # Inner variables
        buckets = {}
        minhashes = []
        num_buckets = 0
        num_minhashes = 0
        docs = table[self.doc_column]
        doc_ids = table[self.doc_id_column]
        # for every document/its integer id
        for n in range(table.num_rows):
            doc = docs[n].as_py()
            doc_id = doc_ids[n].as_py()
            shingles = compute_shingles(txt=doc, word_shingle_size=self.word_shingle_size, delimiter=self.delimiter)
            if len(shingles) > 0:
                mh = self._generate_minhashes(shingles)
                minhashes.append((doc_id, len(doc), mh))
                candidates = self._generate_buckets(mh)

                for b_hash in candidates:
                    bucket_array = buckets.get(b_hash)
                    if bucket_array is None:
                        buckets[b_hash] = [doc_id]
                    else:
                        bucket_array.append(doc_id)
                flush(REQUEST_LEN)
        flush(0)
        # peg stats
        stats = {"generated buckets": num_buckets, "generated minhashes": num_minhashes}
        time.sleep(int(random.random() * self.random_delay_limit))
        return [], stats


class FdedupFilter(AbstractTableTransform):
    """
    Filtering documents
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            doc_column - name of doc column
            doc_id_int_column - name of int doc id column
            cluster_column - name of the cluster column
            remote_docs - list of remote doc collectors
            random_delay_limit - random delay limit
        """
        super().__init__(config)
        self.doc_column = config.get("doc_column", "")
        self.doc_id_column = config.get("doc_id_int_column", "")
        self.cluster_column = config.get("cluster_column", "")
        self.docs = config.get("remote_docs", "")
        self.random_delay_limit = config.get("random_delay_limit", 10)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        De duping (filtering) table content.
        :param table: table
        :param file_name: name of the currently processing file
        :return: resulting table, statistics
        """
        # make sure that the doc column exists
        TransformUtils.validate_columns(table=table, required=[self.doc_column, self.doc_id_column])
        # inner variables
        ids = table.column(self.doc_id_column)
        # Submit requests to an appropriate doc collectors
        request = [[] for _ in range(len(self.docs))]
        for value in ids:
            doc_id = value.as_py()
            request[doc_id % len(self.docs)].append(doc_id)
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.docs[i].filter.remote(req))
            i += 1
        # Process replies
        unique = {}
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            reply = ray.get(ready)[0]
            unique.update(reply)
            remote_replies = not_ready
        # Filter out table
        mask = []
        clusters = []
        # Actual filtering
        for n in range(table.num_rows):
            doc_id = ids[n].as_py()
            if doc_id in unique:
                mask.append(True)
                clusters.append(unique.pop(doc_id))
            else:
                mask.append(False)
        # build out table
        out_table = TransformUtils.add_column(table=table.filter(mask), name=self.cluster_column, content=clusters)
        # build execution statistics
        stats = {"source_documents": table.num_rows, "result_documents": out_table.num_rows}
        time.sleep(int(random.random() * self.random_delay_limit))
        return [out_table], stats


class FdedupRuntime(DefaultRayTransformRuntime):
    """
    Fuzzy dedup runtime support. Here we are using set environment to implement first two steps of fuzzy dedup
    processing - preprocessing and bucket hash processing
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            doc_column - name of the document column
            id_column - name of the integer doc id column
            cluster_column - name of the cluster column
            worker_options - start options for preprocessor - from the orchestrator configuration
            bucket_cpu - number of cpus for bucket actor
            doc_cpu - number of cpus for doc actor
            mhash_cpu - number of cpus for minhash actor
            num_doc_actors - number of document actors
            num_bucket_actors - number of bucket actors
            num_minhash_actors - number of minhash actors
            num_preprocessors - number of preprocessors
            snapshot_delay - delay (sec) in sending snapshot requests to actors
            use_bucket_snapshot - use bucket snapshot
            use_doc_snapshot - use doc snapshot
            random_delay_limit - random_delay limit
            # fuzzy specific parameters
            num_permutations - number of permutations
            threshold - threshold
            world_shingle_size - word shingles size
            delimiters - delimiter
        """
        from data_processing.utils import get_logger

        super().__init__(params)
        self.logger = get_logger(__name__)
        self.sum_buckets = 0
        self.sum_buckets_mem = 0
        self.sum_mh = 0
        self.sum_mh_mem = 0
        self.document_collectors = []
        self.snapshot_delay = self.params.get("snapshot_delay", 1)
        self.random_delay_limit = self.params.get("random_delay_limit", 10)

    def get_transform_config(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        if self.params.get("use_doc_snapshot", False):
            self.logger.info("continuing from the document actors snapshot")
            data_access = data_access_factory.create_data_access()
            path = f"{SnapshotUtils.get_snapshot_folder(data_access)}docs"
            files, retries = data_access.get_folder_files(path=path)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            self.logger.info(f"Found the following snapshot files {files.keys()}")
            self.document_collectors = [None] * len(files)
            for file in files.keys():
                i = int(file[file.rfind("_") + 1 :])
                self.document_collectors[i] = DocCollector.options(
                    **{"num_cpus": self.params.get("doc_cpu", 0.5)}
                ).remote({"id": i, "data_access": data_access_factory, "snapshot": file})
                time.sleep(self.snapshot_delay)
            self.logger.info(f"Created {len(self.document_collectors)} document collectors to continue processing")
        else:
            self.logger.info("starting run from the beginning")
            self._create_doc_actors(data_access_factory=data_access_factory, statistics=statistics, files=files)
        return {
            "doc_column": self.params.get("doc_column", ""),
            "doc_id_int_column": self.params.get("id_column", ""),
            "cluster_column": self.params.get("cluster_column", ""),
            "remote_docs": self.document_collectors,
            "random_delay_limit": self.random_delay_limit,
        }

    def _create_doc_actors(
        self, data_access_factory: DataAccessFactoryBase, statistics: ActorHandle, files: list[str]
    ) -> None:
        """
        Create document actors
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: None
        """
        mn_min_hash = MurmurMH(num_perm=self.params.get("num_permutations", 64), seed=RANDOM_SEED)
        if self.params.get("use_bucket_snapshot", False):
            self.logger.info("continuing from the bucket actors snapshot")
            data_access = data_access_factory.create_data_access()
            # recreate bucket collectors
            path = f"{SnapshotUtils.get_snapshot_folder(data_access)}buckets"
            files, retries = data_access.get_folder_files(path=path)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            self.logger.debug(f"Found the following bucket snapshot files {files.keys()}")
            bucket_collectors = [None] * len(files)
            for file in files.keys():
                i = int(file[file.rfind("_") + 1 :])
                bucket_collectors[i] = BucketsHash.options(**{"num_cpus": self.params.get("bucket_cpu", 0.5)}).remote(
                    {"id": i, "data_access": data_access_factory, "snapshot": file}
                )
                time.sleep(self.snapshot_delay)
            self.logger.info(f"Created {len(bucket_collectors)} bucket collectors to continue processing")
            # recreate minhash collectors
            path = f"{SnapshotUtils.get_snapshot_folder(data_access)}minhash"
            files, retries = data_access.get_folder_files(path=path)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            self.logger.debug(f"Found the following minhash snapshot files {files.keys()}")
            minhash_collectors = [None] * len(files)
            for file in files.keys():
                i = int(file[file.rfind("_") + 1 :])
                minhash_collectors[i] = DocsMinHash.options(**{"num_cpus": self.params.get("mhash_cpu", 0.5)}).remote(
                    {"id": i, "data_access": data_access_factory, "snapshot": file}
                )
                time.sleep(self.snapshot_delay)
            self._process_buckets(
                data_access_factory=data_access_factory,
                statistics=statistics,
                bucket_collectors=bucket_collectors,
                minhash_collectors=minhash_collectors,
                mn_min_hash=mn_min_hash,
            )
            self.logger.info(f"Created {len(minhash_collectors)} minhash collectors to continue processing")
        else:
            self.logger.info("continuing from the very beginning")
            self._create_doc_actors_internal(
                data_access_factory=data_access_factory, statistics=statistics, mn_min_hash=mn_min_hash, files=files
            )

    def _create_doc_actors_internal(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: ActorHandle,
        mn_min_hash: MurmurMH,
        files: list[str],
    ) -> None:
        """
        Create document actors
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param mn_min_hash - MurmurMH class
        :param files - list of files to process
        :return: None
        """
        # compute fuzzy dedup parameters
        num_buckets, length_bucket = fuzzy_optimal_param(
            threshold=self.params.get("threshold", 0.8),
            num_perm=self.params.get("num_permutations", 64),
            false_positive_weight=0.5,
            false_negative_weight=0.5,
        )
        self.logger.info(f"Fuzzy: num buckets {num_buckets}, bucket length {length_bucket}")
        # Build bucket and minhash collectors
        bucket_collectors = [None] * self.params.get("num_bucket_actors", 1)
        for i in range(self.params.get("num_bucket_actors", 1)):
            bucket_collectors[i] = BucketsHash.options(**{"num_cpus": self.params.get("bucket_cpu", 0.5)}).remote(
                {"id": i, "data_access": data_access_factory}
            )
        self.logger.info(f"created {len(bucket_collectors)} bucket actors")
        minhash_collectors = [None] * self.params.get("num_minhash_actors", 1)
        for i in range(self.params.get("num_minhash_actors", 1)):
            minhash_collectors[i] = DocsMinHash.options(**{"num_cpus": self.params.get("mhash_cpu", 0.5)}).remote(
                {"id": i, "data_access": data_access_factory}
            )
        self.logger.info(f"created {len(minhash_collectors)} minhash actors")
        self._preprocess_tables(
            data_access_factory=data_access_factory,
            statistics=statistics,
            files=files,
            mn_min_hash=mn_min_hash,
            num_buckets=num_buckets,
            length_bucket=length_bucket,
            bucket_collectors=bucket_collectors,
            minhash_collectors=minhash_collectors,
            random_delay_limit=self.random_delay_limit,
        )
        # At this point we can snapshot both bucket and minhash collectors for potential restart
        self.logger.info("creating minhash snapshots")
        minhash_replies = [None] * len(minhash_collectors)
        index = 0
        for collector in minhash_collectors:
            minhash_replies[index] = collector.snapshot.remote()
            index += 1
            time.sleep(self.snapshot_delay)
        while minhash_replies:
            ready, not_ready = ray.wait(minhash_replies)
            minhash_replies = not_ready
        self.logger.info("minhash snapshots created")
        self.logger.info("creating bucket snapshots")
        bucket_replies = [None] * len(bucket_collectors)
        index = 0
        for collector in bucket_collectors:
            bucket_replies[index] = collector.snapshot.remote()
            index += 1
            time.sleep(self.snapshot_delay)
        while bucket_replies:
            ready, not_ready = ray.wait(bucket_replies)
            bucket_replies = not_ready
        self.logger.info("bucket snapshots created")
        self._process_buckets(
            data_access_factory=data_access_factory,
            statistics=statistics,
            bucket_collectors=bucket_collectors,
            minhash_collectors=minhash_collectors,
            mn_min_hash=mn_min_hash,
        )

    def _process_buckets(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: ActorHandle,
        bucket_collectors: list[ActorHandle],
        minhash_collectors: list[ActorHandle],
        mn_min_hash: MurmurMH,
    ) -> None:
        """
        Process buckets
        :param data_access_factory - data access factory
        :param statistics - statistics actor
        :param bucket_collectors - bucket collectors
        :param minhash_collectors - minhash collectors
        :param mn_min_hash - MMurmurMH class
        :return: None
        """
        # Create document collectors
        self.document_collectors = [None] * self.params.get("num_doc_actors", 1)
        for i in range(self.params.get("num_doc_actors", 1)):
            self.document_collectors[i] = DocCollector.options(**{"num_cpus": self.params.get("doc_cpu", 0.5)}).remote(
                {"id": i, "data_access": data_access_factory}
            )
        self.logger.info(f"created {len(self.document_collectors)} document actors")
        # create bucket processors
        bucket_processors_list = RayUtils.create_actors(
            clazz=BucketsHashProcessor,
            params={
                "remote_docs": self.document_collectors,
                "remote_minhashes": minhash_collectors,
                "mn_min_hash": mn_min_hash,
                "threshold": self.params.get("threshold", 0.8) * self.params.get("num_permutations", 64),
                "statistics": statistics,
            },
            actor_options=self.params.get("worker_options", None),
            n_actors=self.params.get("num_preprocessors", 1),
        )
        self.logger.info(f"created {len(bucket_processors_list)} bucket processor actors")
        # create bucket processors invoker
        bucket_processor_invoker = BucketsHashProcessorInvoker.options(
            num_cpus=self.params.get("bucket_cpu", 0.5)
        ).remote(bucket_processors=bucket_processors_list)
        self.logger.info(f"created bucket processor invoker")
        # Add invoker to the buckets
        bucket_replies = [
            collector.add_processing_submitter.remote(submitter=bucket_processor_invoker)
            for collector in bucket_collectors
        ]
        RayUtils.wait_for_execution_completion(logger=self.logger, replies=bucket_replies)
        self.logger.info(f"added invoker to bucket collectors")
        # start bucket processing and wait for completion
        start = time.time()
        bucket_replies = [collector.process_buckets.remote() for collector in bucket_collectors]
        RayUtils.wait_for_execution_completion(logger=self.logger, replies=bucket_replies)
        # Wait for pool to complete
        ray.get(bucket_processor_invoker.wait_for_completion.remote())
        self.logger.info(f"Done processing buckets in {round((time.time() - start) / 60.,3)} min")
        # At this point we can save doc actors, in case we would want to restart here
        self.logger.info(f"creating document snapshots")
        doc_replies = [None] * len(self.document_collectors)
        index = 0
        for collector in self.document_collectors:
            doc_replies[index] = collector.snapshot.remote()
            index += 1
            time.sleep(self.snapshot_delay)
        while doc_replies:
            ready, not_ready = ray.wait(doc_replies)
            doc_replies = not_ready
        self.logger.info(f"document snapshots created")
        # At this point we do not need bucket and minhash actors, remove them
        # but first get usage information
        # Bucket collector
        replies = [collector.get_size.remote() for collector in bucket_collectors]
        while replies:
            ready, not_ready = ray.wait(replies)
            b_amount, b_memory = ray.get(ready)[0]
            self.sum_buckets += b_amount
            self.sum_buckets_mem += b_memory
            replies = not_ready
        for collector in bucket_collectors:
            ray.kill(actor=collector, no_restart=True)
        # minhash collector
        replies = [collector.get_size.remote() for collector in minhash_collectors]
        while replies:
            ready, not_ready = ray.wait(replies)
            m_amount, m_memory = ray.get(ready)[0]
            self.sum_mh += m_amount
            self.sum_mh_mem += m_memory
            replies = not_ready
        for collector in minhash_collectors:
            ray.kill(actor=collector, no_restart=True)
        # Clean up processors
        for processor in bucket_processors_list:
            ray.kill(actor=processor, no_restart=True)
        ray.kill(bucket_processor_invoker)

    def _preprocess_tables(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: ActorHandle,
        files: list[str],
        mn_min_hash: MurmurMH,
        num_buckets: int,
        length_bucket: int,
        bucket_collectors: list[ActorHandle],
        minhash_collectors: list[ActorHandle],
        random_delay_limit: int,
    ) -> None:
        """
        Preprocess tables - build, run and cleanup
        :param data_access_factory - data access factory
        :param statistics - statistics actor
        :param files - list of files to process
        :param mn_min_hash - MurmurMH class
        :param num_buckets - number of buckets
        :param length_bucket - bucket length
        :param bucket_collectors - bucket collector actors
        :param minhash_collectors - minhash_collector actors
        :param random_delay_limit - max for random dalay limit
        :return: None
        """
        from ray.util.metrics import Gauge

        worker_options = self.params.get("worker_options", None)
        # Here we are limiting the number of readers not to overwhelm COS
        n_readers = self.params.get("num_preprocessors", 1)
        if n_readers > 1000:
            n_readers = 1000
        self.logger.info(f"Table preprocessing uses {n_readers} readers")
        # Create preprocessing actors
        processor_params = {
            "data_access_factory": data_access_factory,
            "transform_class": FdedupTransform,
            "statistics": statistics,
            "transform_params": {
                "doc_column": self.params.get("doc_column", ""),
                "doc_id_int_column": self.params.get("id_column", ""),
                "word_shingle_size": self.params.get("world_shingle_size", 1),
                "mn_min_hash": mn_min_hash,
                "num_bands": num_buckets,
                "length_band": length_bucket,
                "remote_buckets": bucket_collectors,
                "remote_minhashes": minhash_collectors,
                "delimiter": self.params.get("delimiter", " "),
                "random_delay_limit": random_delay_limit,
            },
            "base_table_stats": False,
        }
        processors_list = RayUtils.create_actors(
            clazz=RayTransformFileProcessor,
            params=processor_params,
            actor_options=worker_options,
            n_actors=n_readers,
        )
        self.logger.info(f"created {len(processors_list)} table processor actors")
        # Execute preprocessing
        # create gauges
        files_in_progress_gauge = Gauge(
            "preprocessing_files_in_progress", "Number of files in progress, preprocessing"
        )
        files_completed_gauge = Gauge(
            "preprocessing_files_processed_total", "Number of files completed, preprocessing"
        )
        available_cpus_gauge = Gauge("preprocessing_available_cpus", "Number of available CPUs, preprocessing")
        available_gpus_gauge = Gauge("preprocessing_available_gpus", "Number of available GPUs, preprocessing")
        available_memory_gauge = Gauge("preprocessing_available_memory", "Available memory, preprocessing")
        available_object_memory_gauge = Gauge(
            "preprocessing_available_object_store", "Available object store, preprocessing"
        )
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        # process data
        processors = ActorPool(processors_list)
        failures = RayUtils.process_files(
            executors=processors,
            files=files,
            print_interval=print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
            logger=self.logger,
        )
        if failures > 0:
            statistics.add_stats.remote({"actor failures": failures})
        # Clean up processors
        for processor in processors_list:
            ray.kill(actor=processor, no_restart=True)
        del processors

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        # Get document collector statistics
        sum_docs = 0
        sum_docs_mem = 0
        sum_removed = 0
        sum_removed_mem = 0
        replies = [collector.get_size.remote() for collector in self.document_collectors]
        while replies:
            ready, not_ready = ray.wait(replies)
            d_amount, d_memory, r_amount, r_memory = ray.get(ready)[0]
            sum_docs += d_amount
            sum_docs_mem += d_memory
            sum_removed += r_amount
            sum_removed_mem += r_memory
            replies = not_ready
        overall_hash_memory = self.sum_buckets_mem + self.sum_mh_mem + sum_docs_mem + sum_docs_mem + sum_removed_mem
        dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 1))
        return {
            "number of buckets": self.sum_buckets,
            "number of docs": sum_docs,
            "number of removed docs": sum_removed,
            "number of min hashes": self.sum_mh,
            "overall hash memory GB": overall_hash_memory,
            "de duplication %": dedup_prst,
        } | stats


class FdedupTableTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=FdedupFilter,
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        """
        parser.add_argument(f"--{cli_prefix}doc_column", type=str, default="contents", help="document column name")
        parser.add_argument(
            f"--{cli_prefix}id_column", type=str, default="int_document_id", help="integer document id column name"
        )
        parser.add_argument(f"--{cli_prefix}cluster_column", type=str, default="cluster", help="cluster column name")
        parser.add_argument(
            f"--{cli_prefix}bucket_cpu", type=float, default=0.5, help="number of CPUs per bucket hash"
        )
        parser.add_argument(
            f"--{cli_prefix}mhash_cpu", type=float, default=0.5, help="number of CPUs per minhash hash"
        )
        parser.add_argument(f"--{cli_prefix}doc_cpu", type=float, default=0.5, help="number of CPUs per doc hash")
        parser.add_argument(f"--{cli_prefix}num_doc_actors", type=int, default=1, help="number of doc actors to use")
        parser.add_argument(
            f"--{cli_prefix}num_minhash_actors", type=int, default=1, help="number of minhash actors to use"
        )
        parser.add_argument(
            f"--{cli_prefix}num_bucket_actors", type=int, default=1, help="number of bucket actors to use"
        )
        parser.add_argument(
            f"--{cli_prefix}num_preprocessors", type=int, default=1, help="number of preprocessors to use"
        )
        parser.add_argument(f"--{cli_prefix}num_permutations", type=int, default=64, help="number of permutations")
        parser.add_argument(f"--{cli_prefix}threshold", type=float, default=0.8, help="threshold")
        parser.add_argument(f"--{cli_prefix}shingles_size", type=int, default=5, help="number of words in shingle")
        parser.add_argument(
            f"--{cli_prefix}delimiters", type=str, default=" ", help="delimiter for splitting document"
        )
        parser.add_argument(f"--{cli_prefix}snapshot_delay", type=int, default=1, help="snapshot delay time")
        parser.add_argument(
            f"--{cli_prefix}use_bucket_snapshot",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="flag to continue with bucket snapshot",
        )
        parser.add_argument(
            f"--{cli_prefix}use_doc_snapshot",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="flag to continue with doc snapshot",
        )
        parser.add_argument(
            f"--{cli_prefix}random_delay_limit", type=int, default=10, help="maximum delay between read"
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.params["worker_options"] = args.runtime_worker_options
        if self.params["use_bucket_snapshot"] and self.params["use_doc_snapshot"]:
            self.logger.warning("both bucket and doc snapshot are specified. Only one allowed")
            return False

        self.logger.info(f"fuzzy dedup params are {self.params}")
        return True


class FdedupRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=FdedupTableTransformConfiguration(), runtime_class=FdedupRuntime)


if __name__ == "__main__":
    launcher = RayTransformLauncher(FdedupRayTransformConfiguration())
    launcher.launch()
