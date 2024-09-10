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
import time
from typing import Any, Iterator, Union

import numpy as np
import ray
from data_processing.data_access import SnapshotUtils
from data_processing.utils import GB, RANDOM_SEED, TransformUtils, get_logger
from data_processing_ray.runtime.ray import RayUtils
from ray.actor import ActorHandle
from ray.util import ActorPool
from scipy.integrate import quad as integrate


NO_SIMILARITY = -1
REQUEST_LEN = 4096
LONG_BUCKET = 5000
LONG_BUCKET_PRINT = 1000


def fuzzy_optimal_param(
    threshold: float,
    num_perm: int,
    false_positive_weight: float,
    false_negative_weight: float,
) -> tuple[int, int]:
    """
    Computes parameters for fuzzy dedup
    :param threshold: filtering threshold
    :param num_perm: number of permutations
    :param false_positive_weight: false positive weight
    :param false_negative_weight: false negative weight
    :return: number of buckets and bucket length
    """

    def _false_positive_probability(ths: float, b: int, r: int) -> float:
        """
        Compute false positive probability
        :param ths: filtering threshold
        :param b: permutation
        :param r: rel permutation
        :return: probability
        """
        _probability = lambda s: 1 - (1 - s ** float(r)) ** float(b)
        a, err = integrate(_probability, 0.0, ths)
        return a

    def _false_negative_probability(ths: float, b: int, r: int) -> float:
        """
        Compute false negative probability
        :param ths: filtering threshold
        :param b: permutation
        :param r: rel permutation
        :return: probability
        """
        _probability = lambda s: 1 - (1 - (1 - s ** float(r)) ** float(b))
        a, err = integrate(_probability, ths, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for perm in range(1, num_perm + 1):
        max_r = int(num_perm / perm)
        for rel in range(1, max_r + 1):
            fp = _false_positive_probability(threshold, perm, rel)
            fn = _false_negative_probability(threshold, perm, rel)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (perm, rel)
    return opt


class MurmurMH:
    def __init__(self, num_perm: int, seed: int = RANDOM_SEED):
        self.seed = seed
        self.num_perm = num_perm
        self.permutations = self._init_permutations(seed, num_perm)

    def minhash(self, shingle_count: int, shingles: Iterator[str]) -> np.array:
        def generator():
            for shingle in shingles:
                yield TransformUtils.str_to_int(shingle)

        hash_values = np.fromiter(generator(), dtype=np.uint64, count=shingle_count)

        result = np.zeros(self.permutations.shape, dtype=np.uint32)
        for i, perm in enumerate(self.permutations):
            result[i] = np.right_shift((perm * hash_values).T, 32).astype(np.uint32).min(axis=0, keepdims=False)
        return result

    @staticmethod
    def _init_permutations(seed: int, num_perm: int) -> np.array:
        # see https://en.wikipedia.org/wiki/Universal_hashing#Avoiding_modular_arithmetic
        max_int = np.uint64((1 << 64) - 1)
        gen = np.random.RandomState(seed)
        # get self.num_perm pseudo random numbers between 2 and max_int (excl)
        permutations = np.array([gen.randint(0, max_int, dtype=np.uint64) for _ in range(num_perm)], dtype=np.uint64).T
        # make all even pseudo random numbers odd by adding 1
        permutations[permutations % 2 == 0] += 1
        return permutations

    @staticmethod
    def jaccard(mh1: np.array, mh2: np.array) -> float:
        return np.count_nonzero(mh1 == mh2)


@ray.remote(scheduling_strategy="SPREAD")
class DocCollector:
    """
    An actor collecting de duped document IDs
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initializer
        """
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        self.removed = set()
        data_access_factory = params.get("data_access")
        self.data_access = data_access_factory.create_data_access()
        snapshot = params.get("snapshot", None)
        if snapshot is None:
            self.ids = {}
        else:
            try:
                bids, _ = self.data_access.get_file(snapshot)
                self.ids = pickle.loads(bids)
            except Exception as e:
                self.logger.warning(f"Failed to load doc collector {self.actor_id} with exception {e}")
                raise e

    def add_documents(self, dr: tuple[list[tuple[int, int]], list[int]]) -> None:
        """
        Add documents and removed document
        :param dr: documents to keep and documents to remove
        :return:
        """
        docs = dr[0]
        rm = dr[1]
        # process documents to remove
        for did in rm:
            self.ids.pop(did, None)
        self.removed.update(rm)
        # process documents to keep
        for key, val in docs:
            if key in self.removed:
                continue
            if key in self.ids and val == NO_SIMILARITY:
                # Do not update existing docs with NO_SIMILARITY
                continue
            else:
                self.ids[key] = val

    def filter(self, docs: list[int]) -> dict[int, int]:
        """
        Filter documents
        :param docs: documents to filter
        :return: documents to keep
        """
        result = {}
        for doc_id in docs:
            r = self.ids.get(doc_id, None)
            if r is not None:
                result[doc_id] = r
        return result

    def snapshot(self) -> None:
        """
        Snapshotting itself
        """
        try:
            b_doc = pickle.dumps(self.ids)
            self.data_access.save_file(
                f"{SnapshotUtils.get_snapshot_folder(self.data_access)}docs/doc_collector_{self.actor_id}", b_doc
            )
        except Exception as e:
            self.logger.warning(f"Failed to snapshot doc collector {self.actor_id} with exception {e}")
            raise e

    def get_size(self) -> tuple[int, float, int, float]:
        """
        get sizes
        :return: number of ids, its memory utilization, number of removed, its memory utilization
        """
        return (
            len(self.ids),
            TransformUtils.deep_get_size(self.ids) / GB,
            len(self.removed),
            TransformUtils.deep_get_size(self.removed) / GB,
        )


@ray.remote(scheduling_strategy="SPREAD")
class DocsMinHash:
    """
    An actor storing min hashes for a doc id
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initialize
        :param params: parameters
        """
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        data_access_factory = params.get("data_access")
        self.data_access = data_access_factory.create_data_access()
        snapshot = params.get("snapshot", None)
        if snapshot is None:
            self.docs = {}
        else:
            try:
                bdocs, _ = self.data_access.get_file(snapshot)
                self.docs = pickle.loads(bdocs)
            except Exception as e:
                self.logger.warning(f"Failed to load minhash collector {self.actor_id} with exception {e}")
                raise e

    def add_minhashes(self, updates: list[tuple[int, int, np.array]]) -> None:
        """
        Add minhashes
        :param updates: minhash for doc_id a tuple of doc len and array of hashes
        :return: None
        """
        for doc_id, length, minhash in updates:
            self.docs[doc_id] = np.concatenate(([length], minhash))

    def get_minhashes(self, doc_ids: list[int]) -> list[tuple[int, int, np.array]]:
        """
        Get minhashes for a list of documents
        :param doc_ids: list of doc ids
        :return: doc id, len, minhashes
        """
        result = []
        for doc_id in doc_ids:
            info = self.docs.get(doc_id)
            if info is not None:
                result.append((doc_id, info[0], info[1:]))
        return result

    def snapshot(self) -> None:
        """
        Snapshotting itself
        """
        try:
            b_doc = pickle.dumps(self.docs)
            self.data_access.save_file(
                f"{SnapshotUtils.get_snapshot_folder(self.data_access)}minhash/minhash_collector_{self.actor_id}",
                b_doc,
            )
        except Exception as e:
            self.logger.warning(f"Failed to snapshot minhash collector {self.actor_id} with exception {e}")
            raise e

    def get_size(self) -> tuple[int, float]:
        """
        Get size of used min hashes
        :return: number of docs, its memory utilization
        """
        return len(self.docs), TransformUtils.deep_get_size(self.docs) / GB


@ray.remote(scheduling_strategy="SPREAD")
class BucketsHash:
    """
    Actor storing buckets information
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initialization
        """
        from ray.util.metrics import Counter

        self.submitter = None
        self.n_buckets = 0
        self.bucket_memory = 0
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        data_access_factory = params.get("data_access")
        self.data_access = data_access_factory.create_data_access()
        snapshot = params.get("snapshot", None)
        if snapshot is None:
            self.buckets = {}
        else:
            try:
                b_buckets, _ = self.data_access.get_file(snapshot)
                self.buckets = pickle.loads(b_buckets)
            except Exception as e:
                self.logger.warning(f"Failed to load buckets collector {self.actor_id} with exception {e}")
                raise e
        self.bucket_created_counter = Counter("bucket_created", "Amount of buckets created")
        self.long_bucket_submit_counter = Counter("long_bucket_submitted", "Amount of long buckets submitted")
        self.short_bucket_submit_counter = Counter("short_bucket_submitted", "Amount of short buckets submitted")

    def add_buckets(self, bck: list[tuple[int, list[int]]]) -> None:
        """
        Add additional buckets to hash
        :param bck: bucket information
        :return: None
        """
        for bucket in bck:
            b_hash = bucket[0]
            buckets_for_hash = self.buckets.get(b_hash)
            if buckets_for_hash:
                if type(buckets_for_hash) == int:
                    self.buckets[b_hash] = [buckets_for_hash] + bucket[1]
                else:
                    buckets_for_hash.extend(bucket[1])
            else:
                if len(bucket[1]) == 1:
                    self.buckets[b_hash] = bucket[1][0]
                else:
                    self.buckets[b_hash] = bucket[1]
                self.bucket_created_counter.inc(1)

    def add_processing_submitter(self, submitter: ActorHandle) -> None:
        """
        Add process submitter
        :param submitter: reference to submitter
        :return:
        """
        self.submitter = submitter

    def process_buckets(self) -> None:
        """
        Process buckets to generate documents
        :return: None
        """

        # Remember usage
        self.n_buckets = len(self.buckets)
        self.bucket_memory = TransformUtils.deep_get_size(self.buckets) / GB

        # split buckets into short and long. Long buckets can take very long to process
        long_buckets = []
        short_buckets = []
        while len(self.buckets) > 0:
            doc_id, bucket = self.buckets.popitem()
            if type(bucket) == list and len(bucket) > LONG_BUCKET:
                # Its long
                long_buckets.append(bucket)
            else:
                short_buckets.append(bucket)
        self.logger.info(f"processing buckets {len(long_buckets)} long, {len(short_buckets)} short")

        # process long buckets first - we are submitting them one at a time
        for bucket in long_buckets:
            if len(bucket) > 2 * LONG_BUCKET:
                # For very long buckets, split them
                self.logger.info(f"Splitting bucket of length len(bucket) into chunks")
                smaller_bucket = [
                    bucket[i * LONG_BUCKET : (i + 1) * LONG_BUCKET]
                    for i in range((len(bucket) + LONG_BUCKET - 1) // LONG_BUCKET)
                ]
                for b in smaller_bucket:
                    ray.get(self.submitter.submit_for_processing.remote([b]))
                    self.long_bucket_submit_counter.inc(1)
            else:
                ray.get(self.submitter.submit_for_processing.remote([bucket]))
                self.long_bucket_submit_counter.inc(1)
        self.logger.info("Done submitting long buckets")

        # And now the rest of buckets
        bucket_chunks = [short_buckets[i * 100 : (i + 1) * 100] for i in range((len(short_buckets) + 99) // 100)]
        for b in bucket_chunks:
            ray.get(self.submitter.submit_for_processing.remote(b))
            self.short_bucket_submit_counter.inc(len(b))

    def snapshot(self) -> None:
        """
        Snapshotting itself
        """
        try:
            b_buckets = pickle.dumps(self.buckets)
            self.data_access.save_file(
                f"{SnapshotUtils.get_snapshot_folder(self.data_access)}buckets/buckets_collector_{self.actor_id}",
                b_buckets,
            )
        except Exception as e:
            self.logger.warning(f"Failed to snapshot buckets collector {self.actor_id} with exception {e}")
            raise e

    def get_size(self) -> tuple[int, float]:
        """
        Get buckets resource utilization
        :return: number of buckets and memory utilization
        """
        return self.n_buckets, self.bucket_memory


@ray.remote(scheduling_strategy="SPREAD")
class BucketsHashProcessor:
    """
    Actor for processing buckets
    """

    def __init__(self, params: dict[str, Any]):
        """
        Init method
        :param params - dictionary of parameters containing the following keys
            remote_docs - handles to the remote docs
            remote_minhashes - handles to the remote minhashes
            mn_min_hash - MurmurMH class
            threshold - threshold
            statistics - statistics actor
        """
        from ray.util.metrics import Counter

        self.threshold = params["threshold"]
        self.mn_min_hash = params["mn_min_hash"]
        self.remote_docs = params["remote_docs"]
        self.remote_minhashes = params["remote_minhashes"]
        self.stats = params["statistics"]
        self.logger = get_logger(__name__)
        self.bucket_processed_counter = Counter("bucket_processed", "Amount of buckets processed")

    def _submit_generated_docs(self, docs: dict[int, int], removed: set[int]) -> None:
        """
        Submit generated documents
        :param docs: docs to submit
        :param removed: removed documents
        :return: None
        """
        # Remove doc ids that are already removed
        for did in removed:
            docs.pop(did, None)
        # Build remote requests
        request = [([], []) for _ in range(len(self.remote_docs))]
        for key, value in docs.items():
            req_tuple = request[key % len(self.remote_docs)]
            req_tuple[0].append((key, value))
        for did in removed:
            req_tuple = request[did % len(self.remote_docs)]
            req_tuple[1].append(did)
        # Submit requests and wait for replies
        remote_replies = []
        i = 0
        for req in request:
            if len(req[0]) > 0 or len(req[1]) > 0:  # Only submit if the request has data
                remote_replies.append(self.remote_docs[i].add_documents.remote(req))
            i += 1
        # Process replies
        RayUtils.wait_for_execution_completion(logger=self.logger, replies=remote_replies)

    # get minhashes and length for docs in the bucket
    def _get_minhashes_docs(self, doc_ids: list[int]) -> dict[int, tuple[int, list[int]]]:
        """
        Get minhashes for documents by submitting requests to an appropriate doc collectors
        :param doc_ids: doc ids
        :return: doc ids with hashes
        """
        request = [[] for _ in range(len(self.remote_minhashes))]
        for value in doc_ids:
            request[value % len(self.remote_minhashes)].append(value)
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(self.remote_minhashes[i].get_minhashes.remote(req))
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

    def process_buckets(self, buckets: list[Union[int, list[int]]]) -> None:
        """
        process buckets to generate documents
        :param buckets: buckets
        :return: none
        """
        t_start = time.time()
        docs = {}
        removed = set()
        for bucket in buckets:
            if type(bucket) == int:
                # This hash has a single document
                if bucket not in docs:
                    docs[bucket] = NO_SIMILARITY
                self.bucket_processed_counter.inc(1)
                continue
            # multiple documents
            start = time.time()
            bucket_len = len(bucket)
            very_long = bucket_len > LONG_BUCKET

            hashes = self._get_minhashes_docs(bucket)
            set_list = []
            unvisited = set(bucket)

            # combine similar documents
            index = 0
            while len(unvisited) > 0:
                current_doc_id = unvisited.pop()
                current_mh = hashes[current_doc_id][1]
                current_set = set()
                for other_doc_id in bucket:
                    if other_doc_id in unvisited:
                        other_mh = hashes[other_doc_id][1]
                        if self.mn_min_hash.jaccard(current_mh, other_mh) >= self.threshold:
                            current_set.add(current_doc_id)
                            current_set.add(other_doc_id)
                            unvisited.discard(other_doc_id)
                if len(current_set) > 0:
                    set_list.append(current_set)
                index += 1
                if index % LONG_BUCKET_PRINT == 0:
                    self.logger.info(f"processing very long {bucket_len} bucket, {index} documents so far")
            if index > LONG_BUCKET_PRINT:
                self.logger.info(f"done processing very long {bucket_len}")

            # process created sets
            for current_set in set_list:
                for d in current_set:
                    bucket.remove(d)
                removed.update(current_set)
                for i, doc_id in enumerate(current_set):
                    if i == 0:
                        cluster_id = doc_id
                        remaining = doc_id
                        min_len = hashes[doc_id][0]
                        max_len = min_len
                        continue
                    c_len = hashes[doc_id][0]
                    if c_len > max_len:
                        max_len = c_len
                        remaining = doc_id
                        continue
                    if c_len <= min_len:
                        min_len = c_len
                        cluster_id = doc_id
                docs[remaining] = cluster_id
                removed.discard(remaining)

            # if we did not find docs in connections, submit them as NO_SIMILARITY
            for d in bucket:
                if d not in docs:
                    docs[d] = NO_SIMILARITY
            if very_long:
                self.logger.info(
                    f"Processed long ({bucket_len}) bucket in {round((time.time() - start) / 60.,3)} "
                    f"min; "
                    f"docs chains {len(set_list)}"
                )
            self.bucket_processed_counter.inc(1)
        # Submit docs
        self._submit_generated_docs(docs, removed)
        # peg stats
        self.stats.add_stats.remote({"generated doc_ids": len(docs), "bucket processing time": time.time() - t_start})


@ray.remote(scheduling_strategy="SPREAD")
class BucketsHashProcessorInvoker(object):
    """
    Bucket hash processing coordinator (singleton)
    """

    def __init__(self, bucket_processors: list[ActorHandle]) -> None:
        self.n_processors = len(bucket_processors)
        self.pool = ActorPool(bucket_processors)
        self.submitted = 0
        self.processed = 0
        self.logger = get_logger(__name__)
        self.start = time.time()

    def submit_for_processing(self, buckets: list[Union[int, list[int]]]) -> None:
        # Get completed results
        if self.submitted < self.n_processors:  # still have room
            self.pool.submit(lambda a, v: a.process_buckets.remote(v), buckets)
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
                    self.processed += 1
            self.processed += 1
            if self.processed % 100 == 0:
                self.logger.info(f"processed {self.processed} buckets in {(time.time() - self.start)/60} min")
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
            self.processed += 1
            if self.processed % 100 == 0:
                self.logger.info(f"processed {self.processed} buckets in {(time.time() - self.start)/60} min")
