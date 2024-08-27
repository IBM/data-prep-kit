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
from data_processing.data_access import SnapshotUtils
from data_processing.utils import GB, RANDOM_SEED, TransformUtils, UnrecoverableException
from scipy.integrate import quad as integrate

NO_SIMILARITY = -1
LONG_BUCKET = 5000


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


class DocCollector:
    """
    Class collecting de duped document IDs
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initializer
        """
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        self.removed = set()
        data_access_factory = params.get("data_access", None)
        if data_access_factory is None:
            raise UnrecoverableException("Bucket hash, data access factory is not defined")
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

    def add_documents(self, dr: tuple[list[tuple[int, tuple[int, int]]], list[int]]) -> None:
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
                result[doc_id] = r[0]
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

    def get_content(self) -> tuple[dict[int, tuple[int, int]], set[int]]:
        return self.ids, self.removed


class DocsMinHash:
    """
    Class storing min hashes for a doc id
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initialize
        :param params: parameters
        """
        from data_processing.utils import get_logger
        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        data_access_factory = params.get("data_access", None)
        if data_access_factory is None:
            raise UnrecoverableException("Bucket hash, data access factory is not defined")
        self.data_access = data_access_factory.create_data_access()
        snapshot = params.get("snapshot", None)
        if snapshot is None:
            self.minhashes = {}
        else:
            try:
                bdocs, _ = self.data_access.get_file(snapshot)
                self.minhashes = pickle.loads(bdocs)
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
            self.minhashes[doc_id] = np.concatenate(([length], minhash))

    def remove_minhashes(self, docs: list[int]) -> None:
        """
        remove minhashes
        :param docs: removed docs
        :return: None
        """
        for d in docs:
            self.minhashes.pop(d, None)

    def get_minhashes(self, doc_ids: list[int]) -> dict[int, tuple[int, np.array]]:
        """
        Get minhashes for a list of documents
        :param doc_ids: list of doc ids
        :return: doc id, len, minhashes
        """
        result = {}
        for doc_id in doc_ids:
            info = self.minhashes.get(doc_id)
            if info is not None:
                result[doc_id] = (info[0], info[1:])
        return result

    def snapshot(self) -> None:
        """
        Snapshotting itself
        """
        try:
            b_doc = pickle.dumps(self.minhashes)
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
        return len(self.minhashes), TransformUtils.deep_get_size(self.minhashes) / GB

    def get_content(self) -> dict[int, tuple[int, np.array]]:
        return self.minhashes


class BucketsHash:
    """
    Class storing buckets information
    """

    def __init__(self, params: dict[str, Any]):
        """
        Initialization
        """
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        self.actor_id = params.get("id")
        data_access_factory = params.get("data_access", None)
        if data_access_factory is None:
            raise UnrecoverableException("Bucket hash, data access factory is not defined")
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

    def add_buckets(self, bck: list[tuple[int, list[int]]]) -> None:
        """
        Add additional buckets to hash
        :param bck: bucket information
        :return: None
        """
        for bucket in bck:
            b_hash = bucket[0]
            self.buckets[b_hash] = self.buckets.get(b_hash, []) + bucket[1]

    def snapshot(self) -> None:
        """
        Snapshot itself
        :return: None
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
        return len(self.buckets), TransformUtils.deep_get_size(self.buckets) / GB

    def get_content(self) -> dict[int, list[int]]:
        return self.buckets


class BucketsHashProcessor:
    """
    class for processing buckets
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
        from data_processing.utils import get_logger

        self.threshold = params.get("threshold", .8)
        self.mn_min_hash = params.get("mn_min_hash", None)
        self.docs_collector = params.get("docs_collector", None)
        self.minhash_collector = params.get("minhash_collector", None)
        self.stats = params.get("statistics", None)
        self.print_interval = params.get("print_interval", 10)
        self.logger = get_logger(__name__)
        if self.mn_min_hash is None or self.docs_collector is None or self.minhash_collector is None or self.stats is None:
            self.logger.error(f"Not all processor parameters are defined: mn_min_hash {self.mn_min_hash}; "
                              f"docs_collector {self.docs_collector}; minhash_collector {self.minhash_collector} "
                              f"stats {self.stats}")
            raise UnrecoverableException("Failed to create BucketsHashProcessor")

    def _submit_generated_docs(self, docs: dict[int, tuple[int, int]], removed: set[int]) -> None:
        """
        Submit generated documents
        :param docs: docs to submit
        :param removed: removed documents
        :return: None
        """
        raise NotImplemented

    def _get_minhashes_docs(self, doc_ids: list[int]) -> dict[int, tuple[int, list[int]]]:
        """
        Get minhashes for documents by submitting requests to an appropriate doc collectors
        :param doc_ids: doc ids
        :return: doc ids with hashes
        """
        raise NotImplemented

    def process_buckets(self, buckets: list[tuple[int, list[int]]]) -> None:
        """
        process buckets to generate documents
        :param buckets: buckets
        :return: none
        """
        t_start = time.time()
        docs = {}
        removed = set()
        for hash_bucket in buckets:
            b_hash = hash_bucket[0]
            bucket = hash_bucket[1]
            if len(bucket) == 1:
                # This hash has a single document
                if bucket[0] not in docs:
                    docs[bucket[0]] = (NO_SIMILARITY, b_hash)
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
                current_set.add(current_doc_id)
                for other_doc_id in bucket:
                    if other_doc_id in unvisited:
                        other_mh = hashes[other_doc_id][1]
                        if self.mn_min_hash.jaccard(current_mh, other_mh) >= self.threshold:
                            current_set.add(other_doc_id)
                            unvisited.discard(other_doc_id)
                set_list.append(current_set)
                index += 1
                if index % self.print_interval == 0:
                    self.logger.info(f"processing very long {bucket_len} bucket, {index} documents so far")
            if very_long:
                self.logger.info(f"done processing very long {bucket_len}")

            # process created sets
            for current_set in set_list:
                removed.update(current_set)
                if len(current_set) == 1:
                    # this is 1 element set
                    d = current_set.pop()
                    if d not in docs:
                        docs[d] = (NO_SIMILARITY, b_hash)
                    continue
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
                docs[remaining] = (cluster_id, b_hash)
                removed.discard(remaining)
            if very_long:
                self.logger.info(
                    f"Processed long ({bucket_len}) bucket in {round((time.time() - start) / 60.,3)} "
                    f"min; "
                    f"docs chains {len(set_list)}"
                )
        # Submit docs
        self._submit_generated_docs(docs, removed)
        # peg stats
        self.stats.add_stats({"generated doc_ids": len(docs), "bucket processing time": time.time() - t_start})
