from scipy.integrate import quad as integrate
from data_processing.utils import TransformUtils, RANDOM_SEED, GB
import numpy as np
from typing import Any, Iterator
import ray

NO_SIMILARITY = -1
REQUEST_LEN = 4096


def find(s: str, ch: str) -> list[int]:
    """
    Get indexes of all locations of character in string
    :param s: string
    :param ch: character
    :return: list of locations
    """
    return [i for i, ltr in enumerate(s) if ltr == ch]


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
        self.ids = {}
        self.removed = set()

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
            r = self.ids.get(doc_id)
            if r is not None:
                result[doc_id] = r
        return result

    def get_size(self) -> tuple[int, float, int, float]:
        """
        get sizes
        :return: number of ids, its memory utilization, number of removed, its memory utilization
        """
        return (len(self.ids), TransformUtils.deep_get_size(self.ids) / GB,
                len(self.removed), TransformUtils.deep_get_size(self.removed) / GB)


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
        self.docs = {}

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
    def __init__(self):
        """
        Initialization
        """
        self.buckets = {}
        self.submitter = None
        self.n_buckets = 0
        self.bucket_memory = 0

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

    def add_processing_submitter(self, submitter: ray.ObjectRef) -> None:
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
            if type(bucket) == list and len(bucket) > 10000:
                # Its long
                long_buckets.append(bucket)
            else:
                short_buckets.append(bucket)

        # process long buckets first - we are submitting them one at a time
        for bucket in long_buckets:
            ray.get(self.submitter.submit_for_processing.remote([bucket]))

        # And now the rest of buckets
        for i in range(0, len(short_buckets), REQUEST_LEN):
            ray.get(self.submitter.submit_for_processing.remote(short_buckets[i: i + REQUEST_LEN]))

    def get_size(self) -> tuple[int, float]:
        """
        Get buckets resource utilization
        :return: number of buckets and memory utilization
        """
        return self.n_buckets, self.bucket_memory
