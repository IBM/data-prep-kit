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

import numpy as np
from scipy.integrate import quad as integrate

from fdedup.utils import BucketsHash, DocCollector, DocsMinHash
from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils


NO_SIMILARITY = -1
LONG_BUCKET = 5000
REQUEST_LEN = 8192



class FdedupSupport:
    """
    Collection of support methods for fdedup implementation
    """
    @staticmethod
    def jaccard_distance(mh1: np.array, mh2: np.array) -> float:
        return len(np.intersect1d(mh1, mh2))/len(np.union1d(mh1, mh2))

    @staticmethod
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

    @staticmethod
    def process_buckets_locally(b_hash: int, bucket: list[int], minhashes: dict[int, tuple[int, np.array]],
                                threshold: float) -> tuple[dict[int, tuple[int, int]], set[int]]:
        """
        process buckets with multiple documents
        :param b_hash: bucket hash
        :param bucket: list of documents
        :param minhashes: minhashes
        :param threshold: distance threshold
        :return: tuple of generated documents and removed
        """
        docs = {}
        removed = set()
        set_list = []
        unvisited = set(bucket)
        # combine similar documents
        while len(unvisited) > 0:
            current_doc_id = unvisited.pop()
            current_mh = minhashes[current_doc_id][1]
            current_set = set()
            current_set.add(current_doc_id)
            for other_doc_id in bucket:
                if other_doc_id in unvisited:
                    other_mh = minhashes[other_doc_id][1]
                    if FdedupSupport.jaccard_distance(current_mh, other_mh) >= threshold:
                        current_set.add(other_doc_id)
                        unvisited.discard(other_doc_id)
            set_list.append(current_set)
        # process created sets
        for current_set in set_list:
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
                    min_len = minhashes[doc_id][0]
                    max_len = min_len
                    continue
                c_len = minhashes[doc_id][0]
                if c_len > max_len:
                    max_len = c_len
                    remaining = doc_id
                    continue
                if c_len <= min_len:
                    min_len = c_len
                    cluster_id = doc_id
            current_set.discard(remaining)
            docs, removed = FdedupSupport.merge_doc_ids(current_ids=docs, current_removed=removed,
                                                        new_ids={remaining: (cluster_id, b_hash)}, new_removed=current_set)
            return docs, removed

    @staticmethod
    def merge_doc_ids(current_ids: dict[int, tuple[int, int]], current_removed: set[int],
                      new_ids: dict[int, tuple[int, int]], new_removed: set[int]) \
            -> tuple[dict[int, tuple[int, int]], set[int]]:
        """
        Merge document ids
        :param current_ids: current ids
        :param current_removed: current removed
        :param new_ids: new ids
        :param new_removed: new removed
        :return: resulting ids and removed
        """
        # process documents to remove
        for did in new_removed:
            current_ids.pop(did, None)
        current_removed.update(new_removed)
        # process documents to keep
        for key, val in new_ids.items():
            if key in current_removed:
                continue
            if key in current_ids and val[0] == NO_SIMILARITY:
                # Do not update existing docs with NO_SIMILARITY
                continue
            else:
                current_ids[key] = val
        return current_ids, current_removed

    @staticmethod
    def remove_docs_buckets(buckets: dict[int, list[int]], removed: set[int]) -> dict[int, list[int]]:
        """
        Remove removed documents from buckets
        :param buckets: buckets dictionary
        :param removed: removed documents set
        :return: buckets without removed docs
        """
        for key in buckets.keys():
            value = [e for e in buckets[key] if e not in removed]
            if len(value) == 0:
                buckets.pop(key, None)
            else:
                buckets[key] = value
        return buckets

    @staticmethod
    def createMinhash(data_access_factory: DataAccessFactoryBase, directory: str) -> DocsMinHash:
        """
        Create minhash class
        :param data_access_factory: data access factory
        :param directory: snapshot directory
        :return: Minhash class
        """
        if directory is None or len(directory) == 0:
            mh_path = None
        else:
            # restarting from snapshot
            mh_path = directory
        return DocsMinHash({"id": 0, "data_access": data_access_factory, "snapshot": mh_path})

    @staticmethod
    def createMinhashCurrent(data_access_factory: DataAccessFactoryBase, directory: str) -> DocsMinHash:
        """
        Create minhash class
        :param data_access_factory: data access factory
        :param directory: snapshot directory
        :return: Minhash class
        """
        data_access = data_access_factory.create_data_access()
        if directory is None or len(directory) == 0:
            mh_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}minhash/minhash_collector_0"
        else:
            mh_path = f"{directory}/minhash_collector_0"
        return DocsMinHash({"id": 0, "data_access": data_access_factory, "snapshot": mh_path})

    @staticmethod
    def createBucket(data_access_factory: DataAccessFactoryBase, directory: str) -> BucketsHash:
        """
         Create bucket class
         :param data_access_factory: data access factory
         :param directory: snapshot directory
         :return: Bucket class
         """
        if directory is None or len(directory) == 0:
            b_path = None
        else:
            # restarting from snapshot
            b_path = directory
        return BucketsHash({"id": 0, "data_access": data_access_factory, "snapshot": b_path})

    @staticmethod
    def createDocID(data_access_factory: DataAccessFactoryBase, directory: str) -> DocCollector:
        """
         Create doc class
         :param data_access_factory: data access factory
         :param directory: snapshot directory
         :return: doc class
         """
        if directory is None or len(directory) == 0:
            d_path = None
        else:
            # restarting from snapshot
            d_path = directory
        return DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": d_path})

    @staticmethod
    def createDocIDCurrent(data_access_factory: DataAccessFactoryBase, directory: str) -> DocCollector:
        """
        Create doc class
        :param data_access_factory: data access factory
        :param directory: snapshot directory
        :return: doc class
           """
        data_access = data_access_factory.create_data_access()
        if directory is None or len(directory) == 0:
            doc_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}docs/doc_collector_0"
        else:
            doc_path = f"{directory}/doc_collector_0"
        return DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": doc_path})
