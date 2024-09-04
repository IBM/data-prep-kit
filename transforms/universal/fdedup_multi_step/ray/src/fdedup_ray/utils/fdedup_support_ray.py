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
import logging
import numpy as np
import ray
from ray.actor import ActorHandle

from data_processing.data_access import DataAccessFactoryBase, SnapshotUtils
from data_processing.utils import UnrecoverableException
from data_processing_ray.runtime.ray import RayUtils
from fdedup.utils import BucketsHash, DocCollector, DocsMinHash


class FdedupSupportRay:
    """
    Collection of support methods for Ray fdedup implementation
    """
    @staticmethod
    def update_buckets(buckets: dict[int, list[int]], bucket_actors: list[ActorHandle],
                       logger: logging.Logger) -> None:
        """
        Update buckets
        :param buckets: dictionary of buckets
        :param bucket_actors: list of bucket actors
        :param logger: logger
        :return: None
        """
        remote_replies = []
        request = [[] for _ in range(len(bucket_actors))]
        for key, value in buckets.items():
            request[key % len(bucket_actors)].append((key, value))
        # Submit requests to appropriate bucket collectors
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(bucket_actors[i].add_buckets.remote(req))
            i += 1
        RayUtils.wait_for_execution_completion(logger=logger, replies=remote_replies)

    @staticmethod
    def update_doc_ids(docs: dict[int, tuple[int, int]], removed: set[int], doc_actors: list[ActorHandle],
                       logger: logging.Logger) -> None:
        """
        Update doc_ids
        :param docs: dictionary of docs
        :param removed: dictionary of removed
        :param doc_actors: list of doc actors
        :param logger: logger
        :return: None
        """
        remote_replies = []
        request = [([], []) for _ in range(len(doc_actors))]
        for key, value in docs.items():
            req_tuple = request[key % len(doc_actors)]
            req_tuple[0].append((key, value))
        for did in removed:
            req_tuple = request[did % len(doc_actors)]
            req_tuple[1].append(did)
        # Submit requests and wait for replies
        i = 0
        for req in request:
            if len(req[0]) > 0 or len(req[1]) > 0:  # Only submit if the request has data
                remote_replies.append(doc_actors[i].add_documents.remote(req))
            i += 1
        RayUtils.wait_for_execution_completion(logger=logger, replies=remote_replies)

    @staticmethod
    def update_minhashes(minhashes: list[tuple[int, int, np.array]], minhash_actors: list[ActorHandle],
                         logger: logging.Logger) -> None:
        """
        Update minhashes
        :param minhashes: list of minhashes
        :param minhash_actors: list of minhash actors
        :param logger: logger
        :return: None
        """
        remote_replies = []
        request = [[] for _ in range(len(minhash_actors))]
        for minh in minhashes:
            request[minh[0] % len(minhash_actors)].append(minh)
        # Submit requests to appropriate minhash collectors
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(minhash_actors[i].add_minhashes.remote(req))
            i += 1
        RayUtils.wait_for_execution_completion(logger=logger, replies=remote_replies)

    @staticmethod
    def create_buckets(data_access_factory: DataAccessFactoryBase, n_actors: int, actor_cpu: float, directory: str,
                       statistics: ActorHandle, logger: logging.Logger) -> list[ActorHandle]:
        """
        create buckets
        :param data_access_factory: data access factory
        :param n_actors: number of actors
        :param actor_cpu: actor cpu
        :param directory: snapshot directory
        :param statistics: statistics
        :param logger: logger
        :return: list of actors
        """
        data_access = data_access_factory.create_data_access()
        bucket_actors = [None] * n_actors
        for i in range(n_actors):
            bucket_actors[i] = ray.remote(BucketsHash).options(**{"num_cpus": actor_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None}
            )
        if directory is not None and len(directory) > 0:
            # get snapshot files.
            files, retries = data_access.get_folder_files(path=directory)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            for file in files.keys():
                # load the file
                try:
                    b_hashes, _ = data_access.get_file(file)
                    buckets = pickle.loads(b_hashes)
                except Exception as e:
                    logger.warning(f"Failed to load minhashes from file {file} with exception {e}")
                    raise UnrecoverableException("failed to load minhashes")
                # Add snapshotted minhashes
                FdedupSupportRay.update_buckets(buckets=buckets, bucket_actors=bucket_actors, logger=logger)
        logger.info(f"Created {len(bucket_actors)} bucket collectors")
        return bucket_actors

    @staticmethod
    def create_doc_ids(data_access_factory: DataAccessFactoryBase, n_actors: int, actor_cpu: float, directory: str,
                       statistics: ActorHandle, logger: logging.Logger) -> list[ActorHandle]:
        """
        create doc ids
        :param data_access_factory: data access factory
        :param n_actors: number of actors
        :param actor_cpu: actor cpu
        :param directory: snapshot directory
        :param statistics: statistics
        :param logger: logger
        :return: list of actors
        """
        data_access = data_access_factory.create_data_access()
        doc_ids = [None] * n_actors
        for i in range(n_actors):
            doc_ids[i] = ray.remote(DocCollector).options(**{"num_cpus": actor_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None}
            )
        if directory is not None and len(directory) > 0:
            # get snapshot files.
            files, retries = data_access.get_folder_files(path=directory)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            for file in files.keys():
                # load the file
                try:
                    d_hashes, _ = data_access.get_file(file)
                    docs, removed = pickle.loads(d_hashes)
                except Exception as e:
                    logger.warning(f"Failed to load docs from file {file} with exception {e}")
                    raise UnrecoverableException("failed to load logs")
                # Add snapshotted docs
                FdedupSupportRay.update_doc_ids(docs=docs, removed=removed, doc_actors=doc_ids, logger=logger)
        logger.info(f"Created {len(doc_ids)} doc collectors")
        return doc_ids

    @staticmethod
    def create_minhashes(data_access_factory: DataAccessFactoryBase, n_actors: int, actor_cpu: float, directory: str,
                         statistics: ActorHandle, logger: logging.Logger) -> list[ActorHandle]:
        """
        create minhashes
        :param data_access_factory: data access factory
        :param n_actors: number of actors
        :param actor_cpu: actor cpu
        :param directory: snapshot directory
        :param statistics: statistics
        :param logger: logger
        :return: list of actors
        """
        data_access = data_access_factory.create_data_access()
        minhash_actors = [None] * n_actors
        for i in range(n_actors):
            minhash_actors[i] = ray.remote(DocsMinHash).options(**{"num_cpus": actor_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": None}
            )
        if directory is not None and len(directory) > 0:
            # get snapshot files.
            files, retries = data_access.get_folder_files(path=directory)
            if retries > 0:
                statistics.add_stats.remote({"data access retries": retries})
            for file in files.keys():
                # load the file
                try:
                    m_hashes, _ = data_access.get_file(file)
                    minhashes = list(pickle.loads(m_hashes).getitems())
                except Exception as e:
                    logger.warning(f"Failed to load minhashes from file {file} with exception {e}")
                    raise UnrecoverableException("failed to load minhashes")
                # Add snapshotted minhashes
                FdedupSupportRay.update_minhashes(minhashes=minhashes, minhash_actors=minhash_actors, logger=logger)
        logger.info(f"Created {len(minhash_actors)} minhash collectors")
        return minhash_actors

    @staticmethod
    def create_doc_id_current(data_access_factory: DataAccessFactoryBase, n_actors: int, actor_cpu: float,
                              directory: str, statistics: ActorHandle, logger=logging.Logger) -> list[ActorHandle]:
        """
        create doc ids
        :param data_access_factory: data access factory
        :param n_actors: number of actors
        :param actor_cpu: actor cpu
        :param directory: directory
        :param statistics: statistics
        :param logger: logger
        :return: actors
        """
        data_access = data_access_factory.create_data_access()
        if directory is None or len(directory) == 0:
            d_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}docs/"
        else:
            d_path = directory
        doc_collectors = [None] * n_actors
        files, retries = data_access.get_folder_files(path=d_path)
        if retries > 0:
            statistics.add_stats.remote({"data access retries": retries})
        for file in files.keys():
            i = int(file[file.rfind("_") + 1:])
            doc_collectors[i] = ray.remote(DocCollector).options(**{"num_cpus": actor_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": file}
            )
        logger.info(f"Created {len(doc_collectors)} doc collectors")
        return doc_collectors

    @staticmethod
    def create_minhashes_current(data_access_factory: DataAccessFactoryBase, n_actors: int, actor_cpu: float,
                                 directory: str, statistics: ActorHandle, logger=logging.Logger) -> list[ActorHandle]:
        """
        create minhashes
        :param data_access_factory: data access factory
        :param n_actors: number of actors
        :param actor_cpu: actor cpu
        :param directory: directory
        :param statistics: statistics
        :param logger: logger
        :return: actors
        """
        data_access = data_access_factory.create_data_access()
        if directory is None or len(directory) == 0:
            mh_path = f"{SnapshotUtils.get_snapshot_folder(data_access)}minhash/"
        else:
            mh_path = directory
        minhashes = [None] * n_actors
        files, retries = data_access.get_folder_files(path=mh_path)
        if retries > 0:
            statistics.add_stats.remote({"data access retries": retries})
        for file in files.keys():
            i = int(file[file.rfind("_") + 1:])
            minhashes[i] = ray.remote(DocsMinHash).options(**{"num_cpus": actor_cpu}).remote(
                {"id": i, "data_access": data_access_factory, "snapshot": file}
            )
        logger.info(f"Created {len(minhashes)} minhash collectors")
        return minhashes

    @staticmethod
    def get_bucket_stats(actors: list[ActorHandle]) -> tuple[int, float]:
        """
        Get bucket hash usage statistics
        :param actors: list of actors
        :return: usage
        """
        sum_buckets = 0
        sum_buckets_mem = 0
        replies = [collector.get_size.remote() for collector in actors]
        while replies:
            ready, not_ready = ray.wait(replies)
            b_amount, b_memory = ray.get(ready)[0]
            sum_buckets += b_amount
            sum_buckets_mem += b_memory
            replies = not_ready
        return sum_buckets, sum_buckets_mem

    @staticmethod
    def get_doc_stats(actors: list[ActorHandle]) -> tuple[int, float, int, float]:
        """
        Get docs hash usage statistics
        :param actors: list of actors
        :return: usage
        """
        sum_docs = 0
        sum_docs_memory = 0
        sum_removed = 0
        sum_removed_memory = 0
        replies = [collector.get_size.remote() for collector in actors]
        while replies:
            ready, not_ready = ray.wait(replies)
            d_size, d_memory, r_size, r_memory = ray.get(ready)[0]
            sum_docs += d_size
            sum_docs_memory += d_memory
            sum_removed += r_size
            sum_removed_memory += r_memory
            replies = not_ready
        return sum_docs, sum_docs_memory, sum_removed, sum_removed_memory

    @staticmethod
    def get_minhash_stats(actors: list[ActorHandle]) -> tuple[int, float]:
        """
        Get minhash hash usage statistics
        :param actors: list of actors
        :return: usage
        """
        sum_mh = 0
        sum_mh_mem = 0
        replies = [collector.get_size.remote() for collector in actors]
        while replies:
            ready, not_ready = ray.wait(replies)
            m_amount, m_memory = ray.get(ready)[0]
            sum_mh += m_amount
            sum_mh_mem += m_memory
            replies = not_ready
        return sum_mh, sum_mh_mem

    @staticmethod
    def snapshot_caches(bucket_actors: list[ActorHandle], doc_actors: list[ActorHandle],
                        minhash_actors: list[ActorHandle], logger: logging.Logger) -> None:
        """
        Snapshot
        :param bucket_actors: bucket actors
        :param doc_actors: doc actors
        :param minhash_actors: minhash actors
        :param logger: logger
        :return: None
        """
        replies_m = [collector.snapshot.remote() for collector in minhash_actors]
        replies_d = [collector.snapshot.remote() for collector in doc_actors]
        replies_b = [collector.snapshot.remote() for collector in bucket_actors]
        RayUtils.wait_for_execution_completion(logger=logger, replies=list(replies_m + replies_b + replies_d))

    @staticmethod
    def get_unique_ids(actors: list[ActorHandle], ids: list[int]) -> dict[int, int]:
        """
        Get unique IDs
        :param actors: list of actors
        :param ids: ids
        :return: unique ids
        """
        request = [[] for _ in range(len(actors))]
        for value in ids:
            doc_id = value
            request[doc_id % len(actors)].append(doc_id)
        remote_replies = []
        i = 0
        for req in request:
            if len(req) > 0:  # Only submit if the length is greater then 0
                remote_replies.append(actors[i].filter.remote(req))
            i += 1
        # Process replies
        unique = {}
        while remote_replies:
            # Wait for replies
            ready, not_ready = ray.wait(remote_replies)
            reply = ray.get(ready)[0]
            unique.update(reply)
            remote_replies = not_ready
        return unique
