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


def fdedup_preprocessor_compute_execution_params(
    ray_worker_options: dict,  # ray worker configuration
    runtime_actor_options: dict,  # actor's resource requirements
    data_s3_config: str,  # s3 configuration
    data_max_files: int,  # max files to process
    data_num_samples: int,  # num samples to process
    runtime_pipeline_id: str,  # pipeline id
    runtime_job_id: str,  # job id
    runtime_code_location: dict,  # code location
    fdedup_preprocessor_doc_column: str,  # document column name
    fdedup_preprocessor_doc_id_column: str,  # integer document id column name
    fdedup_preprocessor_num_permutations: int,  # number of permutations
    fdedup_preprocessor_threshold: float,  # threshold,
    fdedup_preprocessor_shingles_size: int,  # number of words in shingle
    fdedup_preprocessor_delimiters: str,  # delimiter for splitting document
    fdedup_preprocessor_bucket_cpu: float,  # number of CPUs per bucket hash
    fdedup_preprocessor_doc_cpu: float,  # number of CPUs per doc hash
    fdedup_preprocessor_mhash_cpu: float,  # number of CPUs per minhash hash
    fdedup_preprocessor_minhash_snapshot_directory: str,  # minhash_snapshot_directory
    fdedup_preprocessor_buckets_snapshot_directory: str,  # buckets_snapshot_directory
    fdedup_preprocessor_doc_id_snapshot_directory: str,  # doc id snapshot directory
    fdedup_preprocessor_n_samples: int,  # number of samples to use for data evaluation
    fdedup_preprocessor_n_docs: int,  # number of source documents
) -> dict[str, Any]:

    """
    Compute fuzzy dedup preprocessor execution parameters
    :param ray_worker_options: cluster parameters
    :param runtime_actor_options: actor request requirements
    :param data_s3_config: s3 configuration
    :param data_max_files: max files to process
    :param data_num_samples: num samples to process
    :param runtime_pipeline_id: pipeline id
    :param runtime_job_id: job id
    :param runtime_code_location: code location
    :param fdedup_preprocessor_doc_column: document column name
    :param fdedup_preprocessor_doc_id_column: integer document id column name
    :param fdedup_preprocessor_bucket_cpu: number of CPUs per bucket hash
    :param fdedup_preprocessor_doc_cpu: number of CPUs per doc hash
    :param fdedup_preprocessor_mhash_cpu: number of CPUs per minhash hash
    :param fdedup_preprocessor_num_permutations: number of permutations
    :param fdedup_preprocessor_threshold: threshold,
    :param fdedup_preprocessor_shingles_size: number of words in shingle
    :param fdedup_preprocessor_delimiters: delimiter for splitting document
    :param fdedup_preprocessor_minhash_snapshot_directory: minhash snapshot directory
    :param fdedup_preprocessor_doc_id_snapshot_directory: doc id snapshot directory
    :param fdedup_preprocessor_buckets_snapshot_directory: buckets snapshot directory
    :param fdedup_preprocessor_n_samples: number of samples to use for data evaluation
    :param fdedup_preprocessor_n_docs: number of source documents (if negative use sampling)
    :return: a dictionary with a Ray Job execution parameters
    """
    import math
    import sys

    from data_processing.data_access import DataAccessFactory
    from data_processing.utils import GB, KB
    from runtime_utils import KFPUtils
    from scipy.integrate import quad as integrate

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

    # fuzzy parameters
    num_buckets, length_bucket = fuzzy_optimal_param(
        threshold=fdedup_preprocessor_threshold,
        num_perm=fdedup_preprocessor_num_permutations,
        false_positive_weight=0.5,
        false_negative_weight=0.5,
    )
    print(f"Fuzzy parameters: num buckets {num_buckets}, bucket length {length_bucket}")
    # Get cluster parameters
    cluster_cpu = ray_worker_options["replicas"] * ray_worker_options["cpu"]
    cluster_memory = ray_worker_options["replicas"] * ray_worker_options["memory"]
    print(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_memory}")
    cluster_cpu *= 0.85
    cluster_memory *= 0.85
    # get actor requirements
    worker_cpu = runtime_actor_options["num_cpus"]
    print(f"worker required cpu {worker_cpu}")
    if fdedup_preprocessor_n_docs > 0:
        # number of docs specified - use it
        number_of_docs = fdedup_preprocessor_n_docs
        print(f"Specified number of docs {number_of_docs}")
    else:
        # get credentials
        s3_key, s3_secret, s3_endpoint = KFPUtils.credentials()
        s3_creds = {"access_key": s3_key, "secret_key": s3_secret, "url": s3_endpoint}
        # create data access
        s3_config = KFPUtils.load_from_json(data_s3_config.replace("'", '"'))
        if type(s3_config) is list:
            # S3 config is list. take the first element
            s3_config = s3_config[0]
        data_access_factory = DataAccessFactory()
        data_access_factory.apply_input_params({"data_s3_config": s3_config, "data_s3_cred": s3_creds})
        data_access = data_access_factory.create_data_access()
        # sample input data
        sampling, _ = data_access.sample_input_data(n_samples=fdedup_preprocessor_n_samples)
        avg_doc_size = sampling.get("average doc size KB")
        number_of_docs = sampling.get("estimated number of docs")
        if number_of_docs == 0:
            print(f"Estimated number of documents and documents size is zero. Please verify the input path.")
            sys.exit(1)
        print(f"Based on sampling, avg doc size: {avg_doc_size}, number of docs {number_of_docs}")

    # we are creating more buckets actors, so that we get better parallelization for bucket processing
    b_actors = math.ceil(num_buckets * number_of_docs * 64 * 1.1 / GB)
    d_actors = math.ceil(number_of_docs * 48 * 1.1 / GB)
    m_actors = math.ceil(number_of_docs * 128 * 1.1 / GB)
    # Define number of workers.
    n_workers = int(
        (0.85 * cluster_cpu - b_actors * fdedup_preprocessor_bucket_cpu - m_actors * fdedup_preprocessor_mhash_cpu
         - d_actors * fdedup_preprocessor_doc_cpu) / worker_cpu
    )
    if n_workers < 0:
        print(f"Not enough CPUs to run fuzzy dedup preprocessor, computed number of workers is {n_workers}")
        print(f"Required bucket actors {b_actors}, minhash actors {m_actors}, document actors {d_actors}")
        print("Try to increase the size of the cluster")
        sys.exit(1)
    # cap the number of workers to ensure that we do not overwhelm COS
    if n_workers > 1000:
        n_workers = 1000
    print(
        f"Number of workers: {n_workers}, bucket actors {b_actors}, "
        f"minhash actors {m_actors}, document actors {d_actors}"
    )

    # Make sure that we have enough memory. We assume that each actor uses 3 GB memory
    r_mem = 3 * (n_workers + b_actors + m_actors + d_actors)
    if r_mem > cluster_memory:
        print(f"Not enough memory to run de duping, required {r_mem}, available {cluster_memory}")
        print(f"Try to increase the size of the cluster or increase size of the cpu per worker (current {worker_cpu})")
        sys.exit(1)
    required_cpu = (b_actors * fdedup_preprocessor_bucket_cpu + m_actors * fdedup_preprocessor_mhash_cpu
                    + d_actors * fdedup_preprocessor_doc_cpu + n_workers * worker_cpu)
    print(f"Required execution cpu : {required_cpu}, Required execution memory {r_mem} GB")
    # return results
    return {
        "data_s3_config": data_s3_config,
        "data_max_files": data_max_files,
        "data_num_samples": data_num_samples,
        "runtime_num_workers": n_workers,
        "runtime_worker_options": str(runtime_actor_options),
        "runtime_pipeline_id": runtime_pipeline_id,
        "runtime_job_id": runtime_job_id,
        "runtime_code_location": str(runtime_code_location),
        "fdedup_preprocessor_doc_column": fdedup_preprocessor_doc_column,
        "fdedup_preprocessor_doc_id_column": fdedup_preprocessor_doc_id_column,
        "fdedup_preprocessor_bucket_cpu": fdedup_preprocessor_bucket_cpu,
        "fdedup_preprocessor_doc_cpu": fdedup_preprocessor_doc_cpu,
        "fdedup_preprocessor_mhash_cpu": fdedup_preprocessor_mhash_cpu,
        "fdedup_preprocessor__num_doc_id": d_actors,
        "fdedup_preprocessor_num_buckets": b_actors,
        "fdedup_preprocessor_num_minhashs": m_actors,
        "fdedup_num_permutations": fdedup_preprocessor_num_permutations,
        "fdedup_threshold": fdedup_preprocessor_threshold,
        "fdedup_shingles_size": fdedup_preprocessor_shingles_size,
        "fdedup_delimiters": fdedup_preprocessor_delimiters,
        "fdedup_preprocessor_minhash_snapshot_directory": fdedup_preprocessor_minhash_snapshot_directory,
        "fdedup_preprocessor_buckets_snapshot_directory": fdedup_preprocessor_buckets_snapshot_directory,
        "fdedup_preprocessor_doc_id_snapshot_directory": fdedup_preprocessor_doc_id_snapshot_directory,
    }
