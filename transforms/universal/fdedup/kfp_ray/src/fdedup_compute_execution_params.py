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


def fdedup_compute_execution_params(
    worker_options: dict,  # ray worker configuration
    actor_options: dict,  # actor's resource requirements
    data_s3_config: str,  # s3 configuration
    data_max_files: int,  # max files to process
    data_num_samples: int,  # num samples to process
    runtime_pipeline_id: str,  # pipeline id
    runtime_job_id: str,  # job id
    runtime_code_location: dict,  # code location
    doc_column: str,  # document column name
    id_column: str,  # integer document id column name
    cluster_column: str,  # cluster column name
    bucket_cpu: float,  # number of CPUs per bucket hash
    doc_cpu: float,  # number of CPUs per doc hash
    mhash_cpu: float,  # number of CPUs per minhash hash
    num_permutations: int,  # number of permutations
    threshold: float,  # threshold,
    shingles_size: int,  # number of words in shingle
    delimiters: str,  # delimiter for splitting document
    random_delay_limit: int,  # delay between reads to reduce S3 load.
    # A random number between 0 and random_delay_limit is used
    snapshot_delay: int,  # delay between restoring individual actors
    use_doc_snapshot: bool,  # flag to skip documents building and start from existing snapshots
    use_bucket_snapshot: bool,  # flag to skip buckets building and start from existing snapshots
    n_samples: int,  # number of samples to use
) -> dict:  # NamedTuple(
    # "Output", [("workers", int), ("preprocessors", int), ("docs", int), ("buckets", int), ("min_hashes", int)]

    """
    Compute fuzzy dedup execution parameters
    :param worker_options: cluster parameters
    :param actor_options: actor request requirements
    :param data_s3_config: s3 configuration
    :param data_max_files: max files to process
    :param data_num_samples: num samples to process
    :param runtime_pipeline_id: pipeline id
    :param runtime_job_id: job id
    :param runtime_code_location: code location
    :param doc_column: document column name
    :param id_column: integer document id column name
    :param cluster_column: cluster column name
    :param bucket_cpu: number of CPUs per bucket hash
    :param doc_cpu: number of CPUs per doc hash
    :param mhash_cpu: number of CPUs per minhash hash
    :param num_permutations: number of permutations
    :param threshold: threshold,
    :param shingles_size: number of words in shingle
    :param delimiters: delimiter for splitting document
    :param random_delay_limit: # delay between reads to reduce S3 load. A random number between 0 and random_delay_limit is used
    :param snapshot_delay: delay between restoring individual actors
    :param use_doc_snapshot: flag to skip documents building and start from existing snapshots
    :param use_bucket_snapshot: flag to skip buckets building and start from existing snapshots
    :param n_samples: number of samples to use
    :return: a dictionary with a Ray Job execution parameters
    """
    import math
    import sys

    from data_processing.data_access import DataAccessS3
    from data_processing.utils import GB, KB
    from runtime_utils import KFPUtils
    from scipy.integrate import quad as integrate

    EXECUTION_OF_KB_DOC = 0.003

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
        threshold=threshold,
        num_perm=num_permutations,
        false_positive_weight=0.5,
        false_negative_weight=0.5,
    )
    print(f"Fuzzy parameters: num buckets {num_buckets}, bucket length {length_bucket}")
    # Get cluster parameters
    cluster_cpu = worker_options["replicas"] * worker_options["cpu"]
    cluster_memory = worker_options["replicas"] * worker_options["memory"]
    print(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_memory}")
    cluster_cpu -= 1
    cluster_memory *= 0.85
    # get actor requirements
    actor_cpu = actor_options["num_cpus"]
    print(f"actor required cpu {actor_cpu}")
    # get credentials
    s3_key, s3_secret, s3_endpoint = KFPUtils.credentials()
    s3_creds = {"access_key": s3_key, "secret_key": s3_secret, "url": s3_endpoint}
    s3_config = KFPUtils.load_from_json(data_s3_config.replace("'", '"'))
    if type(s3_config) is list:
        # S3 config is list. take the first element
        s3_config = s3_config[0]
    # because S3 is the only viable version for kfp-based implementation, we are here creating DataAccess S3 directly
    data_access = DataAccessS3(s3_credentials=s3_creds, s3_config=s3_config, d_sets=None, checkpoint=False, m_files=-1)
    # sample input data
    sampling, _ = data_access.sample_input_data(n_samples=n_samples)
    avg_doc_size = sampling.get("average doc size KB")
    number_of_docs = sampling.get("estimated number of docs")
    avg_table_size = sampling.get("average table size MB") / KB
    if number_of_docs == 0:
        print(f"Estimated number of documents and documents size is zero. Please verify the input path.")
        sys.exit(1)
    # we are creating more buckets actors, so that we get better parallelization for bucket processing
    b_actors = math.ceil(num_buckets * number_of_docs * 64 * 1.1 / GB)
    d_actors = math.ceil(number_of_docs * 48 * 1.1 / GB)
    m_actors = math.ceil(number_of_docs * 128 * 1.1 / GB)
    # compute cpu requirements
    # Define number of preprocessors. We are assuming that preprocessors and workers are using the same amount
    # of CPUs
    n_preprocessors = int(
        (0.85 * cluster_cpu - b_actors * bucket_cpu - m_actors * mhash_cpu - d_actors * doc_cpu) / actor_cpu
    )
    if n_preprocessors <= 0:
        print(f"Not enough CPUs to run fuzzy de duping, computed number of workers is {n_preprocessors}")
        print(f"Required bucket actors {b_actors}, minhash actors {m_actors}, document actors {d_actors}")
        print("Try to increase the size of the cluster")
        sys.exit(1)
    # compute the amount of workers
    n_workers = int((0.85 * cluster_cpu - d_actors * doc_cpu) / actor_cpu)
    # Ensure that we do not overwhelm S3
    if n_workers > 2000:
        n_workers = 2000
    print(
        f"Number of preprocessors: {n_preprocessors}, Number of workers: {n_workers}, bucket actors {b_actors}, "
        f"minhash actors {m_actors}, document actors {d_actors}"
    )

    # Make sure that we have enough memory
    r_mem = avg_table_size * 4 * n_preprocessors + 2 * (b_actors + m_actors + d_actors)
    print(f"Required execution memory {r_mem} GB")
    if r_mem > cluster_memory:
        print(f"Not enough memory to run de duping, required {r_mem}, available {cluster_memory}")
        print(f"Try to increase the size of the cluster or increase size of the cpu per worker (current {actor_cpu})")
        sys.exit(1)

    print(
        f"Required cpu : "
        f"{b_actors * bucket_cpu + m_actors * mhash_cpu + d_actors * doc_cpu + n_workers * actor_cpu}"
    )

    projected_execution = EXECUTION_OF_KB_DOC * avg_doc_size * number_of_docs / n_workers / 60
    print(f"Projected execution time {projected_execution} min")
    return {
        "data_s3_config": data_s3_config,
        "data_max_files": data_max_files,
        "data_num_samples": data_num_samples,
        "runtime_num_workers": n_workers,
        "runtime_worker_options": str(actor_options),
        "runtime_pipeline_id": runtime_pipeline_id,
        "runtime_job_id": runtime_job_id,
        "runtime_code_location": str(runtime_code_location),
        "fdedup_doc_column": doc_column,
        "fdedup_id_column": id_column,
        "fdedup_cluster_column": cluster_column,
        "fdedup_bucket_cpu": bucket_cpu,
        "fdedup_doc_cpu": doc_cpu,
        "fdedup_mhash_cpu": mhash_cpu,
        "fdedup_num_doc_actors": d_actors,
        "fdedup_num_bucket_actors": b_actors,
        "fdedup_num_minhash_actors": m_actors,
        "fdedup_num_preprocessors": n_preprocessors,
        "fdedup_num_permutations": num_permutations,
        "fdedup_threshold": threshold,
        "fdedup_shingles_size": shingles_size,
        "fdedup_delimiters": delimiters,
        "fdedup_random_delay_limit": random_delay_limit,
        "fdedup_snapshot_delay": snapshot_delay,
        "fdedup_use_doc_snapshot": use_doc_snapshot,
        "fdedup_use_bucket_snapshot": use_bucket_snapshot,
    }
