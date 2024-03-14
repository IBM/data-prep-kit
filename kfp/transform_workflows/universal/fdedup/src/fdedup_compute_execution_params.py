from typing import Any


def fdedup_compute_execution_params(
        worker_options: str,        # ray worker configuration
        actor_options: str,         # actor's resource requirements
        params: dict[str, Any],     # fuzzy dedup specific parameters
        n_samples: int = 10,        # number of samples to use
        ) -> str:
    """
    Compute fuzzy dedup execution parameters
    :param worker_options: cluster parameters
    :param actor_options: actor request requirements
    :param n_samples: number of samples to use
    :param params: fuzzy dedup specific parameters containing the following keys:
        threshold - threshold for fuzzy computations
        num_permutations - number of permutation
        s3_input_folder - s3 input folder
        bucket_cpu - bucket actor cpu requirements
        minhash_cpu - minhash actor cpu requirements
        doc_cpu - doc actor cpu requirements
    :return: json string, containing
        workers - number of workers
        preprocessors - number of preprocessors
        docs - number of doc actors
        buckets - number of bucket actors
        min_hashes - number of minhash actors
    """
    import json
    import math
    import sys
    from kfp_support.workflow_support.utils import KFPUtils
    from data_processing.data_access import DataAccessS3
    from data_processing.utils import KB, GB
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
        threshold=float(params.get("threshold")),
        num_perm=int(params.get("num_permutations")),
        false_positive_weight=0.5,
        false_negative_weight=0.5,
    )
    print(f"Fuzzy parameters: num buckets {num_buckets}, bucket length {length_bucket}")
    # Get cluster parameters
    try:
        worker_options = worker_options.replace("'", '"')
        w_options = json.loads(worker_options)
    except Exception as e:
        print(f"Failed to load parameters {worker_options} with error {e}")
        sys.exit(1)
    cluster_cpu = w_options["replicas"] * w_options["cpu"] * 0.85
    cluster_memory = w_options["replicas"] * w_options["memory"] * 0.85
    print(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_memory}")
    # get actor requirements
    try:
        actor_options = actor_options.replace("'", '"')
        a_options = json.loads(actor_options)
    except Exception as e:
        print(f"Failed to load parameters {actor_options} with error {e}")
        sys.exit(1)
    actor_cpu = a_options["num_cpus"]
    print(f"actor required cpu {actor_cpu}")
    # get credentials
    s3_key, s3_secret, s3_endpoint = KFPUtils.credentials()
    s3_creds = {"access_key": s3_key,
                "secret_key": s3_secret,
                "url": s3_endpoint
                }
    s3_config = {"input_folder": KFPUtils.clean_path(params.get("s3_input_prefix")),
                 "output_folder": "",
                 }
    # because S3 is the only viable version for kfp-based implementation, we are here creating DataAccess S3 directly
    data_access = DataAccessS3(s3_credentials=s3_creds, s3_config=s3_config, d_sets=None, checkpoint=False, m_files=-1)
    # sample input data
    sampling = data_access.sample_input_data(n_samples=n_samples)
    avg_doc_size = sampling.get('average doc size KB')
    number_of_docs = sampling.get('estimated number of docs')
    avg_table_size = sampling.get("average table size MB") / KB
    # we are creating more buckets actors, so that we get better parallelization for bucket processing
    b_actors = math.ceil(num_buckets * number_of_docs * 64 * 1.1 / GB)
    d_actors = math.ceil(number_of_docs * 48 * 1.1 / GB)
    m_actors = math.ceil(number_of_docs * 128 * 1.1 / GB)
    # compute cpu requirements
    bucket_cpu = float(params.get("bucket_cpu"))
    min_hash_cpu = float(params.get("minhash_cpu"))
    doc_cpu = float(params.get("doc_cpu"))
    # Define number of preprocessors. We are assuming that preprocessors and workers are using the same amount
    # of CPUs
    n_preprocessors = int(
        (0.85 * cluster_cpu - b_actors * bucket_cpu - m_actors * min_hash_cpu - d_actors * doc_cpu) / actor_cpu
    )
    if n_preprocessors < 0:
        print(f"Not enough CPUs to run fuzzy de duping, computed number of workers is {n_preprocessors}")
        print(f"Required bucket actors {b_actors}, minhash actors {m_actors}, document actors {d_actors}")
        print("Try to increase the size of the cluster")
        sys.exit(1)
    # Ensure that we do not overwhelm S3
    if n_preprocessors > 1000:
        n_preprocessors = 1000
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
        print(
            f"Try to increase the size of the cluster or increase size of the cpu per worker (current {actor_cpu})"
        )
        sys.exit(1)

    print(
        f"Required cpu : "
        f"{b_actors * bucket_cpu + m_actors * min_hash_cpu + d_actors * doc_cpu + n_workers * actor_cpu}"
    )

    projected_execution = EXECUTION_OF_KB_DOC * avg_doc_size * number_of_docs / n_workers / 60
    print(f"Projected execution time {projected_execution} min")
    # return
    return json.dumps(
        {
            "workers": n_workers,
            "preprocessors": n_preprocessors,
            "docs": d_actors,
            "buckets": b_actors,
            "min_hashes": m_actors,
        }
    )
