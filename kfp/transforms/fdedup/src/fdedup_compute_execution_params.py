from typing import Any

def fdedup_compute_execution_params(
        cluster_cpu: float,         # number cpus for cluster
        cluster_memory: float,      # memory for cluster (GB)
        # fuzzy dedup specific parameters
        params: dict[str, Any],
        # Worker parameters
        worker_cpu: float,          # cpu requirement per actor
        n_samples: int = 10,        # number of samples to use
) -> str:
    """
    Compute fuzzy dedup execution parameters
    :param cluster_cpu: overall cluster cpu
    :param cluster_memory: overall cluster memory
    :param worker_cpu: worker cpu requirement
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
