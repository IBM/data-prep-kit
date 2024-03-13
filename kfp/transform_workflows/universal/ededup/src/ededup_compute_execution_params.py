from typing import Any


def ededup_compute_execution_params(
        cluster_cpu: float,         # number cpus for cluster
        cluster_memory: float,      # memory for cluster (GB)
        # exact dedup specific parameters
        params: dict[str, Any],
        # Worker parameters
        worker_cpu: float,          # cpu requirement per actor
        n_samples: int = 10,        # number of samples to use
) -> str:
    """
    Compute exact dedup execution parameters
    :param cluster_cpu: overall cluster cpu
    :param cluster_memory: overall cluster memory
    :param worker_cpu: worker cpu requirement
    :param n_samples: number of samples to use
    :param params: exact dedup specific parameters containing the following keys:
        s3_input_folder - s3 input folder
        hash_cpu - hash cpu requirements
    :return: json string, containing computed number of workers and hashes
    """
    # required import
    import json
    import math
    import sys
    from kfp_support.workflow_support.utils import KFPUtils
    from data_processing.data_access import DataAccessS3
    from data_processing.utils import KB, GB

    EXECUTION_OF_KB_DOC = 0.00025

    # get credentials
    s3_key, s3_secret, s3_endpoint = KFPUtils.credentials()
    s3_creds = {"access_key": s3_key,
                "secret_key": s3_secret,
                "url": s3_endpoint
                }
    s3_config = {"input_folder": params.get("s3_input_prefix"),
                 "output_folder": "",
                 }
    # because S3 is the only viable version for kfp-based implementation, we are here creating DataAccess S3 directly
    data_access = DataAccessS3(s3_credentials=s3_creds, s3_config=s3_config, d_sets=None, checkpoint=False, m_files=-1)
    # sample input data
    sampling = data_access.sample_input_data(n_samples=n_samples)
    avg_doc_size = sampling.get('average doc size KB')
    number_of_docs = sampling.get('estimated number of docs')
    avg_table_size = sampling.get("average table size MB") / KB
    # compute number of hashes
    n_hashes = math.ceil(number_of_docs * 32 / GB)
    print(f"Estimated Required hashes {n_hashes}")
    print(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_memory}")
    hash_cpu: float = float(params.get("hash_cpu"))
    required_hash_cpu = n_hashes * hash_cpu
    required_hash_mem = n_hashes * 2
    if required_hash_cpu > cluster_cpu or required_hash_mem > cluster_memory:
        print(
            f"Cluster is too small - hashes required cpus {required_hash_cpu}; "
            f"hashes required memory {required_hash_mem}"
        )
        sys.exit(1)
    # Define number of workers
    n_workers = int((.85 * cluster_cpu - required_hash_cpu) / worker_cpu)
    print(f"Number of workers - {n_workers}")
    if n_workers < 5:
        print(f"Cluster is too small - estimated number of workers {n_workers}")
        sys.exit(1)
    # Limit amount of workers and processors to prevent S3 saturation
    if n_workers > 1500:
        n_workers = 1500
    # validate that we have enough memory
    r_mem = required_hash_mem * 2 + avg_table_size * 4 * n_workers
    print(f"Required execution memory {r_mem} GB")
    if r_mem > cluster_memory:
        print(f"Not enough memory to run de duping, required {r_mem}, available {cluster_memory}")
        print(f"Try to increase the size of the cluster or increase size of the cpu per worker")
        sys.exit(1)
    print(f"Projected execution time {EXECUTION_OF_KB_DOC * avg_doc_size * number_of_docs / n_workers / 60} min")
    return json.dumps({"workers": n_workers, "hashes": n_hashes})
