from typing import Any


def ededup_compute_execution_params(
        worker_options: str,        # ray worker configuration
        actor_options: str,         # actor's resource requirements
        params: dict[str, Any],     # exact dedup specific parameters
        n_samples: int = 10,        # number of samples to use
) -> str:
    """
    Compute exact dedup execution parameters
    :param worker_options: cluster parameters
    :param actor_options: actor request requirements
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
    n_workers = int((.85 * cluster_cpu - required_hash_cpu) / actor_cpu)
    print(f"Number of workers - {n_workers}")
    if n_workers < 5:
        print(f"Cluster is too small - estimated number of workers {n_workers}")
        sys.exit(1)
    # Limit amount of workers and processors to prevent S3 saturation
    if n_workers > 1000:
        n_workers = 1000
    # validate that we have enough memory
    r_mem = required_hash_mem * 2 + avg_table_size * 4 * n_workers
    print(f"Required execution memory {r_mem} GB")
    if r_mem > cluster_memory:
        print(f"Not enough memory to run de duping, required {r_mem}, available {cluster_memory}")
        print(f"Try to increase the size of the cluster or increase size of the cpu per worker")
        sys.exit(1)
    print(f"Projected execution time {EXECUTION_OF_KB_DOC * avg_doc_size * number_of_docs / n_workers / 60} min")
    return json.dumps({"workers": n_workers, "hashes": n_hashes})
