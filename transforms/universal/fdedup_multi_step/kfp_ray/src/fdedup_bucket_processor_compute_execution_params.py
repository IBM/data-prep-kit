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


def fdedup_bucket_processor_compute_execution_params(
    worker_options: dict,  # ray worker configuration
    runtime_actor_options: dict,  # actor's resource requirements
    data_s3_config: str,  # s3 configuration
    data_max_files: int,  # max files to process
    data_num_samples: int,  # num samples to process
    runtime_pipeline_id: str,  # pipeline id
    runtime_job_id: str,  # job id
    runtime_code_location: dict,  # code location
    fdedup_bucket_processor_num_permutations: int,  # number of permutations
    fdedup_bucket_processor_threshold: float,  # threshold,
    fdedup_bucket_processor_bucket_cpu: float,  # number of CPUs per bucket hash
    fdedup_bucket_processor_doc_cpu: float,  # number of CPUs per doc hash
    fdedup_bucket_processor_mhash_cpu: float,  # number of CPUs per minhash hash
    fdedup_bucket_processor_processor_cpu: float,  # number of CPUs per bucket processor
    fdedup_bucket_processor_num_buckets: int,  # number of actors for bucket hash
    fdedup_bucket_processor_num_doc_id: int,  # number of actors fo doc hash
    fdedup_bucket_processor_num_minhashes: int,  # number of actors for minhash hash
    fdedup_bucket_processor_minhash_snapshot_directory: str,  # minhash_snapshot_directory
    fdedup_bucket_processor_buckets_snapshot_directory: str,  # buckets_snapshot_directory
    fdedup_bucket_processor_doc_id_snapshot_directory: str,  # doc id snapshot directory
) -> dict[str, Any]:
    """
    Compute fuzzy dedup bucket processor execution parameters
    :param worker_options: cluster parameters
    :param runtime_actor_options: actor request requirements
    :param data_s3_config: s3 configuration
    :param data_max_files: max files to process
    :param data_num_samples: num samples to process
    :param runtime_pipeline_id: pipeline id
    :param runtime_job_id: job id
    :param runtime_code_location: code location
    :param fdedup_bucket_processor_bucket_cpu: number of CPUs per bucket hash
    :param fdedup_bucket_processor_doc_cpu: number of CPUs per doc hash
    :param fdedup_bucket_processor_mhash_cpu: number of CPUs per minhash hash
    :param fdedup_bucket_processor_processor_cpu: number of CPUs per bucket processor
    :param fdedup_bucket_processor_num_buckets: number of actors for bucket hash
    :param fdedup_bucket_processor_num_doc_id: number of actors fo doc hash
    :param fdedup_bucket_processor_num_minhashes: number of actors for minhash hash
    :param fdedup_bucket_processor_num_permutations: number of permutations
    :param fdedup_bucket_processor_threshold: threshold,
    :param fdedup_bucket_processor_minhash_snapshot_directory: minhash snapshot directory
    :param fdedup_bucket_processor_doc_id_snapshot_directory: doc id snapshot directory
    :param fdedup_bucket_processor_buckets_snapshot_directory: buckets snapshot directory
    :return: a dictionary with a Ray Job execution parameters
    """
    import math
    import sys

    cluster_cpu = worker_options["replicas"] * worker_options["cpu"]
    cluster_memory = worker_options["replicas"] * worker_options["memory"]
    print(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_memory}")
    cluster_cpu *= 0.85
    cluster_memory *= 0.85
    # get actor requirements
    worker_cpu = runtime_actor_options["num_cpus"]
    print(f"worker required cpu {worker_cpu}")
    # Define number of workers.
    n_workers = math.ceil(fdedup_bucket_processor_num_buckets / 50)
    b_processors = int(
        (0.85 * cluster_cpu - fdedup_bucket_processor_num_buckets * fdedup_bucket_processor_bucket_cpu
         - fdedup_bucket_processor_num_minhashes * fdedup_bucket_processor_mhash_cpu
         - fdedup_bucket_processor_num_doc_id * fdedup_bucket_processor_doc_cpu
         - n_workers * worker_cpu) / fdedup_bucket_processor_processor_cpu
    )
    if b_processors < 0:
        print(f"Not enough CPUs to run fuzzy dedup bucket processor, computed number of bucket_processors "
              f"is {b_processors}")
        print(f"Required bucket actors {fdedup_bucket_processor_num_buckets}, minhash actors "
              f"{fdedup_bucket_processor_num_minhashes}, document actors {fdedup_bucket_processor_num_doc_id}, "
              f"workers {n_workers}")
        print("Try to increase the size of the cluster")
        sys.exit(1)
    # cap the number of workers to ensure that we do not overwhelm COS
    print(
        f"Number of workers: {n_workers}, bucket actors {fdedup_bucket_processor_num_buckets}, "
        f"minhash actors {fdedup_bucket_processor_num_minhashes}, "
        f"document actors {fdedup_bucket_processor_num_doc_id}, "
        f"bucket processors {b_processors}"
    )

    # Make sure that we have enough memory. We assume that each actor uses 3 GB memory
    r_mem = 3 * (n_workers + b_processors + fdedup_bucket_processor_num_buckets +
                 fdedup_bucket_processor_num_minhashes + fdedup_bucket_processor_num_doc_id)
    if r_mem > cluster_memory:
        print(f"Not enough memory to run de duping, required {r_mem}, available {cluster_memory}")
        print(f"Try to increase the size of the cluster or increase size of the cpu per worker (current {worker_cpu})")
        sys.exit(1)
    required_cpu = (fdedup_bucket_processor_num_buckets * fdedup_bucket_processor_bucket_cpu
                    + fdedup_bucket_processor_num_minhashes * fdedup_bucket_processor_mhash_cpu
                    + fdedup_bucket_processor_num_doc_id * fdedup_bucket_processor_doc_cpu +
                    n_workers * worker_cpu + b_processors * fdedup_bucket_processor_processor_cpu)
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
        "fdedup_bucket_processor_bucket_cpu": fdedup_bucket_processor_bucket_cpu,
        "fdedup_bucket_processor_doc_cpu": fdedup_bucket_processor_doc_cpu,
        "fdedup_bucket_processor_mhash_cpu": fdedup_bucket_processor_mhash_cpu,
        "fdedup_bucket_processor_processor_cpu": fdedup_bucket_processor_processor_cpu,
        "fdedup_bucket_processor_num_doc_id": fdedup_bucket_processor_num_doc_id,
        "fdedup_bucket_processor_num_buckets": fdedup_bucket_processor_num_buckets,
        "fdedup_bucket_processor_num_minhashes": fdedup_bucket_processor_num_minhashes,
        "fdedup_bucket_processor_num_processors": b_processors,
        "fdedup_num_permutations": fdedup_bucket_processor_num_permutations,
        "fdedup_threshold": fdedup_bucket_processor_threshold,
        "fdedup_bucket_processor_minhash_snapshot_directory": fdedup_bucket_processor_minhash_snapshot_directory,
        "fdedup_bucket_processor_buckets_snapshot_directory": fdedup_bucket_processor_buckets_snapshot_directory,
        "fdedup_bucket_processor_doc_id_snapshot_directory": fdedup_bucket_processor_doc_id_snapshot_directory,
    }
