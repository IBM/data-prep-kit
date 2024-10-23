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

import math

from data_processing.utils import GB


"""
This is a small code, that should be run locally to get the estimate of required cluster based on the amount 
the documents to be processed and Ray worker nodes numbers of CPUs. These values can 
be modified based on the job you need to run. The rest of the values are based on the experience of running
exact dedup and should not be modified.
"""

# change these parameters for specific use case
av_table_size = 189056510  # bytes
number_of_docs = 4179209729

# Number of workers
n_workers = 1000
# pod cpus
ray_node_cpu = 15
# cpu sizes fo actors
worker_cpu = 0.75
hash_cpu = 0.5

print(f"suggested number of workers {n_workers}")

n_hashes = math.ceil(number_of_docs * 32 / GB)
required_hash_cpu = n_hashes * hash_cpu
required_hash_mem = n_hashes * 2

print(
    f"Estimated Required hashes {n_hashes}, cpus for hashes {required_hash_cpu}, "
    f"memory for hashes {required_hash_mem} GB"
)

execution_required_cpus = math.ceil((worker_cpu * n_workers + required_hash_cpu) / 0.85)

print(f"required cpus for execution: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"Required ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")

execution_required_memory = math.ceil((n_workers * av_table_size * 4 / GB + required_hash_cpu) / 0.6)
print(f"required memory for execution: {execution_required_memory}")
print(
    f"Minimal required memory per ray pod is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
    f"Practically use the larger number"
)
