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
from fdedup.utils import FdedupSupport


"""
This is a small code, that should be run locally to get the estimate of required cluster based on the amount 
the documents to be processed, Fuzzy dedup parameters and Ray worker nodes numbers of CPUs. These values can 
be modified based on the job you need to run. The rest of the values are based on the experience of running
fuzzy dedup and should not be modified. We here compute three different cluster sizes:
- preprocessor
- bucket processor
- filter
For preprocessor and filter we cap number of workers to 800 to ensure that we are not starving S3.
Number of bucket processors that are used for the actual deduping do not impact S3 and can be made
as large as desired to improve the throughput 
"""


# change these parameters for specific use case
number_of_docs = 4000000000
# Fuzzy dedup params
n_permutations = 64
threshold = 0.7
false_positive = 0.5
false_negative = 0.5
# Number of workers for initial run
n_workers = 800
if n_workers > 800:
    n_workers = 800
n_bucket_processors = 1000
# pod cpus
ray_node_cpu = 50
# cpu sizes fo actors
processor_cpu = 0.8
bucket_processor_cpu = 0.8
bucket_cpu = 0.5
minhash_cpu = 0.5
doc_cpu = 0.5
# Experimental values, do not change
bucket_actor_memory = 3.0
minhash_actor_memory = 5.0
document_actor_memory = 3.0
processor_memory = 2.0

# Fuzzy dedup params
num_buckets, length_bucket = FdedupSupport.fuzzy_optimal_param(
    threshold=threshold,
    num_perm=n_permutations,
    false_positive_weight=false_positive,
    false_negative_weight=false_negative,
)
print(f"threshold {threshold}, num permutations {n_permutations}")
print(f"Fuzzy parameters: num buckets {num_buckets}, bucket length {length_bucket}")

print(f"Number of documents {number_of_docs}")

bucket_actors = math.ceil(num_buckets * number_of_docs * 64 * 1.1 / GB)
document_actors = math.ceil(number_of_docs * 48 * 1.1 / GB)
minhash_actors = math.ceil(number_of_docs * 128 * 1.1 / GB)

print("Preprocessor")
print(f"    Desired of workers {n_workers} with cpu {processor_cpu}")
print(f"    Required document actors {document_actors} with cpu {doc_cpu}")
print(f"    Required minhash actors {minhash_actors} with cpu {minhash_cpu}")
print(f"    Required bucket actors {bucket_actors} with cpu {bucket_cpu}")


execution_required_cpus = math.ceil(
    (processor_cpu * n_workers + bucket_actors * bucket_cpu + minhash_actors * minhash_cpu + document_actors * doc_cpu)
    / 0.85
)

print(f"    Required overall cpu: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"    Required for execution ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")

execution_required_memory = math.ceil(
    (
        n_workers * processor_memory
        + bucket_actors * bucket_actor_memory
        + minhash_actors * minhash_actor_memory
        + document_actors * document_actor_memory
    )
    / 0.6
)

print(f"    Required execution memory : {execution_required_memory}")
print(f"    Minimal required memory per ray node is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
    f"Practically use the larger number"
)

print("Bucket processor")
n_workers_b = math.ceil(bucket_actors/ 50)
print(f"    Desired number of bucket processors {n_bucket_processors} with cpu {bucket_processor_cpu}")
print(f"    Number of workers {n_workers_b} with cpu {processor_cpu}")
execution_required_cpus = math.ceil(
    (processor_cpu * n_workers_b + bucket_actors * bucket_cpu + minhash_actors * minhash_cpu +
     document_actors * doc_cpu + n_bucket_processors * bucket_processor_cpu)
    / 0.85
)

print(f"    Required overall cpu: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"    Required for execution ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")
execution_required_memory = math.ceil(
    (
            n_workers_b * processor_memory
            + n_bucket_processors * processor_memory
            + bucket_actors * bucket_actor_memory
            + minhash_actors * minhash_actor_memory
            + document_actors * document_actor_memory
    )
    / 0.6
)

print(f"    Required execution memory : {execution_required_memory}")
print(f"    Minimal required memory per ray node is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
      f"Practically use the larger number"
      )

print("Filter")
print(f"    Desired of workers {n_workers} with cpu {processor_cpu}")
print(f"    Required document actors {document_actors} with cpu {doc_cpu}")
execution_required_cpus = math.ceil(
    (processor_cpu * n_workers + document_actors * doc_cpu)
    / 0.85
)

print(f"    Required overall cpu: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"    Required for execution ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")

execution_required_memory = math.ceil(
    (
            n_workers * processor_memory
            + document_actors * document_actor_memory
    )
    / 0.6
)

print(f"    Required execution memory : {execution_required_memory}")
print(f"    Minimal required memory per ray node is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
      f"Practically use the larger number"
      )
