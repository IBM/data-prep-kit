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
from fdedup_support import fuzzy_optimal_param


"""
This is a small code, that should be run locally to get the estimate of required cluster based on the amount 
the documents to be processed, Fuzzy dedup parameters and Ray worker nodes numbers of CPUs. These values can 
be modified based on the job you need to run. The rest of the values are based on the experience of running
fuzzy dedup and should not be modified.
"""


# change these parameters for specific use case
number_of_docs = 4000000000
# Fuzzy dedup params
n_permutations = 64
threshold = 0.7
false_positive = 0.5
false_negative = 0.5
# Number of workers for initial run
n_workers = 1200
# pod cpus
ray_node_cpu = 50
# cpu sizes fo actors
processor_cpu = 1.0
bucket_cpu = 0.75
minhash_cpu = 0.75
doc_cpu = 0.75
# Experimental values, do not change
bucket_actor_memory = 3.0
minhash_actor_memory = 5.0
document_actor_memory = 3.0
processor_memory = 2.0
num_buckets, length_bucket = fuzzy_optimal_param(
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

print(f"desired of preprocessors {n_workers} with cpu {processor_cpu}")
print(f"Required document actors {document_actors} with cpu {doc_cpu}")
print(f"Required minhash actors {minhash_actors} with cpu {minhash_cpu}")
print(f"Required bucket actors {bucket_actors} with cpu {bucket_cpu}")


execution_required_cpus = math.ceil(
    (processor_cpu * n_workers + bucket_actors * bucket_cpu + minhash_actors * minhash_cpu + document_actors * doc_cpu)
    / 0.85
)

print(f"required cpu: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"Required for execution ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")

execution_required_memory = math.ceil(
    (
        n_workers * processor_memory
        + bucket_actors * bucket_actor_memory
        + minhash_actors * minhash_actor_memory
        + document_actors * document_actor_memory
    )
    / 0.6
)

n_actors = int((0.85 * execution_required_memory - document_actors * doc_cpu) / processor_cpu)
# cap n_actors not to overwhelm S3
if n_actors > 800:
    n_actors = 800
print(f"number of workers {n_actors}")


print(f"required execution memory : {execution_required_memory}")
print(
    f"Minimal required memory per ray node is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
    f"Practically use the larger number"
)
