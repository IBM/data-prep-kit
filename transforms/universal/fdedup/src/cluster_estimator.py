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
number_of_docs = 8423678743
# Fuzzy dedup params
n_permutations = 64
threshold = 0.75
false_positive = 0.5
false_negative = 0.5
# Number of workers for initial run
n_workers = 1000
# pod cpus
ray_node_cpu = 15
# cpu sizes fo actors
processor_cpu = 1.0
bucket_cpu = 1.0
minhash_cpu = 1.0
doc_cpu = 0.5
# Experimental values, do not change
bucket_actor_memory = 2.0
minhash_actor_memory = 5.0
document_actor_memory = 2.5
processor_memory = 2.0
num_buckets, length_bucket = fuzzy_optimal_param(
    threshold=threshold,
    num_perm=n_permutations,
    false_positive_weight=false_positive,
    false_negative_weight=false_negative,
)
print(f"Fuzzy parameters: num buckets {num_buckets}, bucket length {length_bucket}")

bucket_actors = math.ceil(num_buckets * number_of_docs * 64 * 1.1 / GB)
document_actors = math.ceil(number_of_docs * 48 * 1.1 / GB)
minhash_actors = math.ceil(number_of_docs * 128 * 1.1 / GB)

print(f"desired workers {n_workers}")
print(f"Required document actors {document_actors}")
print(f"Required minhash actors {minhash_actors}")
print(f"Required bucket actors {bucket_actors}")

execution_required_cpus = math.ceil(
    (processor_cpu * n_workers + bucket_actors * bucket_cpu + minhash_actors * minhash_cpu +
     document_actors * doc_cpu) / 0.85
)

print(f"required cpu: {execution_required_cpus}")
n_ray_nodes = math.ceil(execution_required_cpus / ray_node_cpu)
print(f"Required for execution ray nodes with {ray_node_cpu} cpus is {n_ray_nodes}")

execution_required_memory = math.ceil(
    (n_workers * processor_memory + bucket_actors * bucket_actor_memory + minhash_actors * minhash_actor_memory +
     document_actors * document_actor_memory) / 0.6
)
print(f"required execution memory : {execution_required_memory}")
print(f"Minimal required memory per ray node is {math.ceil(execution_required_memory / n_ray_nodes)} GB. "
      f"Practically use the larger number")
