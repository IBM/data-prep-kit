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

import os
import sys

from data_processing.utils import ParamsUtils
from data_processing_ray.runtime.ray import RayTransformLauncher
from fdedup_transform_ray import FdedupRayTransformConfiguration


# create launcher
launcher = RayTransformLauncher(FdedupRayTransformConfiguration())
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # Orchestration parameters
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 1,
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_creation_delay": 0,
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # columns used
    "fdedup_doc_column": "contents",
    "fdedup_id_column": "int_id_column",
    "fdedup_cluster_column": "cluster",
    # infrastructure
    "fdedup_bucket_cpu": 0.5,
    "fdedup_doc_cpu": 0.5,
    "fdedup_mhash_cpu": 0.5,
    "fdedup_num_doc_actors": 1,
    "fdedup_num_bucket_actors": 1,
    "fdedup_num_minhash_actors": 1,
    "fdedup_num_preprocessors": 2,
    # fuzzy parameters
    "fdedup_num_permutations": 64,
    "fdedup_threshold": 0.8,
    "fdedup_shingles_size": 5,
    "fdedup_delimiters": " ",
    # Random delay between reads
    "fdedup_random_delay_limit": 5,
    # snapshotting
    "fdedup_snapshot_delay": 1,
    "fdedup_use_doc_snapshot": False,
    "fdedup_use_bucket_snapshot": False,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
