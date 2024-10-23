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
from doc_id_transform_ray import DocIDRayTransformRuntimeConfiguration
from doc_id_transform_base import (doc_column_name_cli_param,
                                   hash_column_name_cli_param,
                                   int_column_name_cli_param,
                                   start_id_cli_param,
                                   )

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
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 2,
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_creation_delay": 0,
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # doc id configuration
    doc_column_name_cli_param: "contents",
    hash_column_name_cli_param: "hash_column",
    int_column_name_cli_param: "int_id_column",
    start_id_cli_param: 5,
}

sys.argv = ParamsUtils.dict_to_req(d=params)
# create launcher
launcher = RayTransformLauncher(DocIDRayTransformRuntimeConfiguration())
# launch
launcher.launch()
