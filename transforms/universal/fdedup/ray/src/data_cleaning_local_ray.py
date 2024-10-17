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

from data_cleaning_transform import (
    document_id_column_cli_param,
    duplicate_list_location_cli_param,
)
from data_cleaning_transform_ray import DataCleaningRayTransformConfiguration
from data_processing.utils import ParamsUtils
from data_processing_ray.runtime.ray import RayTransformLauncher


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "data_1"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output", "cleaned"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
duplicate_location = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "output", "docs_to_remove_consolidated", "docs_to_remove_consolidated.parquet"
    )
)
worker_options = {"num_cpus": 0.8}

code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    document_id_column_cli_param: "int_id_column",
    duplicate_list_location_cli_param: duplicate_location,
    # execution info
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_creation_delay": 0,
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 3,
}


if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = RayTransformLauncher(DataCleaningRayTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
