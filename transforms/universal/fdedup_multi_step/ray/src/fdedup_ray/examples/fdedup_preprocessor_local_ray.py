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
from fdedup.transforms.base import (preprocessor_doc_column_name_cli_param,
                                    preprocessor_int_column_name_cli_param,
                                    delimiters_cli_param,
                                    preprocessor_num_permutations_cli_param,
                                    preprocessor_threshold_cli_param,
                                    shingles_size_cli_param,
                                    )
from fdedup_ray.transforms import (FdedupPreprocessorRayTransformRuntimeConfiguration,
                                   preprocessor_bucket_cpu_cli_param,
                                   preprocessor_minhash_cpu_cli_param,
                                   preprocessor_num_buckets_cli_param,
                                   preprocessor_num_minhash_cli_param,
                                   )

# create launcher
launcher = RayTransformLauncher(FdedupPreprocessorRayTransformRuntimeConfiguration())
# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # fdedup parameters
    preprocessor_doc_column_name_cli_param: "contents",
    preprocessor_int_column_name_cli_param: "Unnamed: 0",
    delimiters_cli_param: " ",
    preprocessor_num_permutations_cli_param: 64,
    shingles_size_cli_param: 5,
    preprocessor_threshold_cli_param: .8,
    preprocessor_num_buckets_cli_param: 1,
    preprocessor_bucket_cpu_cli_param: .5,
    preprocessor_num_minhash_cli_param: 1,
    preprocessor_minhash_cpu_cli_param: .5,


}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
