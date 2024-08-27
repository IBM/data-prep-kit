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
import ast

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from fdedup.transforms.base import (bucket_processor_num_permutations_cli_param,
                                    bucket_processor_threshold_cli_param,
                                    bucket_processor_minhash_snapshot_directory_cli_param,
                                    )
from fdedup.transforms.python import FdedupBucketProcessorPythonTransformRuntimeConfiguration

# create launcher
launcher = PythonTransformLauncher(FdedupBucketProcessorPythonTransformRuntimeConfiguration())
# create parameters
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test-data/input/snapshot/"))
input_folder = os.path.join(basedir, "buckets")
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "data_files_to_use": ast.literal_eval("['']"),
    # orchestrator
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # fdedup parameters
    bucket_processor_num_permutations_cli_param: 64,
    bucket_processor_threshold_cli_param: .8,
    bucket_processor_minhash_snapshot_directory_cli_param: os.path.join(basedir, "minhash"),
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
