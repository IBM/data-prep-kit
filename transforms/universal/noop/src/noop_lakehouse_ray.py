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

from data_processing.ray import TransformLauncher
from data_processing.utils import DPLConfig, ParamsUtils
from noop_transform import NOOPTransformConfiguration


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
# create parameters
# create parameters
s3_cred = {
    "access_key": "YOUR Lakehouse COS ACCESS KEY",
    "secret_key": "YOUR Lakehouse COS PRIVATE KEY",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "academic.ieee",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "academic.ieee_ededup_test",
    "output_path": "lh-test/tables/academic/ieee_ededup_test",
    "token": DPLConfig.LAKEHOUSE_TOKEN,
}

worker_options = {"num_cpus": 0.5}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 6,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # Noop
    "noop_sleep_sec": 1,
}
sys.argv = ParamsUtils.dict_to_req(d=params)
# for arg in sys.argv:
#     print(arg)

# launch
launcher.launch()
