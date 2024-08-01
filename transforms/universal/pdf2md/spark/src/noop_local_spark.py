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
from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from noop_transform_spark import NOOPSparkRuntimeConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config/spark_profile_local.yml"))
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # execution info
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    "spark_local_config_filepath": config_path,
    # noop params
    "noop_sleep_sec": 1,
}
if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = SparkTransformLauncher(runtime_config=NOOPSparkRuntimeConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
