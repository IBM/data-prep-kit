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
from doc_quality_transform import (
    bad_word_filepath_cli_param,
    doc_content_column_cli_param,
    text_lang_cli_param,
)
from doc_quality_transform_ray import DocQualityRayTransformConfiguration


print(os.environ)
# create parameters
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
s3_cred = {
    "access_key": "localminioaccesskey",
    "secret_key": "localminiosecretkey",
    "url": "http://localhost:9000",
}
s3_conf = {
    "input_folder": "test/doc_quality/input",
    "output_folder": "test/doc_quality/output",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
model_path = os.path.join(basedir, "models")
if not os.path.exists(model_path):
    model_path = os.path.abspath(os.path.join(basedir, "..", "models"))
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    # orchestrator
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 3,
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_creation_delay": 0,
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # doc_quality params
    text_lang_cli_param: "en",
    doc_content_column_cli_param: "contents",
    bad_word_filepath_cli_param: os.path.join(basedir, "ldnoobw", "en"),
}
# for arg in sys.argv:
#     print(arg)

if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = RayTransformLauncher(DocQualityRayTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
