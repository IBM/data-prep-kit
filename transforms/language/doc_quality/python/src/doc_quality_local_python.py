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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from doc_quality_transform import (
    text_lang_cli_param,
    doc_content_column_cli_param,
    bad_word_filepath_cli_param,
    model_path_cli_param,
    model_module_name_cli_param,
    perplex_score_digit_cli_param,
)
from doc_quality_transform_python import DocQualityPythonTransformConfiguration

# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
model_path=os.path.join(basedir, "models")
if not os.path.exists(model_path):
    model_path = os.path.abspath(os.path.join(basedir, "..", "models"))

params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # execution info
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # doc_quality params
    text_lang_cli_param: "en",
    doc_content_column_cli_param: "contents",
    bad_word_filepath_cli_param: os.path.join(basedir, "ldnoobw", "en"),
    model_path_cli_param: model_path,
    model_module_name_cli_param: "perplexity_transformer_model",
    perplex_score_digit_cli_param: 1,
}
if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = PythonTransformLauncher(runtime_config=DocQualityPythonTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
