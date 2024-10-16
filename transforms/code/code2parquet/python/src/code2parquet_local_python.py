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

import ast
import os
import sys

from code2parquet_transform import (  # domain_key,; snapshot_key,
    detect_programming_lang_cli_key,
    supported_langs_file_cli_key,
)
from code2parquet_transform_python import CodeToParquetPythonConfiguration
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils


# create parameters
supported_languages_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../test-data/languages/lang_extensions.json")
)
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
ingest_config = {
    supported_langs_file_cli_key: supported_languages_file,
    detect_programming_lang_cli_key: True,
    # snapshot_key: "github",
    # domain_key: "code",
}

params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "data_files_to_use": ast.literal_eval("['.zip']"),
    # orchestrator
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
}

if __name__ == "__main__":
    sys.argv = ParamsUtils.dict_to_req(d=(params | ingest_config))
    # create launcher
    launcher = PythonTransformLauncher(CodeToParquetPythonConfiguration())
    # launch
    launcher.launch()
