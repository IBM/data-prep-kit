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
from proglang_select_transform import (
    lang_allowed_langs_file_key,
    lang_lang_column_key,
    lang_output_column_key,
)
from proglang_select_transform_python import ProgLangSelectPythonConfiguration


# create parameters
language_column_name = "language"
annotated_column_name = "lang_selected"

selected_languages_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../test-data/languages/allowed-code-languages.txt")
)
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
langselect_config = {
    lang_allowed_langs_file_key: selected_languages_file,
    lang_lang_column_key: language_column_name,
    lang_output_column_key: annotated_column_name,
}
params = {
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # orchestrator
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    # lanuage selection specific parameters
    **langselect_config,
}

if __name__ == "__main__":
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # create launcher
    launcher = PythonTransformLauncher(ProgLangSelectPythonConfiguration())
    # launch
    launcher.launch()
