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
from pathlib import Path

from data_processing.ray import TransformLauncher
from data_processing.utils import DPLConfig, ParamsUtils
from lang_annotator_transform import (
    LangSelectorTransformConfiguration,
    lang_allowed_langs_file_key,
    lang_known_selector,
    lang_lang_column_key,
    lang_output_column_key,
)


# create launcher
launcher = TransformLauncher(transform_runtime_config=LangSelectorTransformConfiguration())
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

s3_cred = {
    "access_key": DPLConfig.S3_ACCESS_KEY,
    "secret_key": DPLConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "code.ready_for_token",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "code.ready_for_token_langselect_test",
    "output_path": "lh-test/tables/code/ready_for_token_langselect_test",
    "token": DPLConfig.LAKEHOUSE_TOKEN,
}

worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}

langselect_config = {
    lang_allowed_langs_file_key: selected_languages_file,
    lang_lang_column_key: language_column_name,
    lang_output_column_key: annotated_column_name,
    lang_known_selector: True,
    "lang_select_local_config": ParamsUtils.convert_to_ast(local_conf),
}

params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # language selection specific parameter
    **langselect_config,
}

if __name__ == "__main__":
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    sys.argv = ParamsUtils.dict_to_req(d=params)
    # launch
    launcher.launch()
