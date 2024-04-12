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

from blocklist_transform import (
    BlockListTransformConfiguration,
    annotation_column_name_cli_param,
    blocked_domain_list_path_cli_param,
    source_url_column_name_cli_param,
)
from data_processing.ray import TransformLauncher
from data_processing.utils import DPLConfig, ParamsUtils


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
# create parameters
blocklist_conf_url = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/domains"))
blocklist_annotation_column_name = "blocklisted"
blocklist_doc_source_url_column = "title"

input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
block_list_params = {
    blocked_domain_list_path_cli_param: blocklist_conf_url,
    annotation_column_name_cli_param: blocklist_annotation_column_name,
    source_url_column_name_cli_param: blocklist_doc_source_url_column,
}

s3_cred = {
    "access_key": DPLConfig.S3_ACCESS_KEY,
    "secret_key": DPLConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "bluepile.academic.doabooks",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "lh_test.bluepile_academic_doabooks_blocklist_test",
    "output_path": "lh-test/tables/lh_test/bluepile_academic_doabooks_blocklist_test",
    "token": DPLConfig.LAKEHOUSE_TOKEN,
}

worker_options = {"num_cpus": 0.5}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
launcher_params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}

# launch
if __name__ == "__main__":
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    # create launcher
    sys.argv = ParamsUtils.dict_to_req(launcher_params | block_list_params)
    launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()
