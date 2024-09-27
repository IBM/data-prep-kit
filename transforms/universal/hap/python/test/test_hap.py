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

# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.

import os
import sys
sys.path.append(os.path.join(os.path.abspath('.'),'./../src'))

from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.utils import ParamsUtils
from data_processing.runtime.pure_python import PythonTransformLauncher
from hap_transform import HAPPythonTransformConfiguration


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "test-data/input"))


code_location = {"github": "github", "commit_hash": "12345", "path": "path"}

params = {
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
}

hap_params = {
    "model_name_or_path": 'ibm-granite/granite-guardian-hap-38m',
    "annotation_column": "hap_score",
    "doc_text_column": "doc_text",
    "inference_engine": "CPU",
    "max_length": 512,
    "batch_size": 128,
}


class TestPythonHAPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        launcher = PythonTransformLauncher(HAPPythonTransformConfiguration())
        fixtures = [
            (
                launcher,
                sys_params | hap_params,
                basedir + "/input",
                basedir + "/expected",
                ["hap_score"],
            )
        ]
        return fixtures

