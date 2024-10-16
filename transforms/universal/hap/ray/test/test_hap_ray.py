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
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from hap_transform_ray import HAPRayTransformConfiguration

hap_params = {
    "run_locally": True,
    "model_name_or_path": 'ibm-granite/granite-guardian-hap-38m',
    "annotation_column": "hap_score",
    "doc_text_column": "contents",
    "inference_engine": "CPU",
    "max_length": 512,
    "batch_size": 128,
}


class TestRayHAPTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """
    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        launcher = RayTransformLauncher(HAPRayTransformConfiguration())
        fixtures = [(launcher, hap_params, basedir + "/input", basedir + "/expected")]
        return fixtures
