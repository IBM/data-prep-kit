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

from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher
from doc_id_transform_ray import DocIDRayTransformRuntimeConfiguration
from doc_id_transform_base import (doc_column_name_cli_param,
                                   hash_column_name_cli_param,
                                   int_column_name_cli_param,
                                   start_id_cli_param,
                                   )


class TestRayDocIDTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        fixtures = []
        transform_config = {
            "run_locally": True,
            doc_column_name_cli_param: "contents",
            hash_column_name_cli_param: "hash_column",
            int_column_name_cli_param: "int_id_column",
            start_id_cli_param: 5,
        }
        launcher = RayTransformLauncher(DocIDRayTransformRuntimeConfiguration())
        fixtures.append((launcher, transform_config, basedir + "/input", basedir + "/expected"))
        return fixtures
