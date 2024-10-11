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
from ededup_transform_ray import EdedupRayTransformRuntimeConfiguration
from ededup_transform_base import doc_column_name_cli_param, int_column_name_cli_param
from ededup_transform_ray import hash_cpu_cli_params, num_hashes_cli_params


class TestRayEdedupTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            "run_locally": True,
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            hash_cpu_cli_params: 0.5,
            num_hashes_cli_params: 2,
            doc_column_name_cli_param: "contents",
            int_column_name_cli_param: "document_id",
        }
        launcher = RayTransformLauncher(EdedupRayTransformRuntimeConfiguration())
        fixtures = [(launcher, config, basedir + "/input", basedir + "/expected")]
        return fixtures
