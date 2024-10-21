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

from data_cleaning_transform import (
    document_id_column_cli_param,
    duplicate_list_location_cli_param,
    operation_mode_cli_param,
)
from data_cleaning_transform_ray import DataCleaningRayTransformConfiguration
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher


class TestRayDataCleaningTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        duplicate_location = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "test-data",
                "expected",
                "get_list_transform",
                "docs_to_remove_consolidated",
                "docs_to_remove_consolidated.parquet",
            )
        )
        config = {
            "run_locally": True,
            document_id_column_cli_param: "int_id_column",
            duplicate_list_location_cli_param: duplicate_location,
            operation_mode_cli_param: "annotate",
        }
        launcher = RayTransformLauncher(DataCleaningRayTransformConfiguration())
        fixtures = [
            (
                launcher,
                config,
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "data_cleaning", "annotated"),
            )
        ]
        return fixtures
