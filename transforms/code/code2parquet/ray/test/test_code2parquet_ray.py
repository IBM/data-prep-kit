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

from code2parquet_transform import (
    detect_programming_lang_cli_key,
    detect_programming_lang_key,
    domain_cli_key,
    snapshot_cli_key,
    supported_langs_file_cli_key,
    supported_langs_file_key,
)
from code2parquet_transform_ray import CodeToParquetRayConfiguration
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher


class TestRayIngestToParquetTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        lang_supported_file = os.path.abspath(
            os.path.join(
                basedir,
                "languages/lang_extensions.json",
            )
        )
        config = {
            "run_locally": True,
            "data_files_to_use": ast.literal_eval("['.zip']"),
            supported_langs_file_cli_key: lang_supported_file,
            detect_programming_lang_cli_key: True,
            snapshot_cli_key: "github",
            domain_cli_key: "code",
        }
        fixtures = [
            (
                RayTransformLauncher(CodeToParquetRayConfiguration()),
                config,
                basedir + "/input",
                basedir + "/expected",
                # this is added as a fixture to remove these 2 columns from comparison
                ["date_acquired", "document_id"],
            )
        ]
        return fixtures
