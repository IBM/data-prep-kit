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
import ast

from data_processing.runtime.ray import RayTransformLauncher
from data_processing.test_support.launch import AbstractTransformLauncherTest
from ingest_2_parquet_transform import (
    IngestToParquetRayConfiguration,
    ingest_supported_langs_file_key,
    ingest_detect_programming_lang_key,
    ingest_domain_key,
    ingest_snapshot_key,
)


class TestRayIngestToParquetTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """
    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        lang_supported_file = os.path.abspath(
            os.path.join(basedir,"languages/lang_extensions.json",)
        )
        config = {
            "data_files_to_use": ast.literal_eval("['.zip']"),
            ingest_supported_langs_file_key: lang_supported_file,
            ingest_detect_programming_lang_key: True,
            ingest_snapshot_key: "github",
            ingest_domain_key: "code",
        }
        fixtures = [
            (
                RayTransformLauncher(IngestToParquetRayConfiguration()),
                config,
                basedir + "/input",
                basedir + "/expected",
                # this is added as a fixture to remove these 2 columns from comparison
                ["size", "date_acquired"],
            )
        ]
        return fixtures
