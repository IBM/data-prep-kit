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

import json
import os

from data_processing_spark.runtime.spark import local_config_path_cli
from data_processing_spark.runtime.spark.spark_launcher import SparkTransformLauncher
from data_processing_spark.test_support.launch.abstract_launcher_test import (
    AbstractSparkTransformLauncherTest,
)
from doc_id_transform_spark import (
    DocIDTransformConfiguration,
    doc_id_column_name_cli_param,
)


class TestSparkDocIDTransform(AbstractSparkTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        proj_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        basedir = os.path.join(proj_dir, "test-data")
        config_file_path = os.path.join(proj_dir, "config", "spark_profile_local.yml")
        fixtures = []

        fixtures.append(
            (
                SparkTransformLauncher(DocIDTransformConfiguration()),
                {
                    doc_id_column_name_cli_param: "doc_id",
                    local_config_path_cli: config_file_path,
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected"),
            )
        )
        return fixtures

    def _validate_directory_contents_match(self, dir: str, expected: str, ignore_columns: list[str] = []):
        super()._validate_directory_contents_match(dir, expected, ignore_columns)
        with open(os.path.join(dir, "metadata.json"), "r") as meta_fp:
            meta_dict = json.load(meta_fp)
        #     with open(os.path.join(expected, "metadata.json")) as expected_meta_fp:
        #         expected_meta_dict = json.load(expected_meta_fp)
        #         assert "nrows" in meta_dict and meta_dict["nrows"] == expected_meta_dict["nrows"]
