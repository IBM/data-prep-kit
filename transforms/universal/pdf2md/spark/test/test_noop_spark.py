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
from noop_transform_spark import NOOPSparkRuntimeConfiguration, sleep_cli_param
from pyspark.sql import DataFrame, SparkSession


class TestSparkNOOPTransform(AbstractSparkTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        proj_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        basedir = os.path.join(proj_dir, "test-data")
        config_file_path = os.path.join(proj_dir, "config", "spark_profile_local.yml")
        fixtures = []
        launcher = SparkTransformLauncher(NOOPSparkRuntimeConfiguration())
        fixtures.append(
            (
                launcher,
                {sleep_cli_param: 0, local_config_path_cli: config_file_path},
                basedir + "/input",
                basedir + "/expected",
            )
        )
        return fixtures

    def _validate_metadata_content(self, test_generated: dict, expected: dict, ignore_columns: list[str] = []):
        assert "nrows" in test_generated and test_generated["nrows"] == expected["nrows"]
