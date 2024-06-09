# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import json
import os

from data_processing.data_access import DataAccessLocal
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from pyspark.sql import SparkSession


class AbstractSparkTransformLauncherTest(AbstractTransformLauncherTest):
    def _validate_metadata_content(self, test_generated: dict, expected: dict):
        """
        Called on the generated and expected metadata content produced by a launch.
        This allows specific comparisons on transform-specific metadata.
        Default implementation is to not do any comparison.
        """
        pass

    def _validate_directory_contents_match(self, dir: str, expected: str, ignore_columns: list[str] = []):
        # we assume launch is completed and stopped,
        # therefore we are creating a new session and want to stop it below
        spark = SparkSession.builder.getOrCreate()
        dal = DataAccessLocal()
        test_generated_files = dal._get_all_files_ext(dir, [".parquet"])
        expected_files = dal._get_all_files_ext(expected, [".parquet"])
        assert len(test_generated_files) > 0
        result_df = spark.read.parquet(*test_generated_files)
        expected_df = spark.read.parquet(*expected_files)
        result_length = result_df.count()
        expected_length = expected_df.count()
        spark.stop()
        assert result_length == expected_length
        self._validate_metadata(dir, expected)

    def _validate_metadata(self, dir, expected):
        """
        Look for metadata in both directories, read them and call
        self._validate_metadata_content().
        Args:
            dir:
            expected:
        Returns:
        """
        expected_metadata_file = os.path.join(expected, "metadata.json")
        if os.path.isfile(expected_metadata_file):
            generated_metadata_file = os.path.join(dir, "metadata.json")
            assert os.path.isfile(generated_metadata_file)
            with open(expected_metadata_file, "r") as meta_fp:
                test_generated_meta_dict = json.load(meta_fp)
                with open(os.path.join(expected, "metadata.json")) as expected_meta_fp:
                    expected_meta_dict = json.load(expected_meta_fp)
                    self._validate_metadata_content(test_generated_meta_dict, expected_meta_dict)
