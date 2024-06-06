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
from filter_transform_spark import (
    FilterTransformConfiguration,
    filter_columns_to_drop_cli_param,
    filter_criteria_cli_param,
    filter_logical_operator_cli_param,
    filter_logical_operator_default,
)


class TestSparkFilterTransform(AbstractSparkTransformLauncherTest):
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
                SparkTransformLauncher(FilterTransformConfiguration()),
                {
                    filter_criteria_cli_param: [
                        "docq_total_words > 100 AND docq_total_words < 200",
                        "ibmkenlm_docq_perplex_score < 230",
                    ],
                    filter_logical_operator_cli_param: filter_logical_operator_default,
                    filter_columns_to_drop_cli_param: ["extra", "cluster"],
                    local_config_path_cli: config_file_path,
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-and"),
            )
        )

        fixtures.append(
            (
                SparkTransformLauncher(FilterTransformConfiguration()),
                {
                    filter_criteria_cli_param: [
                        "docq_total_words > 100 AND docq_total_words < 200",
                        "ibmkenlm_docq_perplex_score < 230",
                    ],
                    filter_logical_operator_cli_param: "OR",
                    filter_columns_to_drop_cli_param: ["extra", "cluster"],
                    local_config_path_cli: config_file_path,
                },
                os.path.join(basedir, "input"),
                os.path.join(basedir, "expected", "test-or"),
            )
        )
        # These test are also done in the python-only tests, so no real need to duplicate here.  They slow down ci/cd builds.
        # fixtures.append(
        #     (
        #         SparkTransformLauncher(FilterTransformConfiguration()),
        #         {
        #             filter_criteria_cli_param: [],
        #             filter_logical_operator_cli_param: filter_logical_operator_default,
        #             filter_columns_to_drop_cli_param: [],
        #             local_config_path_cli: config_file_path,
        #         },
        #         os.path.join(basedir, "input"),
        #         os.path.join(basedir, "expected", "test-default"),
        #     )
        # )
        #
        # fixtures.append(
        #     (
        #         SparkTransformLauncher(FilterTransformConfiguration()),
        #         {
        #             filter_criteria_cli_param: [
        #                 "date_acquired BETWEEN '2023-07-04' AND '2023-07-08'",
        #                 "title LIKE 'https://%'",
        #             ],
        #             filter_logical_operator_cli_param: filter_logical_operator_default,
        #             filter_columns_to_drop_cli_param: [],
        #             local_config_path_cli: config_file_path,
        #         },
        #         os.path.join(basedir, "input"),
        #         os.path.join(basedir, "expected", "test-datetime-like"),
        #     )
        # )
        #
        # fixtures.append(
        #     (
        #         SparkTransformLauncher(FilterTransformConfiguration()),
        #         {
        #             filter_criteria_cli_param: [
        #                 "document IN ('CC-MAIN-20190221132217-20190221154217-00305.warc.gz', 'CC-MAIN-20200528232803-20200529022803-00154.warc.gz', 'CC-MAIN-20190617103006-20190617125006-00025.warc.gz')",
        #             ],
        #             filter_logical_operator_cli_param: filter_logical_operator_default,
        #             filter_columns_to_drop_cli_param: [],
        #             local_config_path_cli: config_file_path,
        #         },
        #         os.path.join(basedir, "input"),
        #         os.path.join(basedir, "expected", "test-in"),
        #     )
        # )
        return fixtures

    def _validate_directory_contents_match(self, dir: str, expected: str, ignore_columns: list[str] = []):
        super()._validate_directory_contents_match(dir, expected, ignore_columns)
        with open(os.path.join(dir, "metadata.json"), "r") as meta_fp:
            meta_dict = json.load(meta_fp)
        #     with open(os.path.join(expected, "metadata.json")) as expected_meta_fp:
        #         expected_meta_dict = json.load(expected_meta_fp)
        #         assert "nrows" in meta_dict and meta_dict["nrows"] == expected_meta_dict["nrows"]
