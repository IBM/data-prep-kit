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

import pyarrow as pa
import pyarrow.parquet as pq
from data_processing.test_support.transform import AbstractTableTransformTest
from data_processing.transform import get_transform_config
from filter_transform import (
    FilterTransform,
    FilterTransformConfiguration,
    filter_columns_to_drop_cli_param,
    filter_criteria_cli_param,
    filter_logical_operator_cli_param,
    filter_logical_operator_default,
)


class TestFilterTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def create_filter_test_fixture(
        self,
        filter_criteria: str,
        filter_logical_operator: str,
        filter_columns_to_drop: str,
        input_dir: str,
        expected_output_dir: str,
    ) -> tuple[FilterTransform, pa.Table, pa.Table, list[dict]]:
        cli = [
            f"--{filter_criteria_cli_param}",
            filter_criteria,
            f"--{filter_logical_operator_cli_param}",
            filter_logical_operator,
            f"--{filter_columns_to_drop_cli_param}",
            filter_columns_to_drop,
        ]
        ftc = FilterTransformConfiguration()
        config = get_transform_config(ftc, cli)
        input_df = pq.read_table(os.path.join(input_dir, "test1.parquet"))
        expected_output_df = pq.read_table(os.path.join(expected_output_dir, "test1.parquet"))
        with open(os.path.join(expected_output_dir, "metadata.json"), "r") as meta_file:
            expected_metadata = json.load(meta_file)
        expected_metadata_list = [expected_metadata, {}]
        return FilterTransform(config), [input_df], [expected_output_df], expected_metadata_list

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = []
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/"))
        # test AND logical operator with two numerical clauses
        filter_criteria = (
            """["docq_total_words > 100 AND docq_total_words < 200", "ibmkenlm_docq_perplex_score < 230"]"""
        )
        filter_logical_operator = filter_logical_operator_default
        filter_columns_to_drop = """["extra", "cluster"]"""
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "test-and-local")
        fixtures.append(
            self.create_filter_test_fixture(
                filter_criteria,
                filter_logical_operator,
                filter_columns_to_drop,
                input_dir,
                expected_output_dir,
            )
        )

        # test OR logical operator with two numerical clauses
        filter_logical_operator = "OR"
        expected_output_dir = os.path.join(basedir, "expected", "test-or-local")
        fixtures.append(
            self.create_filter_test_fixture(
                filter_criteria,
                filter_logical_operator,
                filter_columns_to_drop,
                input_dir,
                expected_output_dir,
            )
        )

        # test default parameters, no filtering on rows, or columns
        filter_criteria = """[]"""
        filter_logical_operator = filter_logical_operator_default
        filter_columns_to_drop = """[]"""
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "test-default-local")
        fixtures.append(
            self.create_filter_test_fixture(
                filter_criteria,
                filter_logical_operator,
                filter_columns_to_drop,
                input_dir,
                expected_output_dir,
            )
        )

        # test filters on non-numeric data types: datetime and string LIKE query
        filter_criteria = """["date_acquired BETWEEN '2023-07-04' AND '2023-07-08'", "title LIKE 'https://%'"]"""
        filter_logical_operator = filter_logical_operator_default
        filter_columns_to_drop = """[]"""
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "test-datetime-like-local")
        fixtures.append(
            self.create_filter_test_fixture(
                filter_criteria,
                filter_logical_operator,
                filter_columns_to_drop,
                input_dir,
                expected_output_dir,
            )
        )

        # test filters that use IN queries (p IN ('x', 'y', 'z'))
        filter_criteria = """["document IN ('CC-MAIN-20190221132217-20190221154217-00305.warc.gz', 'CC-MAIN-20200528232803-20200529022803-00154.warc.gz', 'CC-MAIN-20190617103006-20190617125006-00025.warc.gz')"]"""
        filter_logical_operator = filter_logical_operator_default
        filter_columns_to_drop = """[]"""
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "test-in-local")
        fixtures.append(
            self.create_filter_test_fixture(
                filter_criteria,
                filter_logical_operator,
                filter_columns_to_drop,
                input_dir,
                expected_output_dir,
            )
        )

        return fixtures


if __name__ == "__main__":
    t = TestFilterTransform()
