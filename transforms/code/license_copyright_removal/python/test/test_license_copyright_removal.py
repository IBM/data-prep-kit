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
from license_copyright_removal_transform import (
    LicenseCopyrightRemoveTransform,
    LicenseCopyrightRemovalTransformConfiguration,
    column_cli_params,
    license_cli_params,
    copyright_cli_params,
)


class TestLicenseCopyrightRemovalTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def create_license_copyright_removal_test_fixture(
        self,
        column: str,
        license: bool,
        copyright: bool,
        input_dir: str,
        expected_output_dir: str,
    ) -> tuple[LicenseCopyrightRemoveTransform, pa.Table, pa.Table, list[dict]]:
        cli = [
            f"--{column_cli_params}",
            column,
        ]
        ftc = LicenseCopyrightRemovalTransformConfiguration()
        config = get_transform_config(ftc, cli)
        input_df = pq.read_table(os.path.join(input_dir, "test1.parquet"))
        expected_output_df = pq.read_table(os.path.join(expected_output_dir, "test1.parquet"))
        with open(os.path.join(expected_output_dir, "metadata.json"), "r") as meta_file:
            expected_metadata = json.load(meta_file)
        expected_metadata_list = [expected_metadata, {}]
        return LicenseCopyrightRemoveTransform(config), [input_df], [expected_output_df], expected_metadata_list

    def get_test_transform_fixtures(self) -> list[tuple]:
        fixtures = []
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/"))
        # test for both license and copyright removal
        column_name = 'contents'
        license = True
        copyright = True
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "license-and-copyright-local")
        fixtures.append(
            self.create_license_copyright_removal_test_fixture(
                column_name,
                license,
                copyright,
                input_dir,
                expected_output_dir,
            )
        )

        # test for only license removal
        copyright = False
        expected_output_dir = os.path.join(basedir, "expected", "license-local")
        fixtures.append(
            self.create_license_copyright_removal_test_fixture(
                column_name,
                license,
                copyright,
                input_dir,
                expected_output_dir,
            )
        )

        # test for only copyright removal
        column_name = 'contents'
        license = False
        copyright = True
        input_dir = os.path.join(basedir, "input")
        expected_output_dir = os.path.join(basedir, "expected", "copyright-local")
        fixtures.append(
            self.create_license_copyright_removal_test_fixture(
                column_name,
                license,
                copyright,
                input_dir,
                expected_output_dir,
            )
        )

        return fixtures


if __name__ == "__main__":
    t = TestLicenseCopyrightRemovalTransform()
