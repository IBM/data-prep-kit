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

import pyarrow as pa
from data_processing.test_support.transform import AbstractTableTransformTest
from data_processing.transform import get_transform_config
from license_select_transform import (
    LICENSE_COLUMN_NAME_CLI_KEY,
    LICENSE_SELECT_PARAMS,
    LICENSES_FILE_CLI_KEY,
    LicenseSelectTransform,
    LicenseSelectTransformConfiguration,
)


class TestLicenseSelectTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    table = pa.table(
        {
            "document_id": ["ID_1", "ID_2"],
            "license": ["Apache-2.0", "BAD_LICENSE"],
        }
    )
    expected_table = pa.table(
        {
            "document_id": ["ID_1", "ID_2"],
            "license": ["Apache-2.0", "BAD_LICENSE"],
            "license_status": [True, False],
        }
    )
    expected_metadata_list = [{}, {}]

    def get_test_transform_fixtures(self) -> list[tuple]:
        test_src_dir = os.path.abspath(os.path.dirname(__file__))
        approved_license_file = os.path.abspath(
            os.path.join(test_src_dir, "../test-data/sample_approved_licenses.json")
        )
        cli = [
            # When running outside the Ray orchestrator and its DataAccess/Factory, there is
            # no Runtime class to load the domains and the Transform must do it itself using
            # the lang_select_local_config for this test.
            f"--{LICENSE_COLUMN_NAME_CLI_KEY}",
            "license",
            f"--{LICENSES_FILE_CLI_KEY}",
            approved_license_file,
        ]

        # Use the ProgLangMatchTransformConfiguration to compute the config parameters
        lstc = LicenseSelectTransformConfiguration()
        config = get_transform_config(lstc, cli)

        fixtures = [
            (
                LicenseSelectTransform(
                    {
                        LICENSE_SELECT_PARAMS: {
                            "license_column_name": "license",
                            "allow_no_license": False,
                            "licenses": ["Apache-2.0"],
                            "deny": False,
                        }
                    }
                ),
                [self.table],
                [self.expected_table],
                self.expected_metadata_list,
            ),
        ]
        return fixtures


if __name__ == "__main__":
    t = TestLicenseSelectTransform()
