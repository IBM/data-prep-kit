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
from typing import Tuple

from data_processing.test_support import get_tables_in_folder
from data_processing.test_support.transform import AbstractTableTransformTest
from resize_transform import ResizeTransform


class TestResizeTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = []

        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ray/test-data"))
        input_dir = os.path.join(basedir, "input")
        input_tables = get_tables_in_folder(input_dir)
        expected_metadata_list = [{}] * (1 + len(input_tables))

        config = {"max_rows_per_table": 300}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-rows-300"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        config = {"max_rows_per_table": 125}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-rows-125"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        config = {"max_mbytes_per_table": 0.05}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-mbytes-0.05"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        config = {"max_mbytes_per_table": 1}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-mbytes-1"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        # # Merge the 1st 2 and some of the 2nd with the 3rd
        config = {"max_mbytes_per_table": 0.05}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-mbytes-0.05"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        # Split into 4 or so files
        config = {"max_mbytes_per_table": 0.02}
        expected_tables = get_tables_in_folder(os.path.join(basedir, "expected-mbytes-0.02"))
        fixtures.append((ResizeTransform(config), input_tables, expected_tables, expected_metadata_list))

        return fixtures
