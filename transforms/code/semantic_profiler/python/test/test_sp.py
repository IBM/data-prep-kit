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
from data_processing.test_support import get_tables_in_folder
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from sp_transform import SemanticProfilerTransform, sleep_key, null_libs_file


table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table  # We're a sp after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestSemanticProfilerTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        src_file_dir = os.path.abspath(os.path.dirname(__file__))
        input_dir = os.path.join(src_file_dir, "../test-data/input")
        expected_dir = os.path.join(src_file_dir, "../test-data/expected")
        input_tables = get_tables_in_folder(input_dir)
        expected_tables = get_tables_in_folder(expected_dir)

        existing_column = 'Category' 
        for i in range(len(expected_tables)):
            existing_values = expected_tables[i][existing_column].to_pylist()
            new_column_name = "Concepts"
            new_column = pa.array(existing_values)
            expected_tables[i] = expected_tables[i].append_column(new_column_name, new_column)

        expected_metadata_list = [{"nfiles": 1, "nrows": len(expected_tables[0])}, {}]
        config = {sleep_key: 0, null_libs_file:"ikb/ikb_model.csv"}
        fixtures = [
            (SemanticProfilerTransform(config), input_tables, expected_tables, expected_metadata_list),
        ]
        return fixtures
