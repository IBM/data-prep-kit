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
from profiler_transform_base import DataAggregator
from profiler_transform_python import ProfilerTransform
from profiler_transform_base import doc_column_name_key
from data_processing.data_access import DataAccessFactory


class TestProfilerTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        input_dir = os.path.join(basedir, "input")
        input_tables = get_tables_in_folder(input_dir)
        expected_metadata_list = [{}, {}]
        config = {doc_column_name_key: "contents",
                  "aggregator": DataAggregator({"data_access_factory": DataAccessFactory()})}
        expected_tables = []
        return [
            (ProfilerTransform(config), input_tables, expected_tables, expected_metadata_list),
        ]
