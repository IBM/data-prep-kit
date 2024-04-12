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

from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.noop_transform import NOOPTransform
from data_processing.test_support.transform.transform_test import AbstractTransformTest


table = pa.Table.from_pydict({"name": pa.array(["Tom", "Dick", "Harry"]), "age": pa.array([0, 1, 2])})
expected_table = table  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 3}, {}]  # transform() result  # flush() result


class TestNOOPTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = [
            (NOOPTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
            (NOOPTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
