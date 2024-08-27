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
from data_processing.test_support.transform import AbstractTableTransformTest
from data_processing.utils import TransformUtils
from doc_id_transform_python import DocIDTransform
from doc_id_transform_base import (IDGenerator,
                                   doc_column_name_key,
                                   hash_column_name_key,
                                   int_column_name_key,
                                   id_generator_key,
                                   )



table = pa.Table.from_pydict(
    {
        "doc": pa.array(["Tom", "Joe"]),
    }
)
expected_table = pa.Table.from_pydict(
    {
        "doc": pa.array(["Tom", "Joe"]),
        "doc_hash": pa.array([TransformUtils.str_to_hash("Tom"), TransformUtils.str_to_hash("Joe")]),
        "doc_int": pa.array([5, 6]),
    }
)
expected_metadata_list = [{}, {}]  # transform() result  # flush() result


class TestDocIDTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        fixtures = []
        config = {
            doc_column_name_key: "doc",
            hash_column_name_key: "doc_hash",
            int_column_name_key: "doc_int",
            id_generator_key: IDGenerator(5),
        }
        fixtures.append((DocIDTransform(config), [table], [expected_table], expected_metadata_list))
        return fixtures
