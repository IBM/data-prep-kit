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
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from tokenization_transform import TokenizationTransform


"""
input table:
"""
table = pa.Table.from_pydict(
    {
        "document_id": pa.array(["doc01", "doc02", "doc03"]),
        "contents": pa.array(["This content is for doc01", "", "Another document content is for doc03"]),
    }
)


# '''
# expected output table as per HF's `bigcode/starcoder` tokenizer:
# '''
# tokens = pa.array([[2272,1795,438,436,3693,34,35],[20976,1825,1795,438,436,3693,34,37]])
# document_id = pa.array(["doc01", "doc03"])
# document_length = pa.array([25,37])
# token_count = pa.array([7, 8])

"""
expected output table as per HF's `bigcode/starcoder` tokenizer:
"""
tokens = pa.array([[1, 910, 2793, 338, 363, 1574, 29900, 29896], [1, 7280, 1842, 2793, 338, 363, 1574, 29900, 29941]])
document_id = pa.array(["doc01", "doc03"])
document_length = pa.array([25, 37])
token_count = pa.array([8, 9])

schema = pa.schema(
    [
        ("tokens", pa.list_(pa.int64())),
        ("document_id", pa.string()),
        ("document_length", pa.int64()),
        ("token_count", pa.int64()),
    ]
)

expected_table = pa.Table.from_arrays([tokens, document_id, document_length, token_count], schema=schema)


"""
expected output metadata:
"""
expected_metadata_list = [
    {"num_files": 1, "num_rows": 3, "num_tokenized_rows": 2, "num_empty_rows": 1, "num_tokens": 17, "num_chars": 62},
    {},
]


"""
Config (parameter settings) for the run:
"""
config = {
    "tokenizer": "hf-internal-testing/llama-tokenizer",
    "doc_id_column": "document_id",
    "doc_content_column": "contents",
    "text_lang": "en",
    "chunk_size": 0,
}


class TestTokenizationTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:

        fixtures = [
            (TokenizationTransform(config), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
