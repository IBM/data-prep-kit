from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from tokenization_transform import TokenizationTransform, local_tokenizer


'''
input table:
'''
table = pa.Table.from_pydict({"document_id": pa.array(["doc01","doc02","doc03"]),
                                "contents": pa.array(["Content for doc01","","doc03 contents"])})

'''
expected output table:
'''
data = [
    [[10343, 317, 21001, 3752], [18866, 4840, 6130]]
]
tokens_array = pa.array(data)
schema = pa.schema([("tokens", pa.list_(pa.list_(pa.int64())))])
expected_table = pa.Table.from_arrays([tokens_array], schema=schema)

'''
expected output metadata:
'''
expected_metadata_list = [ {'num_rows': '3', 'num_tokenized_rows': '2', 'num_empty/failed_rows': '1', 'token_count_per_doc': [['doc01', 4], ['doc03', 3]]}
, {}]  # transform() result  # flush() result


class TestTokenizationTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        config = {"tokenizer_path": local_tokenizer,
                  "doc_id_column": "document_id",
                  "doc_content_column": "contents",
                  "sleep": 0,
                  }
        fixtures = [
            (TokenizationTransform(config), [table], [expected_table], expected_metadata_list),
            (TokenizationTransform(config), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
