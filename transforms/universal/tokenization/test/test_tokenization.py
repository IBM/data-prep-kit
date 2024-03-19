from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from tokenization_transform import TokenizationTransform


'''
input table:
'''
table = pa.Table.from_pydict({"document_id": pa.array(["doc01","doc02","doc03"]),
                              "contents": pa.array(["This content is for doc01","","Another content for doc03"])})


'''
expected output table as per `bigcode/starcoder` tokenizer:
'''
tokens = pa.array([[2272,1795,438,436,3693,34,35],[20976,1795,436,3693,34,37]])
document_id = pa.array(["doc01", "doc03"])
token_count = pa.array([7, 6])

schema = pa.schema([("tokens", pa.list_(pa.int64())),
                    ("document_id", pa.string()),
                   ("token_count", pa.int64()),
                   ])

expected_table = pa.Table.from_arrays([tokens,document_id,token_count], schema=schema)


'''
expected output metadata:
'''
expected_metadata_list = [
    {'num_files': 1, 'num_rows': 3, 'num_tokenized_rows': 2, 'num_empty/failed_rows': 1},
    {}]


'''
Config (parameter settings) for the run:
'''
config = {
            "tokenizer_path": "bigcode/starcoder",
            # "tokenizer_path": "Rocketknight1/falcon-rw-1b", # HF's Falcon https://huggingface.co/docs/transformers/en/model_doc/falcon
            # "tokenizer_path": "EleutherAI/gpt-neox-20b", # https://huggingface.co/docs/transformers/en/model_doc/gpt_neox
            # "tokenizer_path": "hf-internal-testing/llama-tokenizer",
            "doc_id_column":"document_id",
            "doc_content_column":"contents",
            "text_lang": "en",
            "chunk_size":0,
            }

class TestTokenizationTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:

        fixtures = [
            (TokenizationTransform(config), [table], [expected_table], expected_metadata_list),
        ]
        return fixtures
