# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.

import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
# from data_processing.test_support.transform import NOOPTransformConfiguration
from tokenization_transform import TokenizationTransformConfiguration


from test_tokenization import table,expected_table,expected_metadata_list,config
tkn_params = {
        # "tokenizer_path":local_tokenizer,
        # "tokenizer_path": "Rocketknight1/falcon-rw-1b", # HF's Falcon https://huggingface.co/docs/transformers/en/model_doc/falcon
        # "tokenizer_path": "EleutherAI/gpt-neox-20b", # https://huggingface.co/docs/transformers/en/model_doc/gpt_neox
        # "tokenizer_path": "hf-internal-testing/llama-tokenizer",
        "tkn_tokenizer_path": "bigcode/starcoder",
        "tkn_doc_id_column":"document_id",
        "tkn_doc_content_column":"contents",
        "tkn_text_lang": "en",
        "tkn_chunk_size":0,
        }
class TestRayTokenizationTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [(TokenizationTransformConfiguration(), tkn_params, basedir + "/input", basedir + "/expected")]
        return fixtures
