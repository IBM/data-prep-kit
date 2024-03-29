import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from tokenization_transform import TokenizationTransformConfiguration


tkn_params = {
        "tkn_tokenizer": "hf-internal-testing/llama-tokenizer",
        "tkn_doc_id_column":"document_id",
        "tkn_doc_content_column":"contents",
        "tkn_text_lang": "en",
        "tkn_chunk_size":20_000,
        }
class TestRayTokenizationTransform(AbstractTransformLauncherTest):
    """
    This involves testing a tokenizer by tokenizing a lengthy document
    through segmenting it into chunks and then tokenizing each segment individually.
    By tokenizing the text in this manner and subsequently merging the tokenized chunks,
    it contributes to enhancing the overall runtime efficiency.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [(TokenizationTransformConfiguration(), tkn_params, basedir + "/ds02/input", basedir + "/ds02/expected")]
        return fixtures
