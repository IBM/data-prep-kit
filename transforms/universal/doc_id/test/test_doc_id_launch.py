import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from doc_id_transform import (
    DocIDTransform,
    DocIDTransformConfiguration,
    doc_column_name_cli_param,
    doc_column_name_key,
    hash_column_name_cli_param,
    hash_column_name_key,
    int_column_name_cli_param,
    int_column_name_key,
)


class TestRayDocIDTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        fixtures = []
        config = {
            doc_column_name_cli_param: "contents",
            hash_column_name_cli_param: "doc_hash",
            int_column_name_cli_param: "doc_int",
        }
        fixtures.append((DocIDTransformConfiguration(), config, basedir + "/input", basedir + "/expected"))
        return fixtures
