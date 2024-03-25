import os

import pyarrow as pa
from cq_transform import CodeQualityTransformConfiguration
from data_processing.test_support.ray import AbstractTransformLauncherTest


class TestCodeQualityTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = {
            "cq_contents_column_name": "contents",
            "cq_language_column_name": "language",
            "cq_tokenizer": "codeparrot/codeparrot",
        }
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [(CodeQualityTransformConfiguration(), cli, basedir + "/input", basedir + "/expected")]
        return fixtures
