import os

from data_processing.test_support.ray import AbstractTransformLauncherTest
from data_processing.utils import ParamsUtils
from doc_quality_transform import DocQualityTransformConfiguration


docq_params = {
    "ft_lang": "en",
    "bad_word_filepath": "../test-data/docq/ldnoobw/",
    "MODEL_DIR": "../lm_sp/",
}


class TestRayDocQualityTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))

        fixtures = [(DocQualityTransformConfiguration(), docq_params, basedir + "/input", basedir + "/expected")]
        return fixtures
