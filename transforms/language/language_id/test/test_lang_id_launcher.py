# This helps to be able to run the test from within an IDE which seems to use the location of the
# file as the working directory.

import os

import pyarrow as pa
from data_processing.test_support.ray import AbstractTransformLauncherTest
from lang_id_implementation import (
    PARAM_CONTENT_COLUMN_NAME,
    PARAM_MODEL_CREDENTIAL,
    PARAM_MODEL_KIND,
    PARAM_MODEL_URL,
    LangIdentificationTableTransformConfiguration,
)
from lang_models import KIND_FASTTEXT


class TestLanguageIdentificationTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = [
            (
                LangIdentificationTableTransformConfiguration(),
                {
                    PARAM_MODEL_KIND: KIND_FASTTEXT,
                    PARAM_MODEL_CREDENTIAL: "YOUR HUGGING FACE ACCOUNT TOKEN",
                    PARAM_MODEL_URL: "facebook/fasttext-language-identification",
                    PARAM_CONTENT_COLUMN_NAME: "text",
                },
                basedir + "/input",
                basedir + "/expected",
            )
        ]
        return fixtures
