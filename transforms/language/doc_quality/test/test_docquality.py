from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from doc_quality_transform import DocQualityTransform


"""
Config (parameter settings) for the run:
"""
config = {
    "ft_lang": "en",
    "bad_word_filepath": "~/Desktop/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/test-data/docq/ldnoobw/",
    "MODEL_DIR": "../lm_sp/",
}


class TestDocQualityTransform(AbstractTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:

        fixtures = [
            (
                DocQualityTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures


if __name__ == "__main__":
    t = TestDocQualityTransform()
