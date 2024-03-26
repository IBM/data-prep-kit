from typing import Tuple

import pyarrow as pa
from data_processing.test_support.transform.transform_test import AbstractTransformTest
from docquality_transform import DocQualityTransform


"""
input table:
"""
table = pa.Table.from_pydict(
    {"document_id": pa.array(["doc01"]), "contents": pa.array([" : This documents is for test . ? "])}
)


"""
expected output table:
"""
# copy expected_table from the input_df
schema = table.schema
data = table.to_pydict()
expected_table = pa.Table.from_pydict(data, schema=schema)

expected_table = expected_table.append_column("docq_total_words", pa.array([8]))
expected_table = expected_table.append_column("docq_mean_word_len", pa.array([3.125]))
expected_table = expected_table.append_column("docq_symbol_to_word_ratio", pa.array([0.0]))
expected_table = expected_table.append_column("docq_sentence_count", pa.array([1]))
expected_table = expected_table.append_column("docq_lorem_ipsum_ratio", pa.array([0.0]))
expected_table = expected_table.append_column("docq_curly_bracket_ratio", pa.array([0.0]))
expected_table = expected_table.append_column("docq_contain_bad_word", pa.array([False]))
expected_table = expected_table.append_column("docq_bullet_point_ratio", pa.array([0.0]))
expected_table = expected_table.append_column("docq_ellipsis_line_ratio", pa.array([0.0]))
expected_table = expected_table.append_column("docq_alphabet_word_ratio", pa.array([0.625000]))
expected_table = expected_table.append_column("docq_contain_common_en_words", pa.array([False]))
expected_table = expected_table.append_column("metakenlm_docq_perplex_score", pa.array([6709.1]))

expected_metadata_list = [{"total_docs_count": 1}, {}]


"""
Config (parameter settings) for the run:
"""
config = {
    "ft_lang": "en",
    "bad_word_filepath": "../test-data/docq/ldnoobw/",
    "model_dir": "../lm_sp/",
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
                [table],
                [expected_table],
                expected_metadata_list,
            ),
        ]
        return fixtures


if __name__ == "__main__":
    t = TestDocQualityTransform()
