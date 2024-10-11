# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os

import pyarrow as pa
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from data_processing.transform import get_transform_config
from doc_quality_transform import DocQualityTransform, DocQualityTransformConfiguration


class TestDocQualityTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        cli = [
            "--docq_text_lang",
            "en",
            "--docq_doc_content_column",
            "contents",
            "--docq_bad_word_filepath",
            os.path.join(basedir, "ldnoobw", "en"),
        ]
        transformConfig = DocQualityTransformConfiguration()
        config = get_transform_config(transformConfig, cli)
        table = pa.Table.from_arrays(
            [
                pa.array(["doc01"]),
                pa.array([" : This documents is for test . ? "]),
            ],
            names=["document_id", "contents"],
        )
        expected_table = pa.Table.from_arrays(
            [
                pa.array(["doc01"]),
                pa.array([" : This documents is for test . ? "]),
                pa.array([8]),
                pa.array([3.125]),
                pa.array([0.0]),
                pa.array([1]),
                pa.array([0.0]),
                pa.array([0.0]),
                pa.array([False]),
                pa.array([0.0]),
                pa.array([0.0]),
                pa.array([0.625000]),
                pa.array([False]),
            ],
            names=[
                "document_id",
                "contents",
                "docq_total_words",
                "docq_mean_word_len",
                "docq_symbol_to_word_ratio",
                "docq_sentence_count",
                "docq_lorem_ipsum_ratio",
                "docq_curly_bracket_ratio",
                "docq_contain_bad_word",
                "docq_bullet_point_ratio",
                "docq_ellipsis_line_ratio",
                "docq_alphabet_word_ratio",
                "docq_contain_common_en_words",
            ],
        )
        return [
            (
                DocQualityTransform(config),
                [table],
                [expected_table],
                [{"total_docs_count": 1}, {}],
            )
        ]
