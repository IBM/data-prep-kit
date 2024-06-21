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
from data_processing.test_support.transform import AbstractTableTransformTest
from data_processing.transform import get_transform_config
from proglang_select_transform import (
    ProgLangSelectTransform,
    ProgLangSelectTransformConfiguration,
    lang_allowed_langs_file_key,
    lang_lang_column_key,
    lang_output_column_key,
)


class TestProgLangSelectTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        test_src_dir = os.path.abspath(os.path.dirname(__file__))
        lang_allowed_file = os.path.abspath(
            os.path.join(test_src_dir, "../test-data/languages/allowed-code-languages.txt")
        )
        cli = [
            # When running outside the Ray orchestrator and its DataAccess/Factory, there is
            # no Runtime class to load the domains and the Transform must do it itself using
            # the lang_select_local_config for this test.
            f"--{lang_allowed_langs_file_key}",
            lang_allowed_file,
            f"--{lang_lang_column_key}",
            "language",
            f"--{lang_output_column_key}",
            "allowed",
        ]

        # Use the ProgLangMatchTransformConfiguration to compute the config parameters
        lstc = ProgLangSelectTransformConfiguration()
        config = get_transform_config(lstc, cli)

        fixtures = [
            (
                ProgLangSelectTransform(config),
                [self.input_df],
                [self.expected_output_df],
                self.expected_metadata_list,
            ),
        ]
        return fixtures

    # test data
    languages = pa.array(
        [
            "Cobol",
            "Java",
            "C",
        ]
    )
    names = ["language"]
    input_df = pa.Table.from_arrays([languages], names=names)

    outa = pa.array(
        [
            False,
            True,
            True,
        ],
    )
    expected_output_df = pa.Table.from_arrays([languages, outa], names=["language", "allowed"])
    expected_metadata_list = [
        {
            "documents with supported languages": 2,
            "documents with unsupported languages": 1,
        },  # transform() metadata
        {},  # Empty flush() metadata
    ]


if __name__ == "__main__":
    t = TestProgLangSelectTransform()
