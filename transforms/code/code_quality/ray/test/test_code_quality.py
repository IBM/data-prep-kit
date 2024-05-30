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

import pyarrow.parquet as pq
from code_quality_transform import (
    CodeQualityTransform,
    CodeQualityTransformConfiguration,
)
from data_processing.test_support.transform import AbstractTableTransformTest
from data_processing.transform import get_transform_config


class TestCodeQualityTransform(AbstractTableTransformTest):
    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            "--cq_contents_column_name",
            "contents",
            "--cq_language_column_name",
            "language",
            "--cq_tokenizer",
            "codeparrot/codeparrot",
        ]

        # Use the CodeQualityTransformConfiguration to compute the config parameters
        cqconfig = CodeQualityTransformConfiguration()
        config = get_transform_config(cqconfig, cli)

        fixtures = [
            (
                CodeQualityTransform(config),
                [self.input_table],
                [self.expected_output_table],
                [{}, {}],
            ),
        ]
        return fixtures

    test_src_dir = os.path.abspath(os.path.dirname(__file__))
    input_file = os.path.abspath(os.path.join(test_src_dir, "../test-data/input/sample_1.parquet"))
    input_table = pq.ParquetFile(input_file).read()
    expected_file = os.path.abspath(os.path.join(test_src_dir, "../test-data/expected/sample_1.parquet"))
    expected_output_table = pq.ParquetFile(expected_file).read()
