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

import pyarrow as pa
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger
from dpk_repo_level_order.internal.repo_level_wrappers import (
    get_dominant_language_func,
    get_sorting_func,
)


class PassThroughTransform(AbstractTableTransform):
    """
    It returns table as it is without any change along with empty metadata.
    """

    def transform(self, table, file_name=None):
        return [table], {}


def compare_tables(input_table, output_table):
    return AbstractTableTransformTest().test_transform(
        PassThroughTransform({}), [input_table], [output_table], [{}, {}]
    )


logger = get_logger("test")


def test_sort_by_path():
    input_table = pa.Table.from_pydict(
        {
            "title": ["z.py", "a.py", "REAME.md"],
            "repo_name": ["abc", "abc", "abc"],
            "language": ["python", "python", "markdown"],
            "ext": [".py", ".py", ".md"],
            "document_id": ["1", "2", "3"],
        }
    )
    expected_table = pa.Table.from_pydict(
        {
            "title": ["REAME.md", "a.py", "z.py"],
            "repo_name": ["abc", "abc", "abc"],
            "language": ["markdown", "python", "python"],
            "ext": [".md", ".py", ".py"],
            "document_id": ["3", "2", "1"],
        }
    )
    sort_by_path_ = get_sorting_func(
        sorting_algo="SORT_BY_PATH", title_column_name="title", logger=logger, language_column_name="language"
    )
    table = sort_by_path_(input_table, "file")
    compare_tables(table, expected_table)


def test_semantic_sort_reverting_to_default():
    input_table = pa.Table.from_pydict(
        {
            "title": ["z.py", "a.py", "REAME.md"],
            "repo_name": ["abc", "abc", "abc"],
            "language": ["python", "python", "markdown"],
            "ext": [".py", ".py", ".md"],
            "document_id": ["1", "2", "3"],
            "contents": ["import os", "import sys\n import z", "Hello"],
        }
    )
    expected_table = pa.Table.from_pydict(
        {
            "title": ["REAME.md", "a.py", "z.py"],
            "repo_name": ["abc", "abc", "abc"],
            "language": ["markdown", "python", "python"],
            "ext": [".md", ".py", ".py"],
            "document_id": ["3", "2", "1"],
            "contents": ["Hello", "import sys\n import z", "import os"],
        }
    )
    sort_by = get_sorting_func(
        sorting_algo="SORT_SEMANTIC", title_column_name="title", logger=logger, language_column_name="language"
    )
    table = sort_by(input_table, "file")
    compare_tables(table, expected_table)


def test_skip_sorting():
    input_table = pa.Table.from_pydict(
        {
            "title": ["z.py"],
            "document_id": ["1"],
        }
    )
    expected_table = pa.Table.from_pydict(
        {
            "title": ["z.py"],
            "document_id": ["1"],
        }
    )
    sort_by = get_sorting_func(
        sorting_algo="SORT_BY_PATH", title_column_name="title", logger=logger, language_column_name="language"
    )
    table = sort_by(input_table, "file")
    compare_tables(table, expected_table)


def test_detect_language():
    input_table = pa.Table.from_pydict(
        {
            "title": ["z.py", "a.py", "REAME.md"],
            "repo_name": ["abc", "abc", "abc"],
            "language": ["python", "python", "markdown"],
            "ext": [".py", ".py", ".md"],
            "document_id": ["1", "2", "3"],
        }
    )
    detect_lang = get_dominant_language_func(title_column_name="title", language_column_name="language")
    path = detect_lang(input_table, "file")
    assert path == "python/file"
