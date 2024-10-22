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

import logging
import os
import uuid
from typing import Callable

import pandas as pd
import pyarrow as pa
from dpk_repo_level_order.internal.check_languages import (
    get_dominant_language_repo_packing,
)
from dpk_repo_level_order.internal.sorting.semantic_ordering import (
    check_and_update_title,
    sort_by_path,
    sort_sem,
)
from func_timeout.exceptions import FunctionTimedOut


SORT_BY_PATH = "SORT_BY_PATH"
SORT_SEMANTIC = "SORT_SEMANTIC"
SORT_SEMANTIC_NORMALISED = "SORT_SEMANTIC_NORMALISED"


def semantic_sort(
    df: pd.DataFrame, logger: logging.Logger, title_column_name: str, language_column_name: str
) -> pd.DataFrame:
    return sort_sem(
        files_df=df, logger=logger, title_column_name=title_column_name, language_column_name=language_column_name
    )


def semantic_sort_normalised(
    df: pd.DataFrame, logger: logging.Logger, title_column_name: str, language_column_name: str
) -> pd.DataFrame:
    check_and_update_title(df)
    return sort_sem(
        files_df=df, logger=logger, title_column_name=title_column_name, language_column_name=language_column_name
    )


def default_sort(
    df: pd.DataFrame, logger: logging.Logger, title_column_name: str, language_column_name: str
) -> pd.DataFrame:
    return sort_by_path(df=df, logger=logger, title_column_name=title_column_name)


def get_sorting_func(
    sorting_algo: str, title_column_name: str, logger: logging.Logger, language_column_name: str
) -> Callable[[pa.Table], pa.Table]:
    """Get a sorting function based on the specified algorithm.

    Args:
        sorting_algo (str): The sorting algorithm to use.
        title_column_name (str): The name of the column containing file
                                 titles.
        logger (logging.Logger): A logger object for logging messages.
        language_column_name (str): The name of the column containing file
                                    languages.

    Returns:
        Callable[[pa.Table, str], pa.Table]: A function that takes a PyArrow Table
                                        and a file name as input and
                                        returns a sorted PyArrow Table.
    """
    if sorting_algo == SORT_SEMANTIC:
        sort_by = semantic_sort
        logger.info("semantic sort enabled")
    elif sorting_algo == SORT_SEMANTIC_NORMALISED:
        sort_by = semantic_sort_normalised
        logger.info("normalised semantic sort enabled")
    else:
        sort_by = default_sort
        logger.info("sort by path enabled")

    def sorter(table: pa.Table, file_name: str) -> pa.Table:
        if table.num_rows < 2:
            logger.info(f"Not enough rows to sort for {file_name}. Skip sorting.")
            return table
        df = table.to_pandas()
        try:
            sorted_df = sort_by(
                df=df, logger=logger, title_column_name=title_column_name, language_column_name=language_column_name
            )
        except FunctionTimedOut as e:
            logger.error(
                f"Exception while sorting [{file_name}].\n Exception: {e.__class__.__name__}.\n Falling back to default sort_by_path"
            )
            sorted_df = sort_by_path(df=df, logger=logger, title_column_name=title_column_name)
        except Exception as e:
            logger.error(
                f"Exception while sorting [{file_name}].\n Exception: {e.__class__.__name__}.\n Falling back to default sort_by_path"
            )
            sorted_df = sort_by_path(df=df, logger=logger, title_column_name=title_column_name)
        return pa.Table.from_pandas(sorted_df, preserve_index=False)

    return sorter


def get_dominant_language_func(language_column_name: str, title_column_name: str) -> Callable[[pa.Table, str], str]:
    """
    This function takes two column names as input and returns a function
    that can be applied to a pyarrow table.
    The returned function determines the dominant programming language in
    the pyarrow table and returns the filename with the detected language
    prepended.

        Args:
            language_column_name (str): Name of the column containing the
    programming languages.
            title_column_name (str): Name of the column containing the file
    titles/paths.

        Returns:
            Callable[[pa.Table, str], str]: A function that takes a table as
    input and returns a new table with the filenames modified to include the
    detected dominant language.
    """

    def dominant_lang_per_repo(table: pa.Table, filename: str) -> str:
        """
        This function takes a table whose rows are documents from a repo
        and determines the most used/common programming language present in the
        table. It then returns the modified filename, prepended with
        the name of detected language.

        eg:  A table from file abc.parquet, with a dominant language C, will be returned
        as `C/abc.parquet`

        """
        lang_name = get_dominant_language_repo_packing(table, language_column_name, title_column_name)
        return os.path.join(lang_name, filename)

    return dominant_lang_per_repo


def superrow_table(table: pa.Table, repo_column_name: str, language_column_name="language") -> pa.Table:
    """
    This function combines all the rows of a parquet table into a single row
    and returns a modified table.
    """

    def lang_distribution(grouping_column):
        """returns the language distribution dictionary like: {'Markdown': 1, 'Tex': 1, 'unknown': 11}"""
        grouped = table.group_by(grouping_column)
        aggregated = grouped.aggregate([(grouping_column, "count")])
        lang_dist = {}
        for k, v in zip(aggregated[grouping_column], aggregated[f"{grouping_column}_count"]):
            lang_dist[k.as_py()] = v.as_py()
        return lang_dist

    super_row = table.column("contents").to_pylist()
    repo_doc_ids = table.column("document_id").to_pylist()
    lang_dist = lang_distribution(language_column_name)

    document_id = (str(uuid.uuid4()),)
    contents = ("".join(super_row),)
    repo_document_ids = (" ".join(repo_doc_ids),)

    names = [
        "document_id",
        "contents",
        "repo_document_ids",
        "repo_name",
        "lang_distribution",
    ]
    new_table = pa.table(
        [
            pa.array(document_id),
            pa.array(contents),
            pa.array(repo_document_ids),
            pa.array([repo_column_name]),
            pa.array([lang_dist]),
        ],
        names=names,
    )

    return new_table


def get_transforming_func(sorting_func=None, superrows_func=None, filename_func=None, language_column_name="language"):
    """
    This function takes three optional functions as input and returns a
    function that can be applied to a pyarrow table and file name.
    The returned function performs some transformation on the input table
    and file name based on the provided functions.

    Args:
        sorting_func (Callable[[pa.Table, str], pa.Table]): A function that sorts the
                     rows of a table based on a column. Defaults to None.
        superrows_func (Callable[[pa.Table, str, str], pa.Table]): A
                     function that creates new rows in a table based on the values of other
                     columns. Defaults to None.
        filename_func (Callable[[pa.Table, str], str]): A function that modifies the
                     file name. Defaults to None.
        language_column_name (str): The name of the column containing the
                     programming languages. Defaults to "language".

    Returns:
        callable: A function that takes a table and file name as input and
                     returns a list of transformed tables and file names.
    """

    def my_transform(table, file_name):
        out_table = table
        if sorting_func:
            out_table = sorting_func(table, file_name)
        if filename_func:
            file_name = filename_func(table, file_name)
        if superrows_func:
            out_table = superrows_func(out_table, file_name, language_column_name=language_column_name)
        return [
            (out_table, file_name),
        ]

    return my_transform
