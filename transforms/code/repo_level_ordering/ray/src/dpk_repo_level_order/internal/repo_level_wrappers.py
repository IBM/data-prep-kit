import logging
import os
import uuid
from typing import Callable

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


def semantic_sort(df, logger, title_column_name, language_column_name):
    return sort_sem(
        files_df=df, logger=logger, title_column_name=title_column_name, language_column_name=language_column_name
    )


def semantic_sort_normalised(df, logger, title_column_name, language_column_name):
    check_and_update_title(df)
    return sort_sem(
        files_df=df, logger=logger, title_column_name=title_column_name, language_column_name=language_column_name
    )


def default_sort(df, logger, title_column_name, language_column_name):
    return sort_by_path(df=df, logger=logger, title_column_name=title_column_name)


def get_sorting_func(
    sorting_algo: str, title_column_name: str, logger: logging.Logger, language_column_name: str
) -> Callable[[pa.Table], pa.Table]:
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


def get_dominant_language_func(language_column_name, title_column_name):
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
