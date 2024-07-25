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


SORT_BY_PATH = "SORT_BY_PATH"
SORT_SEMANTIC = "SORT_SEMANTIC"
SORT_SEMANTIC_NORMALISED = "SORT_SEMANTIC_NORMALISED"


def semantic_sort(df, logger, title_column_name):
    return sort_sem(files_df=df, logger=logger, title_column_name=title_column_name)


def semantic_sort_normalised(df, logger, title_column_name):
    check_and_update_title(df)
    return sort_sem(df, logger, title_column_name)


def get_sorting_func(
    sorting_algo: str, title_column_name: str, logger: logging.Logger
) -> Callable[[pa.Table], pa.Table]:
    if sorting_algo == SORT_SEMANTIC:
        sort_by = semantic_sort
        logger.info("semantic sort enabled")
    elif sorting_algo == SORT_SEMANTIC_NORMALISED:
        sort_by = semantic_sort_normalised
        logger.info("normalised semantic sort enabled")
    else:
        sort_by = sort_by_path
        logger.info(f"sort by path enabled")

    def sorter(table: pa.Table) -> pa.Table:
        df = table.to_pandas()
        sorted_df = sort_by(df=df, logger=logger, title_column_name=title_column_name)
        return pa.Table.from_pandas(sorted_df, preserve_index=False)

    return sorter


def dominant_lang_per_repo(table: pa.Table, filename: str) -> str:
    """
    This function takes a table whose rows are documents from a repo
    and determines the most used/common programming language present in the
    table. It then returns the modified filename, prepended with
    the name of detected language.

    eg:  A table from file abc.parquet, with a dominant language C, will be returned
    as `C/abc.parquet`

    """
    lang_name = get_dominant_language_repo_packing(table)
    if lang_name.lower in ["c#"]:
        lang_name.replace("#", "-sharp")
    return os.path.join(lang_name, filename)


def dominant_lang_detector(table, file_name):
    import random

    folder = random.choice(["Java", "Python", "Go"])
    return os.path.join(folder, file_name)


def superrow_table(table: pa.Table, repo_column_name: str) -> pa.Table:
    """
    This function combines all the rows of a parquet table into a single row
    and returns a modified table.
    """
    table_df = table.to_pandas()
    super_row = table_df["contents"].tolist()
    repo_doc_ids = table_df["document_id"].tolist()
    lang_dist = table_df["language"].value_counts().to_dict()

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


def get_transforming_func(sorting_func=None, superrows_func=None, filename_func=None):
    def my_transform(table, file_name):
        out_table = table
        if sorting_func:
            out_table = sorting_func(table)
        if filename_func:
            file_name = filename_func(table, file_name)
        if superrows_func:
            out_table = superrows_func(out_table, file_name)
        return [
            (out_table, file_name),
        ]

    return my_transform
