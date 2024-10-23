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

import time
from logging import Logger

import pandas as pd
from dpk_repo_level_order.internal.sorting.semantic_ordering.build_dep_graph import (
    build_edges,
)
from dpk_repo_level_order.internal.sorting.semantic_ordering.topological_sort import (
    topological_sort_on_df,
)
from dpk_repo_level_order.internal.sorting.semantic_ordering.utils import sort_by_path
from func_timeout import func_set_timeout


def check_and_update_title(df: pd.DataFrame, title_column="title"):
    https_str = "https://"
    len_str = len(https_str)
    new_prefix = "HTTPS_PREFIX/"
    replace_title = False

    try:
        # check if title contains https_str in the beginning
        if (len(df[title_column][0]) > len_str) and (df[title_column][0][:len_str] == https_str):
            replace_title = True
            df["orig_title"] = df[title_column]
            df[title_column] = df[title_column].apply(lambda x: x.replace(https_str, new_prefix, 1))
    except:
        pass
    return replace_title


supported_exts = {
    ".py",
    ".c",
    ".cpp",
    ".java",
    ".go",
    ".h",
    ".groovy",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".swift",
    ".kt",
    ".m",
    ".rb",
}


@func_set_timeout(1800)
def sort_sem(files_df: pd.DataFrame, logger: Logger, title_column_name="new_title", language_column_name="language"):
    received_shape = files_df.shape
    supported_bools = files_df.ext.isin(supported_exts)

    if supported_bools.any():
        logger.info("Proceeding with semantic sorting")

        start_time = time.time()
        dep_graph = build_edges(files_df, logger, title_column_name, language_column_name)
        graph_time = round(time.time() - start_time, 2)
        logger.info(f"sort_sem: time taken build_edges - {graph_time}")

        # Dependency based Topological Sort
        start_time = time.time()
        files_df_sorted = topological_sort_on_df(dep_graph, files_df, logger, title_column_name)
        sort_time = round(time.time() - start_time, 2)
        logger.info(f"sort_sem: time taken topological sort - {sort_time}")

    else:
        logger.info("Proceeding with directory structure based sorting")
        # Default Sort
        files_df_sorted = sort_by_path(files_df, logger, title_column_name=title_column_name)

    transformed_shape = files_df_sorted.shape
    assert (
        received_shape == transformed_shape
    ), f"Recieved Shape - {received_shape} not matching Transformed Shape - {transformed_shape}"
    return files_df_sorted
