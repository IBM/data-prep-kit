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
import sys
import time
from logging import Logger

import colorlog
import pandas as pd


exts = [
    ".rst",
    ".rest",
    ".rest.txt",
    ".rst.txt",
    ".tex",
    ".aux",
    ".bbx",
    ".bib",
    ".cbx",
    ".dtx",
    ".ins",
    ".lbx",
    ".ltx",
    ".mkii",
    ".mkiv",
    ".mkvi",
    ".sty",
    ".toc",
    ".md",
    ".markdown",
    ".mkd",
    ".mkdn",
    ".mkdown",
    ".ron",
    ".txt",
]


def sort_by_path(
    df: pd.DataFrame,
    logger: Logger,
    split_by_filetype=False,
    extensions_column_name="ext",
    title_column_name="new_title",
):
    readme = []
    otherext = []
    remaining = []
    default_sort_start_time = time.time()
    original_df_cp = df.copy(deep=True)
    try:
        input_row_count = len(df)
        if input_row_count > 0:
            df["filenames"] = df.apply(lambda x: x[title_column_name].split("/")[-1], axis=1)
            df["directories"] = df.apply(
                lambda x: x[title_column_name].replace(os.path.basename(x[title_column_name]), ""),
                axis=1,
            )

            df = df.sort_values(by=["directories", "filenames"])
            grouped_df = df.groupby("directories")

            def sort_group(group):
                is_readme_file = group["filenames"].str.lower().str.contains("readme")
                readme_files = group[is_readme_file]
                readme.append(readme_files)

                other_files = group[~is_readme_file]

                otherextn_bool = other_files[extensions_column_name].str.lower().isin(exts)
                other_files_with_ext = other_files[otherextn_bool]
                otherext.append(other_files_with_ext)

                remaining_files = other_files[~otherextn_bool]
                remaining.append(remaining_files)

                return pd.concat([readme_files, other_files_with_ext, remaining_files])

            sorted_df = grouped_df.apply(sort_group)

            if split_by_filetype:
                df1 = pd.concat(readme + otherext, axis=0).reset_index(drop=True)
                df1 = df1.drop(columns=["filenames", "directories"]).reset_index(drop=True)

                df2 = pd.concat(remaining, axis=0).reset_index(drop=True)
                df2 = df2.drop(columns=["filenames", "directories"]).reset_index(drop=True)

                output_row_count = len(df1) + len(df2)

                if input_row_count != output_row_count:
                    raise Exception(
                        f"Default sorting failed for ${df.iloc[0]['repo_name']}: no of rows dropped ${input_row_count-output_row_count}"
                    )
                logger.info(
                    f"Default sorting completed for repo {df.iloc[0]['repo_name']} in {time.time() - default_sort_start_time}"
                )

                return df1, df2
            else:
                output_row_count = len(sorted_df)
                sorted_df = sorted_df.drop(columns=["filenames", "directories"]).reset_index(drop=True)

                if input_row_count != output_row_count:
                    raise Exception(
                        f"Default sorting failed for ${df.iloc[0]['repo_name']}: no of rows dropped ${input_row_count-output_row_count}"
                    )
                logger.info(
                    f"Default sorting completed for repo {df.iloc[0]['repo_name']} in {time.time() - default_sort_start_time}"
                )
                return sorted_df
        else:
            return original_df_cp

    except Exception:
        logger.exception("Error while default sorting")
        if split_by_filetype:
            return original_df_cp, None
        else:
            return original_df_cp
