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
from argparse import ArgumentParser, Namespace
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from data_processing.transform import AbstractTableTransform
from langdetect import detect
from pygments.lexers import get_all_lexers, guess_lexer
from pygments.util import ClassNotFound


class DatasetStatsProfiler(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, DatasetStatsProfilerRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of DatasetStatsProfilerConfiguration class
        super().__init__(config)
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Computes the dataset statistics
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")

        # Convert the pyarrow.Table to a pandas.DataFrame
        df = table.to_pandas()

        # Check if df is a Series, and convert it to a DataFrame if necessary
        if isinstance(df, pd.Series):
            df = df.to_frame()

        # Ensure df is a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected df to be a pandas DataFrame")

        # Convert all columns to string type safely, handling NaN and other types
        df = df.apply(
            lambda col: col.map(lambda x: str(x) if not pd.isna(x) else "") if isinstance(col, pd.Series) else col
        )

        # Print column names for debugging
        print("DataFrame columns:", df.columns)

        # 1. Rows and Unique Rows
        total_rows = len(df)
        unique_rows = len(df.drop_duplicates())

        # 2. Character Count
        character_counts = self.calculate_character_counts(df, self.config.get("input_code_col"))

        # 3. word Count
        word_counts = self.calculate_word_counts(df, self.config.get("input_code_col"))

        # 4. plang
        prog_langs = self.detect_programming_languages(
            df,
            self.config.get("input_code_col"),
            self.config.get("output_code_col"),
            self.config.get("code_language_col"),
        )
        # 5. Human langs
        human_langs = (
            self.detect_human_languages(df, self.config.get("instruction_col"))
            if self.config.get("instruction_col")
            else None
        )

        if self.config.get("datatype") == "EPT":
            row_alias = "Files"
        elif self.config.get("datatype") == "SFT":
            row_alias = "Instruction Pairs"

        metadata = {
            "Data model": "Dataset statistics",
            f"Number of {row_alias}": total_rows,
            f"Number of Unique {row_alias}": unique_rows,
            "Character count (Minimum)": character_counts["min_char_counts"],
            "Character count (Median)": character_counts["median_char_counts"],
            "Character count (Average/Mean)": character_counts["average_char_counts"],
            "Character count (Maximum)": character_counts["max_char_counts"],
            "Character count for percentiles": character_counts["char_len_dict"],
            "Word count (Minimum)": word_counts["min_word_counts"],
            "Word count (Median)": word_counts["median_word_counts"],
            "Word count (Average/Mean)": word_counts["average_word_counts"],
            "Word count (Maximum)": word_counts["max_word_counts"],
            "Number of programming Languages": prog_langs["num_total_unique_languages"],
            "Programming languages detected (name, count)": prog_langs["sorted_prog_lang_count"],
            "Size of code snippets per programming language (in KB)": prog_langs["language_code_sizes_list"],
        }
        if human_langs:
            metadata.update(
                {
                    f"Number of human languages detected in the {row_alias}": human_langs["num_human_languages"],
                    "Human languages detected (name, count)": human_langs["sorted_human_lang_count"],
                }
            )

        return [table], metadata

    def detect_human_languages(self, df: pd.DataFrame, instruction_col: str) -> dict[str, Any]:
        if instruction_col in df.columns:
            # 5. Human Language Detection
            def detect_language(text):
                try:
                    return detect(text)
                except:
                    return None

            languages = df["instruction_col"].apply(detect_language)
            unique_human_languages = languages.dropna().unique()
            num_human_languages = len(unique_human_languages)
            # Applying detection and creating a new column
            hlang_data = df["instruction_col"].apply(detect_language)
            # Counting occurrences of each language
            human_lang_counts = hlang_data.value_counts(dropna=False)
            sorted_human_lang_count = sorted(human_lang_counts.items(), key=lambda x: x[1], reverse=True)
            return {
                "num_human_languages": num_human_languages,
                "sorted_human_lang_count": sorted_human_lang_count,
            }

    def detect_programming_languages(
        self, df: pd.DataFrame, input_code_col: str, output_code_col: str, code_language_col: str
    ) -> dict[str, Any]:
        def detect_languages(text):
            # Check if the text is empty or NaN before proceeding
            if pd.isna(text) or text.strip() == "":
                return None
            try:
                lexer = guess_lexer(text)
                return lexer.aliases[0]
            except ClassNotFound:
                return None

        print(df.columns)

        if code_language_col in df.columns:
            prog_lang_count = {}
            for lang in df[code_language_col]:
                prog_lang_count[lang] = prog_lang_count.get(lang, 0) + 1

            filtered_prog_lang_count = {
                lang: count for lang, count in prog_lang_count.items() if lang is not None and lang != "unknown"
            }

            # Count unique values in the 'code_language' column
            detected_languages_in_input = df[code_language_col].value_counts()

            # Sort the counts
            sorted_prog_lang_count = sorted(filtered_prog_lang_count.items(), key=lambda x: x[1], reverse=True)

            num_total_unique_languages = len(filtered_prog_lang_count)
            all_unique_languages = set(detected_languages_in_input)
        else:
            detected_languages_in_input = df[input_code_col].apply(detect_languages)
            unique_languages_in_input = detected_languages_in_input.dropna().unique()
            prog_lang_count = {}
            for lang in detected_languages_in_input:
                prog_lang_count[lang] = prog_lang_count.get(lang, 0) + 1

            unique_languages_in_output = set()
            if code_language_col in df.columns:
                detected_languages_in_output = df[output_code_col].apply(detect_languages)
                unique_languages_in_output = detected_languages_in_output.dropna().unique()
                for lang in detected_languages_in_output:
                    prog_lang_count[lang] = prog_lang_count.get(lang, 0) + 1

            # Take the union of the unique languages from both 'input' and 'output'
            all_unique_languages = set(unique_languages_in_input).union(set(unique_languages_in_output))
            num_total_unique_languages = len(all_unique_languages)

        filtered_prog_lang_count = {
            lang: count for lang, count in prog_lang_count.items() if lang is not None and lang != "text"
        }
        sorted_prog_lang_count = sorted(filtered_prog_lang_count.items(), key=lambda x: x[1], reverse=True)

        def get_language(code_snippet, language):
            if language is not None:
                return language
            else:
                return detect_languages(code_snippet)

        detected_languages_and_sizes_input = df.apply(
            lambda row: (
                get_language(row[input_code_col], row.get(code_language_col)),
                len(row[input_code_col].encode("utf-8")),
            ),
            axis=1,
        )

        if output_code_col in df.columns:
            detected_languages_and_sizes_output = df.apply(
                lambda row: (
                    get_language(row[output_code_col], row.get(code_language_col)),
                    len(row[output_code_col].encode("utf-8")),
                ),
                axis=1,
            )
        else:
            detected_languages_and_sizes_output = pd.Series([], dtype=object)

        detected_languages_combined = pd.DataFrame(
            list(detected_languages_and_sizes_input) + list(detected_languages_and_sizes_output),
            columns=["detected_language", "code_size"],
        )

        language_code_sizes = detected_languages_combined.groupby("detected_language")["code_size"].sum().divide(1024)
        language_code_sizes_list = sorted(language_code_sizes.items(), key=lambda x: x[1], reverse=True)

        print(language_code_sizes_list)

        return {
            "num_total_unique_languages": num_total_unique_languages,
            "sorted_prog_lang_count": sorted_prog_lang_count,
            "language_code_sizes_list": language_code_sizes_list,
        }

    def calculate_word_counts(self, df: pd.DataFrame, column: str) -> dict[str, Any]:
        """
        Calculate word count statistics for a specific column in the DataFrame.
        """
        # Check if the column exists in the DataFrame
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame columns")

        # Ensure the column is treated as a Series
        column_series = df[column]
        if not isinstance(column_series, pd.Series):
            raise TypeError(f"Expected column '{column}' to be a pandas Series")

        # Calculate the word counts
        word_counts = column_series.apply(lambda s: len(str(s).split()))

        # Generate the percentile range from 0 to 100 with a 0.5 increment
        percentiles = np.arange(0, 1.005, 0.005)  # 1.005 to ensure 100% is included
        # Calculate the word lengths at each specified percentile
        word_len_percentiles = word_counts.quantile(percentiles)
        # Convert the percentiles Series to a dictionary with formatted keys
        word_len_dict = {f"{key*100:.1f}%": float(val) for key, val in word_len_percentiles.items()}

        # Clean up the statistics and convert to native Python types
        min_word_counts = int(word_counts.min())
        max_word_counts = int(word_counts.max())
        median_word_counts = float(word_counts.median())
        average_word_counts = round(float(word_counts.mean()), 2)

        return {
            "min_word_counts": min_word_counts,
            "max_word_counts": max_word_counts,
            "median_word_counts": median_word_counts,
            "average_word_counts": average_word_counts,
            "word_len_dict": word_len_dict,
        }

    def calculate_character_counts(self, df: pd.DataFrame, column: str) -> dict[str, Any]:
        """
        Calculate character count statistics for a specific column in the DataFrame.
        """
        # Check if the column exists in the DataFrame
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame columns")

        # Ensure the column is treated as a Series
        column_series = df[column]
        if not isinstance(column_series, pd.Series):
            raise TypeError(f"Expected column '{column}' to be a pandas Series")

        row_sums = column_series.str.len()

        # Generate the percentile range from 0 to 100 with a 0.5 increment
        percentiles = np.arange(0, 1.005, 0.005)  # 1.005 to ensure 100% is included
        # Calculate the character lengths at each specified percentile
        char_len_percentiles = row_sums.quantile(percentiles)
        # Convert the percentiles Series to a dictionary with formatted keys
        char_len_dict = {f"{key*100:.1f}%": float(val) for key, val in char_len_percentiles.items()}

        # Clean up the statistics and convert to native Python types
        min_char_counts = int(row_sums.min())
        max_char_counts = int(row_sums.max())
        median_char_counts = float(row_sums.median())
        average_char_counts = round(float(row_sums.mean()), 2)

        return {
            "min_char_counts": min_char_counts,
            "max_char_counts": max_char_counts,
            "median_char_counts": median_char_counts,
            "average_char_counts": average_char_counts,
            "char_len_dict": char_len_dict,
        }
