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

import json

import pyarrow as pa
from data_processing.data_access import DataAccessLocal


"""
input table:
"""
# Define the arrays for each column
document_id = pa.array(["doc01"])
contents = pa.array([" : This documents is for test . ? "])

# Define the column names
names = ["document_id", "contents"]

# Create the PyArrow table from the arrays
table = pa.Table.from_arrays([document_id, contents], names=names)

"""
expected output table:
"""
docq_total_words = pa.array([8])
docq_mean_word_len = pa.array([3.125])
docq_symbol_to_word_ratio = pa.array([0.0])
docq_sentence_count = pa.array([1])
docq_lorem_ipsum_ratio = pa.array([0.0])
docq_curly_bracket_ratio = pa.array([0.0])
docq_contain_bad_word = pa.array([False])
docq_bullet_point_ratio = pa.array([0.0])
docq_ellipsis_line_ratio = pa.array([0.0])
docq_alphabet_word_ratio = pa.array([0.625000])
docq_contain_common_en_words = pa.array([False])
metakenlm_docq_perplex_score = pa.array([6709.1])

expected_column_names = [
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
    "metakenlm_docq_perplex_score",
]

expected_table = pa.Table.from_arrays(
    [
        document_id,
        contents,
        docq_total_words,
        docq_mean_word_len,
        docq_symbol_to_word_ratio,
        docq_sentence_count,
        docq_lorem_ipsum_ratio,
        docq_curly_bracket_ratio,
        docq_contain_bad_word,
        docq_bullet_point_ratio,
        docq_ellipsis_line_ratio,
        docq_alphabet_word_ratio,
        docq_contain_common_en_words,
        metakenlm_docq_perplex_score,
    ],
    names=expected_column_names,
)


expected_metadata_list = [{"total_docs_count": 1}, {}]


# This main() is run to generate the test data file whenever the test data defined in TestDocQualityTransform changes.
# It generates the test and out put data into the input and expected directories.
if __name__ == "__main__":
    config = {"input_folder": "../test-data", "output_folder": "../test-data"}
    data_access = DataAccessLocal(config, [], False, -1)
    inp = table
    out = expected_table
    data_access.save_table("../test-data/input/test1.parquet", inp)
    data_access.save_table("../test-data/expected/test1.parquet", out)

    with open("../test-data/expected/metadata.json", "w") as json_file:
        json.dump(expected_metadata_list, json_file)
