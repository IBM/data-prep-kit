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

from data_processing.data_access import DataAccessLocal
from doc_quality_transform import (
    DocQualityTransform,
    bad_word_filepath_key,
    doc_content_column_key,
    text_lang_key,
)


# create parameters
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
input_folder = os.path.join(basedir, "test-data", "input")
doc_quality_params = {
    text_lang_key: "en",
    doc_content_column_key: "contents",
    bad_word_filepath_key: os.path.join(basedir, "ldnoobw", "en"),
}
if __name__ == "__main__":
    # Here we show how to run outside of the runtime
    # Create and configure the transform.
    transform = DocQualityTransform(doc_quality_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table, _ = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")
