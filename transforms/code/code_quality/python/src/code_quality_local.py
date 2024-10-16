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

from code_quality_transform import CodeQualityTransform
from data_processing.data_access import DataAccessLocal


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))

if __name__ == "__main__":
    codequality_params = {
        "code_quality_params": {
            "contents_column_name": "contents",
            "language_column_name": "language",
            "tokenizer": "codeparrot/codeparrot",
            "hf_token": None,
        }
    }
    transform = CodeQualityTransform(codequality_params)

    data_access = DataAccessLocal()
    table, _ = data_access.get_table(os.path.join(input_folder, "sample_1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
