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
from doc_id_transform_ray import DocIDTransform


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))

doc_id_params = {"doc_column": "contents", "hash_column": "doc_hash"}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = DocIDTransform(doc_id_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table, _ = data_access.get_table(os.path.join(input_folder, "sample1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    table = table_list[0]
    print(f"\noutput table: {table}")
    print(f"output metadata : {metadata}")
    column = table["doc_hash"]
    print(f"hashed column : {column}")
    print(f"index column : {column}")
