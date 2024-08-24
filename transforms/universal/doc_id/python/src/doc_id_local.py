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
from doc_id_transform_python import DocIDTransform
from doc_id_transform_base import (IDGenerator,
                                   doc_column_name_key,
                                   hash_column_name_key,
                                   int_column_name_key,
                                   id_generator_key,
                                   )

# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}

doc_id_params = {doc_column_name_key: "contents",
                 hash_column_name_key: "hash_column",
                 int_column_name_key: "int_id_column",
                 id_generator_key: IDGenerator(5),
                 }
doc_column_name_key = "doc_column"
hash_column_name_key = "hash_column"
int_column_name_key = "int_column"
start_id_key = "start_id"

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Filter transform needs a DataAccess to ready the domain list.
    data_access = DataAccessLocal(local_conf)
    # Create and configure the transform.
    transform = DocIDTransform(doc_id_params)
    # Use the local data access to read a parquet table.
    table, _ = data_access.get_table(os.path.join(input_folder, "sample1.parquet"))
    print(f"input table has {table.num_rows} rows")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table has {table_list[0].num_rows} rows")
    print(f"output metadata : {metadata}")
