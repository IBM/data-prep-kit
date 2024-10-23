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
import os
import pyarrow.parquet as pq


from data_processing.data_access import DataAccessLocal
from code_profiler_transform import CodeProfilerTransform

# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "output"))

data_profiler_params = input_folder + "/data_profiler_params.json"

# Load the JSON object from the file and map it to a dictionary
with open(data_profiler_params, "r") as json_file:
    profiler_params = json.load(json_file)

# Print the loaded dictionary
print("Loaded dictionary:", profiler_params)

def save_tables_to_parquet(table_list, output_folder, base_filename):
    """
    Save each table in the table_list to individual parquet files in the specified output folder.
    """
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Iterate over the tables and save each one
    for idx, table in enumerate(table_list):
        # Construct the file path for each table
        output_file = os.path.join(output_folder, f"{base_filename}_part_{idx}.parquet")
        pq.write_table(table, output_file)
        print(f"Table {idx} saved to {output_file}")

if __name__ == "__main__":
    print("Code profiling started")
    # Here we show how to run outside of the runtime
    # Create and configure the transform.
    profiler = CodeProfilerTransform(profiler_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table, other_val = data_access.get_table(os.path.join(input_folder, profiler_params.get("input")))
    print(f"input table: {table}")
    print(f"other_val: {other_val}")

    # Transform the table
    table_list, metadata = profiler.transform(table)

    print(len(table_list))
    print(f"\noutput table has {table_list[0].num_rows} rows and {table_list[0].num_columns} columns")
    print(f"output metadata : {metadata}")

    save_tables_to_parquet(table_list, output_folder=output_folder, base_filename="uast_table")