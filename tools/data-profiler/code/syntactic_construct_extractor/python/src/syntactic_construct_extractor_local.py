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

from data_processing.data_access import DataAccessLocal
from syntactic_construct_extractor_profiler import SyntacticConstructExtractorProfiler


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "../", "../", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "../", "../", "output"))

data_profiler_params = input_folder + "/data_profiler_params.json"

# Load the JSON object from the file and map it to a dictionary
with open(data_profiler_params, "r") as json_file:
    profiler_params = json.load(json_file)

# Print the loaded dictionary
print("Loaded dictionary:", profiler_params)

if __name__ == "__main__":
    print("Syntactic constructs extraction started")
    # Here we show how to run outside of the runtime
    # Create and configure the transform.
    profiler = SyntacticConstructExtractorProfiler(profiler_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    table, other_val = data_access.get_table(os.path.join(input_folder, profiler_params.get("input")))
    print(f"input table: {table}")

    # Transform the table
    table_list, metadata = profiler.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")

    # Path to the JSON file where metadata will be saved
    syntactic_constructs = f"{output_folder}/syntactic_constructs.json"
    # Write the metadata dictionary to a JSON file
    with open(syntactic_constructs, "w") as json_file:
        json.dump(metadata, json_file, indent=4)
    print(f"Metadata has been written to {syntactic_constructs}")

    # Path to the JSON file where metadata will be saved
    aggregated_profiler = os.path.join(output_folder, "aggregated_profiler.json")

    # Load existing data
    if os.path.exists(aggregated_profiler):
        with open(aggregated_profiler, "r") as json_file:
            try:
                existing_data = json.load(json_file)
            except json.JSONDecodeError:
                existing_data = {}
    else:
        existing_data = {}

    # Update the metadata
    existing_data["SemanticConstructExtractor"] = metadata

    # Write the updated data to the JSON file
    with open(aggregated_profiler, "w") as json_file:
        json.dump(existing_data, json_file, indent=4)

    print(f"Metadata has been written to {aggregated_profiler}")
