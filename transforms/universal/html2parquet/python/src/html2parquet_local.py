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
from html2parquet_transform import Html2ParquetTransform


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))

html2parquet_params = {}
if __name__ == "__main__":
    # Here we show how to run outside of the runtime
    # Create and configure the transform.
    transform = Html2ParquetTransform(html2parquet_params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    file_to_process = os.path.join(input_folder, "test1.html")
    byte_array, _ = data_access.get_file(file_to_process)
    print(f"input file: {file_to_process}")
    # Transform the table
    table_list, metadata = transform.transform_binary(file_name=file_to_process, byte_array=byte_array)
    # print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")
