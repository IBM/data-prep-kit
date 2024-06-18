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

import ast
import os

from data_processing.data_access import DataAccessFactory, DataAccessLocal
from data_processing.utils import TransformUtils
from ingest_2_parquet_transform_ray import (
    IngestToParquetTransform,
    ingest_data_factory_key,
    ingest_detect_programming_lang_key,
    ingest_domain_key,
    ingest_snapshot_key,
    ingest_supported_langs_file_key,
)


supported_languages_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../test-data/languages/lang_extensions.json")
)
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))

params = {
    ingest_supported_langs_file_key: supported_languages_file,
    ingest_detect_programming_lang_key: True,
    ingest_snapshot_key: "github",
    ingest_domain_key: "code",
    "data_files_to_use": ast.literal_eval("['.zip']"),
    ingest_data_factory_key: DataAccessFactory(),  # Expect to create DataAccessLocal
}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = IngestToParquetTransform(params)
    # Use the local data access to read a parquet table.
    data_access = DataAccessLocal()
    file_to_process = os.path.join(input_folder, "application-java.zip")
    byte_array, _ = data_access.get_file(file_to_process)
    # Transform the table
    files_list, metadata = transform.transform_binary(file_name=file_to_process, byte_array=byte_array)
    print(f"Got {len(files_list)} output files")
    print(f"output metadata : {metadata}")
