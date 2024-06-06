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
import pyarrow.parquet as pq
from data_processing.data_access import DataAccessLocal
from license_copyright_removal_transform import LicenseCopyrightRemoveTransform


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),'../test-data/expected'))

if __name__ == "__main__":
    license_copyright_removal_pram = {
        "license_copyright_removal_prams": {"contents_column_name": "contents",
                                            'license' : True,
                                            'copyright' : True}}
    transform = LicenseCopyrightRemoveTransform(license_copyright_removal_pram)

    data_access = DataAccessLocal()
    table = data_access.get_table(os.path.join(input_folder, "sample_parquet.parquet"))
    print(f"input table: {table}")
    table_list, metadata = transform.transform(table)
    
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, 'sample_paquet.parquet')

    pq.write_table(table_list[0], output_path)
    print(f"Output column : \n")
    new_data = table_list[0].column('updated_content').to_pylist()
    for data in new_data:
        for line in data.split('\n'):
            print(line)