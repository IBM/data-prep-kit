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
from license_select_transform import LicenseSelectTransform


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))

licenses_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/sample_approved_licenses.json"))

with open(licenses_file) as f:
    licenses = json.loads(f.read())

if __name__ == "__main__":
    license_select_params = {
        "license_select_params": {
            "license_column_name": "license",
            "allow_no_license": False,
            "licenses": licenses,
            "deny": False,
        }
    }
    transform = LicenseSelectTransform(license_select_params)

    data_access = DataAccessLocal()
    table, _ = data_access.get_table(os.path.join(input_folder, "sample_1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
