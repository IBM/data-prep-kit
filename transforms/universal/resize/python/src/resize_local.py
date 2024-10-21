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
from resize_transform import ResizeTransform


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
resize_params = {"max_rows_per_table": 75}
if __name__ == "__main__":
    # Here we show how to run outside of ray
    # Create and configure the transform.
    transform = ResizeTransform(resize_params)

    # Use the local data access to read a parquet table that has 200 rows
    data_access = DataAccessLocal()
    table, _ = data_access.get_table(os.path.join(input_folder, "test1.parquet"))

    # Transform the table to tables of 75 rows.  This should give two tables each with 75 rows.
    table_list, metadata = transform.transform(table=table)
    print(f"# of output tables: {len(table_list)}")
    # Then flush the buffer which should contain the remaining 50 rows.
    table_list, metadata = transform.flush()
    table = table_list[0]
    print(f"Flushed output table with {table.num_rows} (expecting 50)")
