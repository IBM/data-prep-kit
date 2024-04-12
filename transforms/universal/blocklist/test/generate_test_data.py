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

from data_processing.data_access import DataAccessLocal
from test_blocklist import TestBlockListTransform


# This main() is run to generate the test data file whenever the test data defined in TestBlockListTransform changes.
# It generates the test and out put data into the input and expected directories.
if __name__ == "__main__":
    t = TestBlockListTransform()
    inp = t.input_df
    out = t.expected_output_df
    config = {"input_folder": "../test-data", "output_folder": "../test-data"}
    data_access = DataAccessLocal(config, [], False, -1)
    data_access.save_table("input/test1.parquet", inp)
    data_access.save_table("expected/test1.parquet", out)
