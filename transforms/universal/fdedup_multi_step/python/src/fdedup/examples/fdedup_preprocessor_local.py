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

from data_processing.data_access import DataAccessFactory
from data_processing.utils import RANDOM_SEED
from fdedup.utils import BucketsHash, MurmurMH, DocsMinHash
from fdedup.transforms.base import (doc_column_name_key, int_column_name_key, shingles_size_key,
                                    delimiters_key, mn_min_hash_key, minhashes_cache_key, buckets_cache_key)
from fdedup.transforms.python import FdedupPreprocessorTransform


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}

data_access_factory = DataAccessFactory()
data_access_factory.apply_input_params({"data_local_config": local_conf})
mn_min_hash = MurmurMH(num_perm=64, seed=RANDOM_SEED)
minhash_collector = DocsMinHash({"id": 0, "data_access": data_access_factory, "snapshot": None})
bucket_collector = BucketsHash({"id": 0, "data_access": data_access_factory, "snapshot": None})

fdedup_params = {doc_column_name_key: "contents", int_column_name_key: "Unnamed: 0", shingles_size_key: 5,
                 delimiters_key: " ", mn_min_hash_key: mn_min_hash, minhashes_cache_key: minhash_collector,
                 buckets_cache_key: bucket_collector}

if __name__ == "__main__":
    # Create and configure the transform.
    transform = FdedupPreprocessorTransform(fdedup_params)
    # Use the local data access to read a parquet table.
    table, _ = data_access_factory.create_data_access().get_table(os.path.join(input_folder, "sample1.parquet"))
    print(f"Input table has {table.num_rows} rows and {table.num_columns} columns")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"Metadata {metadata}")
    minhashes = minhash_collector.get_content()
    print(f"Number of minhashes {len(minhashes)}")
    print(f"Buckets: {bucket_collector.get_content()}")
