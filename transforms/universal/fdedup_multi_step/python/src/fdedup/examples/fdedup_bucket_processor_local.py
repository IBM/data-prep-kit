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
from data_processing.transform import TransformStatistics
from fdedup.utils import DocsMinHash, DocCollector
from fdedup.transforms.python import PythonBucketsHashProcessor, FdedupBucketProcessorTransform, processor_key


# create parameters
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test-data/input"))
input_folder = os.path.join(basedir, "snapshot/buckets")
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}

data_access_factory = DataAccessFactory()
data_access_factory.apply_input_params({"data_local_config": local_conf})
minhashes = DocsMinHash({"id": 0, "data_access": data_access_factory,
                         "snapshot": os.path.join(basedir, "snapshot/minhash/minhash_collector_0")})
doc_collector = DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": None})
bucket_processor = PythonBucketsHashProcessor({"threshold": .8,
                                               "docs_collector": doc_collector,
                                               "minhash_collector": minhashes,
                                               "statistics": TransformStatistics(),
                                               "print_interval": 10})
fdedup_params = {processor_key: bucket_processor}

if __name__ == "__main__":
    # Create and configure the transform.
    transform = FdedupBucketProcessorTransform(fdedup_params)
    # Use the local data access to read a parquet table.
    f_name = os.path.join(input_folder, "buckets_collector_0")
    bucket, _ = data_access_factory.create_data_access().get_file(f_name)
    # Transform the buckets file
    _, metadata = transform.transform_binary(file_name=f_name, byte_array=bucket)
    print(f"Metadata {metadata}")
