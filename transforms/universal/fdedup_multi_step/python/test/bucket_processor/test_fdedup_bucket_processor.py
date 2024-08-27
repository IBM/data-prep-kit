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
from typing import Tuple

from data_processing.test_support import get_files_in_folder
from data_processing.data_access import DataAccessFactory
from data_processing.utils import RANDOM_SEED
from data_processing.test_support.transform import AbstractBinaryTransformTest
from data_processing.transform import TransformStatistics
from fdedup.utils import DocsMinHash, MurmurMH, DocCollector
from fdedup.transforms.python import PythonBucketsHashProcessor, FdedupBucketProcessorTransform, processor_key


class TestFdedupPreprocessorTransform(AbstractBinaryTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test-data/input"))
        input_dir = os.path.join(basedir, "snapshot/buckets")
        input_files = get_files_in_folder(dir=input_dir, ext="")
        input_files = [(name, binary) for name, binary in input_files.items()]
        data_access_factory = DataAccessFactory()
        mn_min_hash = MurmurMH(num_perm=64, seed=RANDOM_SEED)
        minhashes = DocsMinHash({"id": 0, "data_access": data_access_factory,
                                 "snapshot": os.path.join(basedir, "snapshot/minhash/minhash_collector_0")})
        doc_collector = DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": None})
        bucket_processor = PythonBucketsHashProcessor({"threshold": .8,
                                                       "mn_min_hash": mn_min_hash,
                                                       "docs_collector": doc_collector,
                                                       "minhash_collector": minhashes,
                                                       "statistics": TransformStatistics(),
                                                       "print_interval": 10})
        fdedup_params = {processor_key: bucket_processor}
        expected_metadata_list = [{'long buckets': 0, 'short buckets': 3}, {}]
        return [
            (FdedupBucketProcessorTransform(fdedup_params), input_files, [], expected_metadata_list),
        ]
