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

from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher
from fdedup.transforms.base import (preprocessor_doc_column_name_cli_param,
                                    preprocessor_int_column_name_cli_param,
                                    delimiters_cli_param,
                                    preprocessor_num_permutations_cli_param,
                                    preprocessor_threshold_cli_param,
                                    shingles_size_cli_param,
                                    )
from fdedup_ray.transforms import (FdedupPreprocessorRayTransformRuntimeConfiguration,
                                   preprocessor_bucket_cpu_cli_param,
                                   preprocessor_minhash_cpu_cli_param,
                                   preprocessor_doc_id_cpu_cli_param,
                                   preprocessor_num_buckets_cli_param,
                                   preprocessor_num_doc_id_cli_param,
                                   preprocessor_num_minhash_cli_param,
                                   )

class TestRayFdedupPreprocessorTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        launcher = RayTransformLauncher(FdedupPreprocessorRayTransformRuntimeConfiguration())
        config = {"run_locally": True,
                  preprocessor_doc_column_name_cli_param: "contents",
                  preprocessor_int_column_name_cli_param: "Unnamed: 0",
                  delimiters_cli_param: " ",
                  preprocessor_num_permutations_cli_param: 64,
                  preprocessor_threshold_cli_param: .8,
                  shingles_size_cli_param: 5,
                  preprocessor_num_buckets_cli_param: 1,
                  preprocessor_bucket_cpu_cli_param: .5,
                  preprocessor_num_doc_id_cli_param: 1,
                  preprocessor_doc_id_cpu_cli_param: .5,
                  preprocessor_num_minhash_cli_param: 1,
                  preprocessor_minhash_cpu_cli_param: .5,
                  }
        return [(launcher, config, basedir + "/input", basedir + "/preprocessor")]

