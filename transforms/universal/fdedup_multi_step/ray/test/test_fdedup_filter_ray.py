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
from fdedup.transforms.base import (filter_doc_column_name_cli_param,
                                    filter_int_column_name_cli_param,
                                    filter_cluster_column_name_cli_param,
                                    filter_removed_docs_column_name_cli_param,
                                    filter_doc_id_snapshot_directory_cli_param,
                                    )

from fdedup_ray.transforms import (FdedupFilterRayTransformRuntimeConfiguration,
                                   filter_docid_cpu_cli_param,
                                   filter_num_docid_cli_param,
                                   )

class TestRayFdedupFilterTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        launcher = RayTransformLauncher(FdedupFilterRayTransformRuntimeConfiguration())
        config = {"run_locally": True,
                  filter_doc_column_name_cli_param: "contents",
                  filter_int_column_name_cli_param: "Unnamed: 0",
                  filter_cluster_column_name_cli_param: "cluster",
                  filter_removed_docs_column_name_cli_param: "removed",
                  filter_doc_id_snapshot_directory_cli_param: os.path.join(basedir, "bucket_processor/snapshot/docs"),
                  filter_docid_cpu_cli_param: .5,
                  filter_num_docid_cli_param: 1,
                  }
        return [(launcher, config, basedir + "/input", basedir + "/filter")]
