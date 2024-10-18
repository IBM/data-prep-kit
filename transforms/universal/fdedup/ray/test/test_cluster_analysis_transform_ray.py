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

from cluster_analysis_transform import (
    jaccard_similarity_threshold_cli_param,
    num_bands_cli_param,
    num_segments_cli_param,
    sort_output_cli_param,
)
from cluster_analysis_transform_ray import ClusterAnalysisRayTransformConfiguration
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing_ray.runtime.ray import RayTransformLauncher


class TestRayClusterAnalysisTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            "run_locally": True,
            num_bands_cli_param: 14,
            num_segments_cli_param: 2,
            jaccard_similarity_threshold_cli_param: 0.7,
            sort_output_cli_param: True,
        }
        launcher = RayTransformLauncher(ClusterAnalysisRayTransformConfiguration())
        fixtures = [
            (
                launcher,
                config,
                os.path.join(basedir, "expected", "signature_calc", "bands"),
                os.path.join(basedir, "expected", "cluster_analysis", "docs_to_remove"),
            )
        ]
        return fixtures
