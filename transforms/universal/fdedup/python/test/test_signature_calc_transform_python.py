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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.utils import ParamsUtils
from signature_calc_transform_python import (
    SignatureCalculationPythonTransformConfiguration,
)


class TestPythonSignatureCalcTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    # # create parameters
    # input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
    # output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
    # local_conf = {"input_folder": input_folder, "output_folder": output_folder}
    # code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    # params = {
    #     # Data access. Only required parameters are specified
    #     "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    #     # execution info
    #     "runtime_pipeline_id": "pipeline_id",
    #     "runtime_job_id": "job_id",
    #     "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    #     "minhash_num_permutations": 112,
    #     "minhash_num_bands": 14,
    #     "minhash_num_segments": 2,
    # }
    print("====")

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            "minhash_num_permutations": 112,
            "minhash_num_bands": 14,
            "minhash_num_segments": 2,
            # # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            # # columns used
            # "fdedup_doc_column": "contents",
            # "fdedup_id_column": "int_id_column",
            # "fdedup_cluster_column": "cluster",
            # # infrastructure
            # "fdedup_bucket_cpu": 0.5,
            # "fdedup_doc_cpu": 0.5,
            # "fdedup_mhash_cpu": 0.5,
            # "fdedup_num_doc_actors": 1,
            # "fdedup_num_bucket_actors": 1,
            # "fdedup_num_minhash_actors": 1,
            # "fdedup_num_preprocessors": 1,
            # # fuzzy parameters
            # "fdedup_num_permutations": 64,
            # "fdedup_threshold": 0.8,
            # "fdedup_shingles_size": 5,
            # "fdedup_delimiters": " ",
            # # Random delay between reads
            # "fdedup_random_delay_limit": 5,
            # # snapshotting
            # "fdedup_snapshot_delay": 1,
            # "fdedup_use_doc_snapshot": False,
            # "fdedup_use_bucket_snapshot": False,
        }
        launcher = PythonTransformLauncher(SignatureCalculationPythonTransformConfiguration())
        fixtures = [(launcher, config, basedir + "/input/data_1/", basedir + "/expected/signature_calc/")]
        return fixtures
