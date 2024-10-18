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

import glob
import os
import sys

from data_processing.utils import ParamsUtils
from data_processing_ray.runtime.ray import RayTransformLauncher
from repo_level_order_transform import RepoLevelOrderRayTransformConfiguration


def execute(tf_params, tf_flags, input_folder, output_folder):
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    worker_options = {"num_cpus": 0.8}
    code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
    params = {
        # where to run
        "run_locally": True,
        # Data access. Only required parameters are specified
        "data_local_config": ParamsUtils.convert_to_ast(local_conf),
        # orchestrator
        "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
        "runtime_num_workers": 1,
        "runtime_pipeline_id": "pipeline_id",
        "runtime_job_id": "job_id",
        "runtime_creation_delay": 0,
        "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
    }
    sys.argv = ParamsUtils.dict_to_req(d=params | tf_params)
    # create launcher
    launcher = RayTransformLauncher(RepoLevelOrderRayTransformConfiguration())
    # Launch the ray actor(s) to process the input
    launcher.launch()


def num_repos(folder):
    repos = glob.glob(f"{folder}/*.parquet")
    return len(repos)


def cleanup_output(folder):
    try:
        import shutil

        shutil.rmtree(folder)
    except OSError:
        pass


def test_repo_ordering():
    # prepare params
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
    output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
    cleanup_output(output_folder)
    import tempfile

    with tempfile.TemporaryDirectory() as store_backend_dir:
        repo_level_params = {
            "repo_lvl_sorting_algo": "SORT_SEMANTIC_NORMALISED",
            "repo_lvl_store_type": "local",
            "repo_lvl_store_backend_dir": store_backend_dir,
            "repo_lvl_output_by_langs": True,
            "repo_lvl_sorting_enabled": True,
        }
        execute(repo_level_params, None, input_folder, output_folder)

        assert len(os.listdir(output_folder)) == 2  # one language folder, one metadata file
        assert len(os.listdir(f"{output_folder}/unknown")) == 2  # two repos, in language `unknown`
        assert num_repos(f"{output_folder}/*") == 2  # total two repos

        cleanup_output(output_folder)

    with tempfile.TemporaryDirectory() as store_backend_dir:
        repo_level_params = {
            "repo_lvl_store_type": "local",
            "repo_lvl_store_backend_dir": store_backend_dir,
            "repo_lvl_sorting_enabled": True,
        }
        execute(repo_level_params, None, input_folder, output_folder)
        os.listdir(output_folder)
        assert len(os.listdir(output_folder)) == 3  # two repos folder, one metadata file
        assert num_repos(f"{output_folder}") == 2  # total two repos
        cleanup_output(output_folder)

    with tempfile.TemporaryDirectory() as store_backend_dir:
        repo_level_params = {
            "repo_lvl_store_type": "local",
            "repo_lvl_store_backend_dir": store_backend_dir,
            "repo_lvl_combine_rows": True,
            "repo_lvl_sorting_enabled": True,
        }
        execute(repo_level_params, None, input_folder, output_folder)
        files = glob.glob(f"{output_folder}/*.parquet")
        import pyarrow as pa

        for file in files:
            table = pa.parquet.read_table(file)
            assert len(table) == 1
        cleanup_output(output_folder)
