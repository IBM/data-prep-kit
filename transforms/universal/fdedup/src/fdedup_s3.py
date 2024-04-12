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

import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPLConfig, ParamsUtils
from fdedup_transform import FdedupTableTransformConfiguration


# create launcher
launcher = TransformLauncher(transform_runtime_config=FdedupTableTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": DPLConfig.S3_ACCESS_KEY,
    "secret_key": DPLConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/sanity-test/input/dataset=fuzzy_dedup/",
    "output_folder": "cos-optimal-llm-pile/boris-da-test/",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    # Orchestration parameters
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 3,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # columns used
    "doc_column": "contents",
    "id_column": "int_id_column",
    "cluster_column": "cluster",
    # infrastructure
    "bucket_cpu": 0.5,
    "doc_cpu": 0.5,
    "mhash_cpu": 0.5,
    "num_doc_actors": 2,
    "num_bucket_actors": 1,
    "num_minhash_actors": 1,
    "num_preprocessors": 2,
    # fuzzy parameters
    "num_permutations": 64,
    "threshold": 0.8,
    "shingles_size": 5,
    "japanese_data": False,
    "delimiters": " ",
    # Random delay between reads
    "random_delay_limit": 5,
    # snapshotting
    "snapshot_delay": 1,
    "use_doc_snapshot": False,
    "use_bucket_snapshot": False,
}
sys.argv = ParamsUtils.dict_to_req(d=params)


# launch
launcher.launch()
