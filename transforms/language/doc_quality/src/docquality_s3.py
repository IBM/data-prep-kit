import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPFConfig, ParamsUtils
from doc_quality_transform import DocQualityTransformConfiguration


print(os.environ)
# create launcher
launcher = TransformLauncher(transform_runtime_config=DocQualityTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": DPFConfig.S3_ACCESS_KEY,
    "secret_key": DPFConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}
docq_params = {
    "ft_lang": "en",
    "bad_word_filepath": "../test-data/docq/ldnoobw/",
    "MODEL_DIR": "../lm_sp/",
}
s3_conf = {
    "input_folder": "cos-optimal-llm-pile/test/hajar/input/",
    "output_folder": "cos-optimal-llm-pile/test/hajar/output/",
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "s3_config": ParamsUtils.convert_to_ast(s3_conf),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 5,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}
sys.argv = ParamsUtils.dict_to_req(d=params | docq_params)

# launch
launcher.launch()
