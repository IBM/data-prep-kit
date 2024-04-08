import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import DPLConfig, ParamsUtils
from lang_id_transform import (
    PARAM_MODEL_CREDENTIAL,
    PARAM_MODEL_KIND,
    PARAM_MODEL_URL,
    LangIdentificationTableTransformConfiguration,
)
from lang_models import KIND_FASTTEXT


# create launcher
launcher = TransformLauncher(transform_runtime_config=LangIdentificationTableTransformConfiguration())
# create parameters

s3_cred = {
    "access_key": DPLConfig.S3_ACCESS_KEY,
    "secret_key": DPLConfig.S3_SECRET_KEY,
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure s3 folders
s3_conf = {
    "input_folder": "lh-test/tables/academic/ieee_lh_unittest",
    "output_folder": "lh-test/tables/academic/ieee_lang_id_0311_02/",
}

worker_options = {"num_cpus": 1}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
langid_config = {
    PARAM_MODEL_KIND: KIND_FASTTEXT,
    PARAM_MODEL_URL: "facebook/fasttext-language-identification",
    PARAM_MODEL_CREDENTIAL: DPLConfig.HUGGING_FACE_TOKEN,
}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_s3_config": ParamsUtils.convert_to_ast(s3_conf),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # lang id specific
    **langid_config,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
