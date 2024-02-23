import os
import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from doc_quality_implementation import DocQualityTableTransformConfiguration


print(os.environ)
# create launcher
launcher = TransformLauncher(name="DocQuality", transform_runtime_config=DocQualityTableTransformConfiguration())
# create parameters

s3_cred = {
    "access_key": "e49c0337161a424bb77061a23e5a853e",
    "secret_key": "8431539f43d6df45c6acc39bee45ef291ac86188b3503ee3",
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "academic.ieee",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "academic.ieee_doc_quality_0223_10",
    "output_path": "lh-test/tables/academic/ieee_doc_quality_0223_10",
    "token": "eyJhbGciOiJIUzI1NiIsInppcCI6IkRFRiJ9.eJyrVkrNTczMUbJSqixNzEvOyHQoLdbLTMrVS87PVdJRKi5NwiWVmViiZGVobmBhbmBkYWCko5RaUQASMDQzMbcACtQCAGU5G8E.b3aYMb3ViwN-gPs5XKA9iKizDuHfB2rVvc51iUwu9IA"
}

worker_options = {"num_cpus": 1}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
