import sys

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from antivirus_transform import AntivirusTransformConfiguration


# create launcher
launcher = TransformLauncher(transform_runtime_config=AntivirusTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": "YOUR KEY",
    "secret_key": "YOUR SECRET KEY",
    "url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "code.cobol",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "code.cobol_antivirus_test",
    "output_path": "lh-test/tables/code/cobol_antivirus_test",
    "token": "YOUR LAKEHOUSE TOKEN",
}

worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "data_lh_config": ParamsUtils.convert_to_ast(lakehouse_config),
    # orchestrator
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 5,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    # antivirus specific
    "antivirus_input_column": "contents",
    "antivirus_output_column": "virus_detection",
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
