import os
import sys
from pathlib import Path

from blocklist_lh_transform import (
    BlockListLHTransform,
    BlockListLHTransformConfiguration,
    annotation_column_name_key,
    blocked_domain_list_url_key,
    source_url_column_name_key,
)
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils


# create launcher
launcher = TransformLauncher(transform_runtime_config=BlockListLHTransformConfiguration())
# create parameters
s3_cred = {
    "access_key": "YOUR Lakehouse COS ACCESS KEY",
    "secret_key": "YOUR Lakehouse COS PRIVATE KEY",
    "cos_url": "https://s3.us-east.cloud-object-storage.appdomain.cloud",
}

# Configure lakehouse unit test tables
lakehouse_config = {
    "lh_environment": "STAGING",
    "input_table": "bluepile.academic.doabooks",
    "input_dataset": "",
    "input_version": "main",
    "output_table": "lh_test.bluepile_academic_doabooks_blocklist_test",
    "output_path": "lh-test/tables/lh_test/bluepile_academic_doabooks_blocklist_test",
    "token": "YOUR Lakehouse TOKEN",
}

blocklist_conf_url = os.path.abspath(os.path.join(os.path.dirname(__file__), ".", "test-data", "domains"))
blocklist_annotation_column_name = "url_blocklisting_refinedweb"
blocklist_doc_source_url_column = "title"

worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": 4,
    "s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "lh_config": ParamsUtils.convert_to_ast(lakehouse_config), 
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 2,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
    blocked_domain_list_url_key: blocklist_conf_url,
    annotation_column_name_key: blocklist_annotation_column_name,
    source_url_column_name_key: blocklist_doc_source_url_column,
}
sys.argv = ParamsUtils.dict_to_req(d=params)

# launch
launcher.launch()
