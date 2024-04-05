import os
import sys

from data_processing.data_access import DataAccessLocal
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from doc_quality_transform import DocQualityTransform, DocQualityTransformConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "run_locally": True,
    "max_files": -1,
    "local_config": ParamsUtils.convert_to_ast(local_conf),
    "worker_options": ParamsUtils.convert_to_ast(worker_options),
    "num_workers": 5,
    "checkpointing": False,
    "pipeline_id": "pipeline_id",
    "job_id": "job_id",
    "creation_delay": 0,
    "code_location": ParamsUtils.convert_to_ast(code_location),
}

if __name__ == "__main__":
    # Here we show how to run outside of ray
    # docquality transform needs a DataAccess to ready the kenLM model.
    data_access = DataAccessLocal(local_conf)
    params["data_access"] = data_access

    # Create and configure the transform.
    transform = DocQualityTransform(params)
    # Use the local data access to read a parquet table.
    table = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
    print(f"input table: {table}")
    # Transform the table
    table_list, metadata = transform.transform(table)
    print(f"\noutput table: {table_list}")
    print(f"output metadata : {metadata}")

    # sys.argv = ParamsUtils.dict_to_req(d=params)
    # # create launcher
    # launcher = TransformLauncher(transform_runtime_config=DocQualityTransformConfiguration())
    # # Launch the ray actor(s) to process the input
    # launcher.launch()
