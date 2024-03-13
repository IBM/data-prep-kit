import os
import sys
from pathlib import Path

from data_processing.data_access import DataAccessLocal
from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from noop_transform import NOOPTransform, NOOPTransformConfiguration


# create parameters
input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test-data", "input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
noop_params = {"noop_sleep_sec": 2}
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
run_in_ray = False
run_in_ray = True
if __name__ == "__main__":
    if not run_in_ray:
        # Here we show how to run outside of ray
        # Create and configure the transform.
        transform = NOOPTransform(noop_params)
        # Use the local data access to read a parquet table.
        data_access = DataAccessLocal(local_conf)
        table = data_access.get_table(os.path.join(input_folder, "test1.parquet"))
        print(f"input table: {table}")
        # Transform the table
        table_list, metadata = transform.transform(table)
        print(f"\noutput table: {table_list}")
        print(f"output metadata : {metadata}")
    else:
        # Here we show to run the transform in the ray launcher
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        # Set the simulated command line args
        sys.argv = ParamsUtils.dict_to_req(d=params | noop_params)
        # create launcher
        launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
        # Launch the ray actor(s) to process the input
        launcher.launch()
