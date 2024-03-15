import os
import sys
from argparse import ArgumentParser
from pathlib import Path

from data_processing.ray import TransformLauncher
from data_processing.utils import ParamsUtils
from doc_quality_transform import (
    DocQualityTransform,
    DocQualityTransformConfiguration,
    drop_column_if_existed_key,
    ft_lang_key,
)


# create parameters
ft_lang = "en"
drop_column_if_existed = True

bad_word_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/docq"))


input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data/input"))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output"))
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
doc_quality_params = {
    ft_lang_key: ft_lang,
    drop_column_if_existed_key: drop_column_if_existed,
    "doc_quality_local_config": ParamsUtils.convert_to_ast(local_conf),
}
worker_options = {"num_cpus": 0.8}
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
launcher_params = {
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

# launch
run_in_ray = False
run_in_ray = True
if __name__ == "__main__":
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    if not run_in_ray:
        sys.argv = ParamsUtils.dict_to_req(doc_quality_params)
        config = DocQualityTransformConfiguration()
        parser = ArgumentParser()
        config.add_input_params(parser)
        args = parser.parse_args()
        config.apply_input_params(args)
        config = config.get_input_params()
        transform = DocQualityTransform(config)
        print(f"config: {config}")
        print(f"transform: {transform}")
    else:
        # create launcher
        sys.argv = ParamsUtils.dict_to_req(launcher_params | doc_quality_params)
        launcher = TransformLauncher(transform_runtime_config=DocQualityTransformConfiguration())
        # Launch the ray actor(s) to process the input
        launcher.launch()
