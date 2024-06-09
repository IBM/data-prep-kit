import json

import yaml


PRE_COMMIT = "./pre-commit-config.yaml"
PIPELINE_TEMPLATE_FILE = "pipeline.ptmpl"

INPUT_PARAMETERS = "input_parameters"
PIPELINE_PARAMETERS = "pipeline_parameters"
PIPELINE_COMMON_INPUT_PARAMETERS_VALUES = "pipeline_common_input_parameters_values"
PIPELINE_TRANSFORM_INPUT_PARAMETERS = "pipeline_transform_input_parameters"

NAME = "name"
TYPE = "type"
VALUE = "value"
DESCRIPTION = "description"


def get_pipeline_input_parameters(arguments) -> str:
    ret_str = ""
    ret_str += get_generic_params(arguments.get("pipeline_arguments", None))
    return ret_str


def get_generic_params(params) -> str:
    ret_str = ""
    if params is None:
        return ret_str
    for param in params:
        ret_str += f"\n    {param[NAME]}: {param[TYPE]} = "
        if param[TYPE] == "str":
            ret_str += f'"{param[VALUE]}"'
        else:
            ret_str += f"{param[VALUE]}"
        ret_str += f",  {param.get(DESCRIPTION, '')}"
    return ret_str


def get_execute_job_params_guf(args) -> (str):
    ret_execute_job_params = ""
    if args is not None:
        pargs = args.get("pipeline_arguments", None)
        if pargs is not None:
            for a in pargs:
                ret_execute_job_params += f'"{a[NAME]}": {a[NAME]},\n'
    return ret_execute_job_params


if __name__ == "__main__":
    import argparse
    import os
    from pathlib import Path

    from pre_commit.main import main

    parser = argparse.ArgumentParser(description="Kubeflow pipeline generator for Foundation Models")
    parser.add_argument("-c", "--config_file", type=str, default="")
    parser.add_argument("-od", "--output_dir_file", type=str, default="")

    args = parser.parse_args()
    # open configuration file
    with open(args.config_file, "r") as file:
        pipeline_definitions = yaml.safe_load(file)

    pipeline_parameters = pipeline_definitions[PIPELINE_PARAMETERS]
    common_input_params_values = pipeline_definitions[PIPELINE_COMMON_INPUT_PARAMETERS_VALUES]

    # Pipeline template file
    fin = open(PIPELINE_TEMPLATE_FILE, "rt")

    # Output file to write the pipeline
    fout = open(f"{pipeline_parameters[NAME]}_wf.py", "wt")

    # define the generated pipeline input parameters
    transform_input_parameters = get_pipeline_input_parameters(pipeline_definitions[PIPELINE_TRANSFORM_INPUT_PARAMETERS])

    # define arguments to the Ray execution job
    execute_job_params = get_execute_job_params_guf(pipeline_definitions[PIPELINE_TRANSFORM_INPUT_PARAMETERS])

    component_spec_path = pipeline_parameters.get("component_spec_path", "")
    if component_spec_path == "":
        component_spec_path = "../../../../../kfp/kfp_ray_components/"

    compute_func_name = pipeline_parameters.get("compute_func_name", "")
    if compute_func_name == "":
        compute_func_name = "ComponentUtils.default_compute_execution_params"

    compute_func_import = pipeline_parameters.get("compute_func_import", "")

    execute_comp_file = "executeRayJobComponent.yaml"
    prefix_name = ""
    prefix_execute = ""
    prefix_set_secret = ""
    if pipeline_parameters.get("multi_s3", False) == True:
        execute_comp_file = "executeRayJobComponent_multi_s3.yaml"
        prefix_name = pipeline_parameters.get("prefix", "")
        prefix_execute = "prefix=PREFIX"
        prefix_set_secret = f"ComponentUtils.set_s3_env_vars_to_component(execute_job, {prefix_name}_s3_access_secret, prefix=PREFIX)"

    # For each line in the template file
    for line in fin:
        # read replace the string and write to output pipeline file
        fout.write(
            line.replace("__pipeline_name__", pipeline_parameters[NAME])
            .replace("__pipeline_description__", pipeline_parameters["description"])
            .replace("__pipeline_arguments__", transform_input_parameters)
            .replace("__execute_job_params__", execute_job_params)
            .replace("__compute_func_name__", compute_func_name)
            .replace("__component_spec_path__", component_spec_path)
            .replace("__compute_import__", compute_func_import)
            .replace("__script_name__", pipeline_parameters["script_name"])
            .replace("__image_pull_secret__", common_input_params_values["image_pull_secret"])
            .replace("__s3_access_secret__", common_input_params_values["s3_access_secret"])
            .replace("__input_folder__", common_input_params_values.get("input_folder", ""))
            .replace("__output_folder__", common_input_params_values.get("output_folder", ""))
            .replace("__transform_image__", common_input_params_values["transform_image"])
            .replace("__kfp_base_image__", common_input_params_values["kfp_base_image"])
            .replace("__execute_comp__", execute_comp_file)
            .replace("__prefix_name__", prefix_name)
            .replace("__prefix_execute__", prefix_execute)
            .replace("__prefix_set_secret__", prefix_set_secret)
        )
    # Move the generated file to the output directory
    curr_dir = os.getcwd()
    src_file = Path(f"{curr_dir}/{pipeline_parameters[NAME]}_wf.py")
    dst_file = Path(f"{args.output_dir_file}/{pipeline_parameters[NAME]}_wf.py")
    src_file.rename(dst_file)

    fout.close()

    import sys

    from pre_commit.main import main

    print(f"Pipeline ${dst_file} auto generation completed")
    # format the pipeline python file
    args = ["run", "--file", f"{dst_file}", "-c", PRE_COMMIT]
    sys.exit(main(args))
