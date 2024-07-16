import json

import yaml


PRE_COMMIT = "../pre-commit-config.yaml"
PIPELINE_TEMPLATE_FILE = "template/superpipeline.ptmpl"

PIPELINE_TASKS = "super_pipeline_tasks"
COMMON_INPUT_PARAMETERS = "super_pipeline_common_parameters"
PIPELINE_METADATA = "super_pipeline_metadata"

INPUT_PARAMETERS = "input_parameters"
PIPELINE_PARAMETERS = "pipeline_parameters"
STEP_PARAMETERS = "step_parameters"
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

def get_sub_pipelines_names(pipeline_tasks) -> str:
    ret_str = ""
    for task in pipeline_tasks:
        task_name = task["name"]
        task_pipeline_name = task["pipeline_name"]
        ret_str += "\n    p1_orch_" + task_name + "_name: str = \"" + task_pipeline_name + '",'
    return ret_str

def get_generic_params(params, prefix = "") -> str:
    ret_str = ""
    if params is None:
        return ret_str
    for param in params:
        ret_str += f"\n    {prefix}{param[NAME]}: {param[TYPE]} = "
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

    tasks_steps_params = []
    pipeline_metadata = pipeline_definitions[PIPELINE_METADATA]
    pipeline_tasks = pipeline_definitions[PIPELINE_TASKS]
    common_input_params = pipeline_definitions[COMMON_INPUT_PARAMETERS]

    component_spec_path = pipeline_metadata.get("component_spec_path", "")
    if component_spec_path == "":
        component_spec_path = "../../../../../kfp/kfp_ray_components/"

    sub_workflows_components = ""
    sub_workflows_images = ""
    for task in pipeline_tasks:
        task_name = task["name"]
        task_pipeline_name = task["pipeline_name"]
        task_image = task["image"]

        tasks_steps_params.append(pipeline_definitions[task_name+"_step_parameters"])
        sub_workflows_components += "\nrun_" + task_name + '_op = comp.load_component_from_file(component_spec_path + "executeSubWorkflowComponent.yaml")'
        sub_workflows_images += "\n" + task_name + '_image = "' + task_image + '"'

    # Pipeline template file
    fin = open(PIPELINE_TEMPLATE_FILE, "rt")

    # Output file to write the pipeline
    fout = open(f"{pipeline_metadata[NAME]}_wf.py", "wt")

    # get tasks pipelines
    tasks_pipelines = get_sub_pipelines_names(pipeline_tasks)
    # print(tasks_pipelines)

    # get the common super pipeline input parameters
    common_input_parameters_text = get_generic_params(common_input_params, "p2_pipeline_")
    # print(common_input_parameters_text)

    # sub_tasks_input_parameters = []
    # get the sub tasks input parameters
    sub_workflows_parameters = ""
    index = 0
    for task_step_params in tasks_steps_params:
        task_params = task_step_params.get(STEP_PARAMETERS)
        task_name = pipeline_tasks[index]["name"]
        sub_workflows_parameters += "\n\t# " + task_name + " step parameters"
        sub_workflows_parameters += "\n\tp" + str(index + 3) + "_name: str = \"" + task_name + "\","
        sub_workflows_parameters += get_generic_params(task_params, "p" + str(index + 3) + "_")
        override_params = "\n\tp" + str(index + 3) + "_overriding_params: str = '{\"ray_worker_options\": {\"image\": \"' + " + task_name + "_image + '\"}, "
        override_params += "\"ray_head_options\": {\"image\": \"' + " + task_name + "_image + '\"}}',"
        sub_workflows_parameters += override_params

        index += 1
    # print(sub_workflows_parameters)

    sub_workflows_operations = ""
    # build the op for the first sub workflow
    task_name = pipeline_tasks[0]["name"]
    task_op = "    " + task_name + " = run_" + task_name + "_op(name=p1_orch_" + task_name + "_name, prefix=\"p3_\", params=args, host=orch_host, input_folder=p2_pipeline_input_parent_path)"
    task_op += "\n    _set_component(" + task_name + ', "' + task_name + '")'

    sub_workflows_operations += task_op
    i = 1
    prefix_index = 4
    for task in pipeline_tasks[1:]:
        task_name = task["name"]
        task_op = "\n    " + task_name + " = run_" + task_name + "_op(name=p1_orch_" + task_name + "_name, prefix=\"p" + str(prefix_index) + '_", params=args, host=orch_host, input_folder=' + pipeline_tasks[i-1]["name"] + ".output)"
        task_op += "\n    _set_component(" + task_name + ', "' + task_name + '", ' + pipeline_tasks[i-1]["name"] + ")"
        sub_workflows_operations += task_op
        prefix_index += 1
        i += 1

    # For each line in the template file
    for line in fin:
        # read replace the string and write to output pipeline file
        fout.write(
            line.replace("__superpipeline_name__", pipeline_metadata[NAME])
            .replace("__superpipeline_description__", pipeline_metadata[DESCRIPTION])
            .replace("__sub_workflows_components__", sub_workflows_components)
            .replace("__component_spec_path__", component_spec_path)
            .replace("__sub_workflows_images__", sub_workflows_images)
            .replace("__p1_parameters__", tasks_pipelines)
            .replace("__add_p2_parameters__", common_input_parameters_text)
            .replace("__sub_workflows_parameters__", sub_workflows_parameters)
            .replace("__sub_workflows_operations__", sub_workflows_operations)
        )
    # Move the generated file to the output directory
    curr_dir = os.getcwd()
    src_file = Path(f"{curr_dir}/{pipeline_metadata[NAME]}_wf.py")
    dst_file = Path(f"{args.output_dir_file}/{pipeline_metadata[NAME]}_wf.py")
    src_file.rename(dst_file)

    fout.close()

    import sys

    from pre_commit.main import main

    print(f"Pipeline ${dst_file} auto generation completed")
    # format the pipeline python file
    args = ["run", "--file", f"{dst_file}", "-c", PRE_COMMIT]
    sys.exit(main(args))
