import yaml


PRE_COMMIT = "../pre-commit-config.yaml"
PIPELINE_TEMPLATE_FILE = "template_superpipeline.py"

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


def get_generic_params(params, prefix="") -> str:
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


if __name__ == "__main__":
    import argparse

    from jinja2 import Environment, FileSystemLoader
    from pre_commit.main import main

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template(PIPELINE_TEMPLATE_FILE)

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

    for task in pipeline_tasks:
        task_name = task["name"]
        task_pipeline_name = task["pipeline_name"]
        task_image = task["image"]

        tasks_steps_params.append(pipeline_definitions[task_name + "_step_parameters"])

    # get the sub tasks input parameters
    sub_workflows_parameters = ""
    index = 0
    for task_step_params in tasks_steps_params:
        task_params = task_step_params.get(STEP_PARAMETERS)
        task_name = pipeline_tasks[index]["name"]
        sub_workflows_parameters += "\n\t# " + task_name + " step parameters"
        sub_workflows_parameters += "\n\tp" + str(index + 3) + '_name: str = "' + task_name + '",'
        sub_workflows_parameters += get_generic_params(task_params, "p" + str(index + 3) + "_")
        override_params = (
            "\n\tp"
            + str(index + 3)
            + '_overriding_params: str = \'{"ray_worker_options": {"image": "\' + '
            + task_name
            + "_image + '\"}, "
        )
        override_params += '"ray_head_options": {"image": "\' + ' + task_name + "_image + '\"}}',"
        sub_workflows_parameters += override_params

        index += 1

    sub_workflows_operations = ""
    # build the op for the first sub workflow
    task_name = pipeline_tasks[0]["name"]
    task_op = (
        "    "
        + task_name
        + " = run_"
        + task_name
        + "_op(name=p1_orch_"
        + task_name
        + '_name, prefix="p3_", params=args, host=orch_host, input_folder=p2_pipeline_input_parent_path)'
    )
    task_op += "\n    _set_component(" + task_name + ', "' + task_name + '")'

    sub_workflows_operations += task_op
    i = 1
    prefix_index = 4
    for task in pipeline_tasks[1:]:
        task_name = task["name"]
        task_op = (
            "\n    "
            + task_name
            + " = run_"
            + task_name
            + "_op(name=p1_orch_"
            + task_name
            + '_name, prefix="p'
            + str(prefix_index)
            + '_", params=args, host=orch_host, input_folder='
            + pipeline_tasks[i - 1]["name"]
            + ".output)"
        )
        task_op += (
            "\n    _set_component(" + task_name + ', "' + task_name + '", ' + pipeline_tasks[i - 1]["name"] + ")"
        )
        sub_workflows_operations += task_op
        prefix_index += 1
        i += 1

    content = template.render(
        superpipeline_name=pipeline_metadata[NAME],
        superpipeline_description=pipeline_metadata[DESCRIPTION],
        sub_workflows_components=pipeline_definitions[PIPELINE_TASKS],
        component_spec_path=component_spec_path,
        p1_parameters=pipeline_definitions[PIPELINE_TASKS],
        add_p2_parameters=common_input_params,
        sub_workflows_parameters=sub_workflows_parameters,
        sub_workflows_operations=sub_workflows_operations,
    )
    output_file = f"{args.output_dir_file}/{pipeline_metadata[NAME]}_wf.py"
    with open(output_file, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {output_file}")

    import sys

    from pre_commit.main import main

    print(f"Pipeline ${output_file} auto generation completed")
    # format the pipeline python file
    args = ["run", "--file", f"{output_file}", "-c", PRE_COMMIT]
    sys.exit(main(args))
