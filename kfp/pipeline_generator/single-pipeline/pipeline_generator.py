
PIPELINE_TEMPLATE_FILE = "simple_pipeline.py"

INPUT_PARAMETERS = "input_parameters"
PIPELINE_PARAMETERS = "pipeline_parameters"
PIPELINE_COMMON_INPUT_PARAMETERS_VALUES = "pipeline_common_input_parameters_values"
PIPELINE_TRANSFORM_INPUT_PARAMETERS = "pipeline_transform_input_parameters"

NAME = "name"
TYPE = "type"
VALUE = "value"
DESCRIPTION = "description"

if __name__ == "__main__":
    import argparse
    import os
    import yaml
    from jinja2 import Environment, FileSystemLoader

    script_dir = os.path.dirname(os.path.abspath(__file__))
    environment = Environment(loader=FileSystemLoader(f"{script_dir}/templates/"))
    template = environment.get_template(PIPELINE_TEMPLATE_FILE)

    pre_commit_config = f"{script_dir}/../../../.pre-commit-config.yaml"
    parser = argparse.ArgumentParser(description="Kubeflow pipeline generator for Foundation Models")
    parser.add_argument("-c", "--config_file", type=str, default="")
    parser.add_argument("-od", "--output_dir_file", type=str, default="")
    args = parser.parse_args()

    with open(args.config_file, "r") as file:
        pipeline_definitions = yaml.safe_load(file)

    pipeline_parameters = pipeline_definitions[PIPELINE_PARAMETERS]
    common_input_params_values = pipeline_definitions[PIPELINE_COMMON_INPUT_PARAMETERS_VALUES]
    pipeline_transform_input_parameters = pipeline_definitions[PIPELINE_TRANSFORM_INPUT_PARAMETERS]

    component_spec_path = pipeline_parameters.get("component_spec_path", "")
    if component_spec_path == "":
        component_spec_path = "../../../../kfp/kfp_ray_components/"

    content = template.render(
        transform_image=common_input_params_values["transform_image"],
        script_name=pipeline_parameters["script_name"],
        kfp_base_image=common_input_params_values["kfp_base_image"],
        component_spec_path=component_spec_path,
        pipeline_arguments=pipeline_transform_input_parameters["pipeline_arguments"],
        pipeline_name=pipeline_parameters[NAME],
        pipeline_description=pipeline_parameters["description"],
        input_folder=common_input_params_values.get("input_folder", ""),
        output_folder=common_input_params_values.get("output_folder", ""),
        s3_access_secret=common_input_params_values["s3_access_secret"],
        image_pull_secret=common_input_params_values["image_pull_secret"],
        multi_s3=pipeline_parameters["multi_s3"],
    )
    output_file = f"{args.output_dir_file}{pipeline_parameters[NAME]}_wf.py"
    with open(output_file, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {output_file}")

    # format the pipeline python file
    import sys

    from pre_commit.main import main

    args = ["run", "--file", f"{output_file}", "-c", f"{pre_commit_config}"]
    sys.exit(main(args))
