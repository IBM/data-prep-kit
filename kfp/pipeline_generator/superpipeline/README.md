## Steps to generate a new super pipeline in KFP v1.
- The super pipeline allows you to execute several transforms within a single pipeline. For more details, refer [multi_transform_pipeline.md](../../doc/multi_transform_pipeline.md).
- Create a `super_pipeline_definitions.yaml` file for the required task. You can refer to the example [super_pipeline_definitions.yaml](./super_pipeline_definitions.yaml).
- Execute `./run.sh --config_file < super_pipeline_definitions.yaml> --output_dir_file <destination_directory>`. Here, `super_pipeline_definitions.yaml` is the super pipeline definition file, that you created above, and `destination_directory` is the directory where the new super pipeline file will be generated.


*__NOTE__*: the `component_spec_path` is the path to the `kfp_ray_components` folder and depends on where the workflow is compiled.
