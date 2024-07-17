## Steps to generate a new super pipeline in KFP v1.
- The super pipeline is a way to execute several transforms with one pipeline. You can find more details in [multi_transform_pipeline.md](../../doc/multi_transform_pipeline.md)
- create a `super_pipeline_definitions.yaml` file for the required task (similar to the example [super_pipeline_definitions.yaml](./super_pipeline_definitions.yaml)).
- execute `./run.sh <pipeline_definitions_file_path> <destination directory>`. When `pipeline_definitions_file_path` is the path of the `super_pipeline_definitions.yaml` file that defines the pipeline and `destination directory` is a directory where new super pipeline file will be generated.

*__NOTE__*: the `component_spec_path` is the path of the `kfp_ray_components` folder and it depends on where the workflow is compiled.