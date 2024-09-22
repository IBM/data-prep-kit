## Steps to generate a new pipeline
- create a `pipeline_definitions.yaml` file for the required task (similar to the example [pipeline_definitions.yaml for the noop task](../../../transforms/universal/noop/kfp_ray/pipeline_definitions.yaml)).
- execute `make -C ../../../transforms workflow-venv` from this directory
- execute `source ../../../transforms/venv/bin/activate`
- execute `./run.sh --config_file <pipeline_definitions_file_path> --output_dir_file <destination directory>`. When `pipeline_definitions_file_path` is the path of the `pipeline_definitions.yaml` file that defines the pipeline and `destination directory` is a directory where new pipeline file 
will be generated.
