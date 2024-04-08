# Pipeline Generator

Most of the "simple" pipelines are very similar, they all contains the same 4 share components:
- Compute execution parameters.
- Create a Ray cluster
- Execute Ray Job
- Exit Handler, that destroys the Ray cluster

The difference between pipelines is in the input parameters and their default values. 
This pipeline generator helps to create new basic pipelines.

### Pipeline parameters
The new pipeline input parameters and their default values are defined in the 
[pipeline_definitions.yaml](./example/pipeline_definitions.yaml) file.

This YAML file contains two sections:

#### pipeline_parameters
This section defines the entire pipeline parameters, 
description of each field:
- name: name of the transform
- description: description of the transform
- script_name: name of the tranfrom script
- prefix: prefix for trasnform arguments
- multi_s3: True if the transform uses multi sources (see Blocklisting pipeline). Otherwise, False
- compute_func_name: name of special compute function name (in empty string case it will take the default)
- compute_func_import: import lines if needed for the compute function

#### pipeline_common_input_parameters_values
The common input parameters and their default values (users are able to change their values when running the pipeline).


##### pipeline_transform_input_parameters
The pipeline specific arguments, each entry includes the name of the parameter, its type, default value, and a description of the parameter.

## Steps to generate a new pipeline

- Update the [pipeline_definitions.yaml](./example/pipeline_definitions.yaml) file.
- Execute `make venv'. This step should be done once to build python virtual environment.
- Execute `make PIPELINE_DIST_DIR=".." generate_pipeline` command. This command will generate a new pipeline definition in the directory specified by the `PIPELINE_DIST_DIR` variable.
