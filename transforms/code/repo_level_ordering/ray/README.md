# Repo Level Order Ray Transform 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This transform requires the input data to have at least the following columns: 

- repo name: Name of the repo, it is used for grouping in this transform.
- title : Which is usually file path.
- language: Programming language of content

The input data for this transform should be in parquet format. The input data is expected to have code data arranged in rows
such that each row represents a file. The required columns in the input data shoud correspond to a) repository name b) file path
c) content. This transform supports searching the repo across the whole dataset and writing the files of a repo as a single 
parquet file. 

The transform gives the option to write the repo to file in the following ways.

a) sort the repo content by file path and write a parquet with multiple rows
b) sort the repo content by file path and write a parquet with a single combined row.
c) sort the repo content in semantic ordering and write the parquet with multiple rows.
d) sort the repo content in semantic ordering and write the parquet with a single combined row.

Additionally this transform can grooup the repos in the folders named after the most dominant language in the repo. For more information on this transform, please refer to [here](https://arxiv.org/pdf/2407.13739).


## Configuration and command line Options

repo_level_order configuration and command line options are the same as for the base python transform. 

Transform Configuration.

- For output:
   either the output is directly a file or if dominant language flag is enabled, it should output
   it in folder of that langauge.
- Enable sorting: 
   if sorting is enabled, we should be able to choose one of the available ones (SORT_BY_PATH, SORT_SEMANTIC, SORT_SEMANTIC_NORMALISED)
     - SORT_BY_PATH: Normal ascending sort by filename
     - SORT_SEMANTIC: Uses semantic analysis to sort the files within a repo.
     - SORT_SEMANTIC_NORMALISED: Normalises the title to remove https:// prefix, if present, and then runs SORT_SEMANTIC
- Enable superrows:
   of writing superrows is enabled, then it combines rows of a repo to a single row otherwise writes normal.


Limitation of transform. This expects a flat input folder structure with parquets.


## Running

### Launched Command Line Options 

Ray runtime options of [ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md) are available.

### Running the samples

To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/repo_level_order_transform.py using command line args
* `run-local-sample` - runs src/repo_level_order_local_ray.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.


## Using the CLI

Simplest Usage:

All data in input is divided by parquet 

```sh
export INPUT_FOLDER="input_data_folder/"
export OUTPUT_FOLDER="output_data_folder/"


local_conf="{'input_folder' : '$INPUT_FOLDER', 'output_folder' : '$OUTPUT_FOLDER'  }"

python src/repo_level_order_transform_ray.py \
       --run_locally True \
       --data_local_config "$local_conf" \
       --repo_lvl_store_type  local  \
       --repo_lvl_store_backend_dir '/tmp/mystore' \
       --repo_lvl_sorting_enabled \
       --repo_lvl_sorting_algo SORT_SEMANTIC \
```


Case 1:

When we want the output with the following configuration enabled:
- repos should be sorted by semantic order
- output should be grouped by dominant language per repo
- all files of repo must be combined to a single row in the output.
- data is local
- store backend : local filsystem
(store_backend: s3/local are persistent stores and retain the mappings stores in them, they need to be cleaned/deleted after use. It is recommented to use a different location for store if data is different. They are added to aid in large data processing in multiple stages.

store_backend: ray, is not presistent and ephimeral and does not retain any data.
  It is the simplest to use if resources are available.
)

```sh
export INPUT_FOLDER="input_data_folder/"
export OUTPUT_FOLDER="output_data_folder/"


local_conf="{'input_folder' : '$INPUT_FOLDER', 'output_folder' : '$OUTPUT_FOLDER'  }"

python src/repo_level_order_transform_ray.py \
       --run_locally True \
       --data_local_config "$local_conf" \
       --repo_lvl_store_type  local  \
       --repo_lvl_store_backend_dir '/tmp/mystore' \
       --repo_lvl_combine_rows \
       --repo_lvl_sorting_enabled \
       --repo_lvl_sorting_algo SORT_SEMANTIC \
       --repo_lvl_output_by_langs
        
```

Case 2:

When data is on cloud and 

```shell


export INPUT_FOLDER="input_bucket/input_dir"
export OUTPUT_FOLDER="output_bucket/output_dir"

s3_kreds="{ ... }" # in the deafult way used for all transforms. 
s3_conf="{'input_folder' : '$INPUT_FOLDER', 'output_folder' : '$OUTPUT_FOLDER'  }"

python src/repo_level_order_transform_ray.py \
       --run_locally True \
       --data_s3_cred "$s3_kreds" \
       --data_s3_config "$s3_conf" \
       --repo_lvl_store_type  local  \
       --repo_lvl_store_backend_dir '/tmp/mystore' \
       --repo_lvl_combine_rows \
       --repo_lvl_sorting_enabled \
       --repo_lvl_sorting_algo SORT_SEMANTIC \
       --repo_lvl_output_by_langs   
```
