# Repo Level Order Ray Transform 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This transform can group the data by `repo_name` and apply additional transformations like( sorting or output_by_language or combining rows) on the  grouped data.
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

This transform is dependant on ray runtime. 

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


Limitation of transform. This expects a flat input folder structure with parquet files.


## Running

### Launched Command Line Options 

Ray runtime options of [ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md) are available.

### Running the samples

To run the samples, use the following `make` targets

* `run-local-sample` - runs src/repo_level_order_local_ray.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```sh

make run-local-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.


## Using the CLI

1. Simplest Usage:

Running on local computer setup with data available on local filesystem.
Sorting the data at repo level using default algorithm. (SORT_BY_PATH).
In this configuration, the workers run locally, store is local.
This is a recommended method if number of available cpus is less.

```sh
export INPUT_FOLDER="input_data_folder/"
export OUTPUT_FOLDER="output_data_folder/"


local_conf="{'input_folder' : '$INPUT_FOLDER', 'output_folder' : '$OUTPUT_FOLDER'  }"
rm -rf /tmp/mystore # remove if it exists

python src/repo_level_order_transform_ray.py \
       --run_locally True \
       --data_local_config "$local_conf" \
       --repo_lvl_store_type  local  \
       --repo_lvl_store_backend_dir '/tmp/mystore' \
       --repo_lvl_sorting_enabled true,
       --repo_lvl_sorting_algo SORT_SEMANTIC
```

NOTE: Make sure you use an empty folder as `repo_lvl_store_backend_dir`. 


2. Running on Cluster

This configuration can be used when data is on S3 like COS storage and computation is done
on a ray cluster.

Recommended `repo_lvl_store_type` for cluster is 'ray'. We need to dedicate some ray actors specially for
ray store

for example, if we have a ray cluster with 100 actors available. We can dedicate some for the store. Let us randomly
use 35 for computation and 10 for store and let remaining free. 

NOTE: The transform reads parquet files in two stages and each stage has its own actor pool for computations. The 
actor pool to read parquet data in the first stage is managed by the library and the actor pool used in the second stage
is managed by the ray runtime of the transform. The number of actors in each pool is same and is configured using `--runtime_num_workers`.
So we have to carefully choose number of actors required based on the number resources available. 
The number of workers/actors should be less than 35% of total resources available. 

We need to add the following cli args:

 `--runtime_num_workers 35` 
 `--repo_lvl_store_type "ray" --repo_lvl_store_ray_cpus 0.2 --repo_lvl_store_ray_nworkers 10` 

When we want the output with the following configuration enabled:

> NOTE: store_backend=`s3/local` are persistent stores and retain the mappings stores in them, they need to be cleaned/deleted after use. It is recommented to use a different location for store if data is different. They are added to aid in large data processing in multiple stages.
  store_backend=`ray`, is not presistent and ephimeral and does not retain any data. It is the simplest to use if resources are available.
 

```sh
export INPUT_FOLDER="input_cos_bucket"
export OUTPUT_FOLDER="output_cos_bucket"

s3_kreds="{ ... }" # in the deafult way used for all transforms. 
s3_conf="{'input_folder' : '$INPUT_FOLDER', 'output_folder' : '$OUTPUT_FOLDER'  }"

python src/repo_level_order_transform_ray.py \
       --run_locally True \
       --data_s3_cred "$s3_kreds" \
       --data_s3_config "$s3_conf" \
       --repo_lvl_store_type  local  \
       --repo_lvl_store_backend_dir '/tmp/mystore' \
       --repo_lvl_combine_rows True\
       --repo_lvl_sorting_enabled True\
       --repo_lvl_sorting_algo SORT_SEMANTIC \
       --repo_lvl_output_by_langs True   
```
