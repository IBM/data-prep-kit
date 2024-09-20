# Running on Google Colab

[Google Colab](https://colab.research.google.com/) is free hosted Jupyter environment.

Here are some tips for running data prep kit application code on Google Colab.

## How to Determine if We are Running on Colab?

Use one of these code snippets

```python
import os

if os.getenv("COLAB_RELEASE_TAG"):
   print("Running in Colab")
   RUNNING_IN_COLAB = True
else:
   print("NOT in Colab")
   RUNNING_IN_COLAB = False
```

## Installing Dependencies

We need to install data prep kit and libraries in Colab

```python
if RUNNING_IN_COLAB:
    ! pip install  --default-timeout=100  data-prep-toolkit-transforms-ray==0.2.1.dev3
```

## Downloading Data Files on Colab

```python
if RUNNING_IN_COLAB:
    !mkdir -p 'input'
    !wget -O 'input/1.pdf'  'remote_file_url'
```

## Ray Runtime Settings

These are some recommended settings for running RAY based notebooks.  You can use these as starting points and tweak for your application

```python
if RUNNING_IN_COLAB:
  RAY_RUNTIME_WORKERS = 2
  RAY_NUM_CPUS =  0.3
  RAY_MEMORY_GB = 2  # GB
```

It is recommended to set cpu per worker (RAY_NUM_CPUS) to a low number.  Otherwise Ray jobs seem to hang and will not complete.

## Fuzzy Dedupe Settings

Start with the following settings before launching fuzzy dedupe job.


Here is the **infrastructure** section for fuzzy dedupe.  Again we recommend to keep CPU share low.

```python
    # infrastructure
    "fdedup_bucket_cpu": 0.3,
    "fdedup_doc_cpu": 0.3,
    "fdedup_mhash_cpu": 0.3,
    "fdedup_num_doc_actors": 1,
    "fdedup_num_bucket_actors": 1,
    "fdedup_num_minhash_actors": 1,
    "fdedup_num_preprocessors": 1,
```

Here is full code for completeness

```python
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
}
worker_options = {"num_cpus" : RAY_NUM_CPUS}

params = {
    # where to run
    "run_locally": True,
    # Data access. Only required parameters are specified
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # Orchestration parameters
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": RAY_RUNTIME_WORKERS,
    # columns used
    "fdedup_doc_column": "contents",
    "fdedup_id_column": "int_id_column",
    "fdedup_cluster_column": "hash_column",
    # infrastructure
    "fdedup_bucket_cpu": 0.3,
    "fdedup_doc_cpu": 0.3,
    "fdedup_mhash_cpu": 0.3,
    "fdedup_num_doc_actors": 1,
    "fdedup_num_bucket_actors": 1,
    "fdedup_num_minhash_actors": 1,
    "fdedup_num_preprocessors": 1,
    # fuzzy parameters
    "fdedup_num_permutations": 64,
    "fdedup_threshold": 0.7,
    "fdedup_shingles_size": 5,
    "fdedup_delimiters": " "
}

# Pass commandline params
sys.argv = ParamsUtils.dict_to_req(d=params)

launcher = RayTransformLauncher(FdedupRayTransformConfiguration())
return_code = launcher.launch()
```