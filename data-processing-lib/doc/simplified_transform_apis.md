# Simplified APIs for invoking transforms

Current transform invocation, requires defining transform parameters, passing them to `sys.argv` and 
then invoking launcher (runtime specific) with transform/runtime specific configuration (see for example
[NOOP Python invocation](../../transforms/universal/noop/python/src/noop_local_python.py),
[NOOP Ray invocation](../../transforms/universal/noop/ray/src/noop_local_ray.py) and
[NOOP Spark invocation](../../transforms/universal/noop/spark/src/noop_local_spark.py)). 

Simplified APIs, described here make invocations a little simpler by eliminating the need for the boilerplate code.
Currently we provide 2 APIs for simplified transform invocation:
* [execute_python_transform](../python/src/data_processing/runtime/pure_python/transform_invoker.py)
* [execute_ray_transform](../ray/src/data_processing_ray/runtime/ray/transform_invoker.py)

Both APIs look the same, defining runtime in the API name and accept the following parameters:
* transform name
* transforms configuration object (see below)
* input_folder containing data to be processed, currently can be local file system or S3 compatible
* output_folder defining where execution results will be placed, currently can be local file system or S3 compatible
* S3 configuration, required only for input/output folders in S3
* transform params - a dictionary of transform specific parameters

APIs returns `True` if transform execution succeeds or `False` otherwise

APIs implementation is leveraging [TransformsConfiguration class](../python/src/data_processing/utils/transform_configurator.py)
which manages configurations of all existing transforms. By default transforms information is loaded
from this [json file](../python/src/data_processing/utils/transform_configuration.json), but can be overwritten
by the user.

Additionally configuration provides the method for listing existing (known) transforms.

Finally, as configurator knows  about all existing transforms (and their dependencies) it checks
whether transform is code is installed locally and install it if it is not (using 
[PipInstaller](../python/src/data_processing/utils/pipinstaller.py)). If transforms are installed, they
are removed after transform execution is complete.

An example of the APIs usage can be found in this [notebook](../../examples/notebooks/code/demo_with_apis.ipynb).

Creation and usage of configuration: 

```python
from data_processing.utils import TransformsConfiguration

t_configuration = TransformsConfiguration()
transforms = t_configuration.get_available_transforms()
```

Invoking Python transform:

```python
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}

runtime_python_params = {
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
}
input_folder = os.path.abspath(zip_input_folder)
output_folder =  os.path.abspath(parquet_data_output)
supported_languages_file = os.path.abspath("../../../transforms/code/code2parquet/python/test-data/languages/lang_extensions.json")

ingest_config = {
    "data_files_to_use": ast.literal_eval("['.zip']"),
    "code2parquet_supported_langs_file": supported_languages_file,
    "code2parquet_detect_programming_lang": True,
}

execute_python_transform(
    configuration = t_configuration,
    name="code2parquet",
    input_folder=input_folder,
    output_folder=output_folder,
    params=runtime_python_params | ingest_config
)    
```

In the fragment above, we first define Python runtime parameters. Most of them are defaulted, so their use is optional
(only if we need to overwrite them). Then we define input/output folder and location of the support file.
We also define code to parquet specific parameters and finally invoke the transform itself.
Note here, that `runtime_python_params` can be defined once and then reused across several Python transform
invocation.

Invoking Python transform:

```python
runtime_ray_params = {
    "runtime_worker_options": ParamsUtils.convert_to_ast(worker_options),
    "runtime_num_workers": 3,
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_creation_delay": 0,
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
}

ededup_config = {
    "ededup_hash_cpu": 0.5,
    "ededup_num_hashes": 2,
    "ededup_doc_column": "contents",
}

execute_ray_transform(
    configuration = t_configuration,
    name="ededup",
    input_folder=input_folder,
    output_folder=output_folder,
    params=runtime_ray_params | ededup_config
)    
```

In the fragment above, we first define Ray runtime parameters. Most of them are defaulted, so their use is optional
(only if we need to overwrite them). Then we define input/output folder and ededup specific parameters and finally 
invoke the transform itself.
Note here, that `runtime_ray_params` can be defined once and then reused across several Python transform
invocation.