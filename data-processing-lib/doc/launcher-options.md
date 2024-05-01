# Launcher Command Line Options
A number of command line options are available when launching a transform.  

The following is a current --help output (a work in progress) for 
the `NOOPTransform` (note the --noop_sleep_sec option):

```
usage: noop_transform.py [-h] 
                         [--run_locally RUN_LOCALLY]
                         [--noop_sleep_sec NOOP_SLEEP_SEC] 
                         [--data_s3_cred DATA_S3_CRED]
                         [--data_s3_config DATA_S3_CONFIG]
                         [--data_local_config DATA_LOCAL_CONFIG]
                         [--data_max_files DATA_MAX_FILES]
                         [--data_checkpointing DATA_CHECKPOINTING]
                         [--data_data_sets DATA_DATA_SETS]
                         [--data_max_files MAX_FILES]
                         [--data_files_to_use DATA_FILES_TO_USE]
                         [--data_num_samples DATA_NUM_SAMPLES]
                         [--runtime_num_workers NUM_WORKERS] 
                         [--runtime_worker_options WORKER_OPTIONS]
                         [--runtime_pipeline_id PIPELINE_ID] [--job_id JOB_ID]
                         [--runtime_creation_delay CREATION_DELAY]
                         [--runtime_code_location CODE_LOCATION]

Driver for NOOP processing

options:
  -h, --help            show this help message and exit
  --run_locally RUN_LOCALLY
                        running ray local flag
  --noop_sleep_sec NOOP_SLEEP_SEC
                        Sleep actor for a number of seconds while processing the data frame, before writing the file to COS
  --data_s3_cred S3_CRED     
                        AST string of options for cos credentials. Only required for COS or Lakehouse.
                        access_key: access key help text
                        secret_key: secret key help text
                        cos_url: COS url
                        Example: { 'access_key': 'access', 'secret_key': 'secret', 's3_url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud' }
  --data_s3_config S3_CONFIG
                        AST string containing input/output paths.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': 'your input folder', 'output_folder ': 'your output folder' }
  --data_local_config LOCAL_CONFIG
                        ast string containing input/output folders using local fs.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': './input', 'output_folder': '/tmp/output' }
  --data_max_files MAX_FILES
                        Max amount of files to process
  --data_checkpointing CHECKPOINTING
                        checkpointing flag
  --data_data_sets DATA_SETS
                        List of data sets
  --data_files_to_use DATA_FILES_TO_USE
                        files extensions to use, default .parquet
  --data_num_samples DATA_NUM_SAMPLES
                        number of randomply picked files to use
  --runtime_num_workers NUM_WORKERS
                        number of workers
  --runtime_worker_options WORKER_OPTIONS
                        AST string defining worker resource requirements.
                        num_cpus: Required number of CPUs.
                        num_gpus: Required number of GPUs
                        resources: The complete list can be found at
                                   https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options
                                   and contains accelerator_type, memory, name, num_cpus, num_gpus, object_store_memory, placement_group,
                                   placement_group_bundle_index, placement_group_capture_child_tasks, resources, runtime_env,
                                   scheduling_strategy, _metadata, concurrency_groups, lifetime, max_concurrency, max_restarts,
                                   max_task_retries, max_pending_calls, namespace, get_if_exists
                        Example: { 'num_cpus': '8', 'num_gpus': '1', 'resources': '{"special_hardware": 1, "custom_label": 1}' }
  --runtime_pipeline_id PIPELINE_ID
                        pipeline id
  --runtime_job_id JOB_ID       job id
  --runtime_creation_delay CREATION_DELAY
                        delay between actor' creation
  --runtime_code_location CODE_LOCATION
                        AST string containing code location
                        github: Github repository URL.
                        commit_hash: github commit hash
                        path: Path within the repository
                        Example: { 'github': 'https://github.com/somerepo', 'commit_hash': '13241231asdfaed', 'path': 'transforms/universal/ededup' }
```
