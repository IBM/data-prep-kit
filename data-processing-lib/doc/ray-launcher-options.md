# Ray Launcher Command Line Options
A number of command line options are available when launching a transform.  

The following is a current --help output (a work in progress) for 
the `NOOPTransform` (note the --noop_sleep_sec and --noop_pwd options):

```
usage: noop_transform.py [-h] [--run_locally RUN_LOCALLY] [--noop_sleep_sec NOOP_SLEEP_SEC] [--noop_pwd NOOP_PWD] [--data_s3_cred DATA_S3_CRED] [--data_s3_config DATA_S3_CONFIG] [--data_local_config DATA_LOCAL_CONFIG]
                         [--data_max_files DATA_MAX_FILES] [--data_checkpointing DATA_CHECKPOINTING] [--data_data_sets DATA_DATA_SETS] [--data_files_to_use DATA_FILES_TO_USE] [--data_num_samples DATA_NUM_SAMPLES]
                         [--runtime_num_workers RUNTIME_NUM_WORKERS] [--runtime_worker_options RUNTIME_WORKER_OPTIONS] [--runtime_creation_delay RUNTIME_CREATION_DELAY] [--runtime_pipeline_id RUNTIME_PIPELINE_ID]
                         [--runtime_job_id RUNTIME_JOB_ID] [--runtime_code_location RUNTIME_CODE_LOCATION]

Driver for noop processing

options:
  -h, --help            show this help message and exit
  --run_locally RUN_LOCALLY
                        running ray local flag
  --noop_sleep_sec NOOP_SLEEP_SEC
                        Sleep actor for a number of seconds while processing the data frame, before writing the file to COS
  --noop_pwd NOOP_PWD   A dummy password which should be filtered out of the metadata
  --data_s3_cred DATA_S3_CRED
                        AST string of options for s3 credentials. Only required for S3 data access.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: optional s3 url
                        region: optional s3 region
                        Example: { 'access_key': 'access', 'secret_key': 'secret', 
                        'url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud', 
                        'region': 'us-east-1' }
  --data_s3_config DATA_S3_CONFIG
                        AST string containing input/output paths.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': 's3-path/your-input-bucket', 
                        'output_folder': 's3-path/your-output-bucket' }
  --data_local_config DATA_LOCAL_CONFIG
                        ast string containing input/output folders using local fs.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': './input', 'output_folder': '/tmp/output' }
  --data_max_files DATA_MAX_FILES
                        Max amount of files to process
  --data_checkpointing DATA_CHECKPOINTING
                        checkpointing flag
  --data_data_sets DATA_DATA_SETS
                        List of sub-directories of input directory to use for input. For example, ['dir1', 'dir2']
  --data_files_to_use DATA_FILES_TO_USE
                        list of file extensions to choose for input.
  --data_num_samples DATA_NUM_SAMPLES
                        number of random input files to process
  --runtime_num_workers RUNTIME_NUM_WORKERS
                        number of workers
  --runtime_worker_options RUNTIME_WORKER_OPTIONS
                        AST string defining worker resource requirements.
                        num_cpus: Required number of CPUs.
                        num_gpus: Required number of GPUs
                        resources: The complete list can be found at
                                   https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options
                                   and contains accelerator_type, memory, name, num_cpus, num_gpus, object_store_memory, placement_group,
                                   placement_group_bundle_index, placement_group_capture_child_tasks, resources, runtime_env,
                                   scheduling_strategy, _metadata, concurrency_groups, lifetime, max_concurrency, max_restarts,
                                   max_task_retries, max_pending_calls, namespace, get_if_exists
                        Example: { 'num_cpus': '8', 'num_gpus': '1', 
                        'resources': '{"special_hardware": 1, "custom_label": 1}' }
  --runtime_creation_delay RUNTIME_CREATION_DELAY
                        delay between actor' creation
  --runtime_pipeline_id RUNTIME_PIPELINE_ID
                        pipeline id
  --runtime_job_id RUNTIME_JOB_ID
                        job id
  --runtime_code_location RUNTIME_CODE_LOCATION
                        AST string containing code location
                        github: Github repository URL.
                        commit_hash: github commit hash
                        path: Path within the repository
                        Example: { 'github': 'https://github.com/somerepo', 'commit_hash': '1324', 
                        'path': 'transforms/universal/code' }
```
