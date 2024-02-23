# Launcher Command Line Options
A number of command line options are available when launching a transform.  

The following is a current --help output (a work in progress) for 
the `NOOPTransform` (note the --noop_sleep_sec option):

```
usage: noop_implementation.py [-h] [--run_locally RUN_LOCALLY]
                              [--noop_sleep_sec NOOP_SLEEP_SEC]
                              [--s3_cred S3_CRED] [--s3_config S3_CONFIG]
                              [--lh_config LH_CONFIG]
                              [--local_config LOCAL_CONFIG]
                              [--max_files MAX_FILES]
                              [--checkpointing CHECKPOINTING]
                              [--data_sets DATA_SETS]
                              [--num_workers NUM_WORKERS]
                              [--worker_options WORKER_OPTIONS]
                              [--pipeline_id PIPELINE_ID] [--job_id JOB_ID]
                              [--creation_delay CREATION_DELAY]
                              [--code_location CODE_LOCATION]

Driver for NOOP processing

options:
  -h, --help            show this help message and exit
  --run_locally RUN_LOCALLY
                        running ray local flag
  --noop_sleep_sec NOOP_SLEEP_SEC
                        Sleep actor for a number of seconds while processing the data frame, before writing the file to COS
  --s3_cred S3_CRED     AST string of options for cos credentials. Only required for COS or Lakehouse.
                        access_key: access key help text
                        secret_key: secret key help text
                        cos_url: COS url
                        Example: { 'access_key': 'AFDSASDFASDFDSF ', 'secret_key': 'XSDFYZZZ', 'cos_url': 's3:/cos-optimal-llm-pile/test/' }
  --s3_config S3_CONFIG
                        AST string containing input/output paths.
                        input_path: Path to input folder of files to be processed
                        output_path: Path to outpu folder of processed files
                        Example: { 'input_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup', 'output_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed' }
  --lh_config LH_CONFIG
                        AST string containing input/output using lakehouse.
                        input_table: Path to input folder of files to be processed
                        input_dataset: Path to outpu folder of processed files
                        input_version: Version number to be associated with the input.
                        output_table: Name of table into which data is written
                        output_path: Path to output folder of processed files
                        token: The token to use for Lakehouse authentication
                        lh_environment: Operational environment. One of STAGING or PROD
                        Example: { 'input_table': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup', 'input_dataset': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed', 'input_version': '1.0', 'output_table': 'ededup', 'output_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed', 'token': 'AASDFZDF', 'lh_environment': 'STAGING' }
  --local_config LOCAL_CONFIG
                        ast string containing input/output folders using local fs.
                        input_path: Path to input folder of files to be processed
                        output_path: Path to output folder of processed files
                        Example: { 'input_path': './input', 'output_path': '/tmp/output' }
  --max_files MAX_FILES
                        Max amount of files to process
  --checkpointing CHECKPOINTING
                        checkpointing flag
  --data_sets DATA_SETS
                        List of data sets
  --num_workers NUM_WORKERS
                        number of workers
  --worker_options WORKER_OPTIONS
                        ast string of options for worker execution
  --pipeline_id PIPELINE_ID
                        pipeline id
  --job_id JOB_ID       job id
  --creation_delay CREATION_DELAY
                        delay between actor' creation
  --code_location CODE_LOCATION
                        AST string containing code location
                        github: Github repository URL.
                        commit_hash: github commit hash
                        path: Path within the repository
                        Example: { 'github': 'https://github.com/somerepo', 'commit_hash': '13241231asdfaed', 'path': 'transforms/universal/ededup' }
```