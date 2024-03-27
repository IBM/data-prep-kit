# Data Tokenization
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The data tokenization transform maps a (no-empty) input table to an output table using a pre-trained tokenizer.
The input table must contain at least two columns, by default named `document_id` and `contents`,
and can be specified through `--tkn_doc_id_column` and `--tkn_doc_content_column` respectively.
The `document_id` should be unique within the dataset and the `contents` stores
its corresponding document content.

A pre-trained tokenizer must be specified through `--tkn_tokenizer` parameter 
which can be the name of a ready-for-download tokenizer
from Huggingface such as `hf-internal-testing/llama-tokenizer`, `bigcode/starcoder` or those being loadable by Huggingface `AutoTokenizer` library.
Parameter `--tkn_tokenizer_args` can be further used to specify extra arguments for the corresponding tokenizer. For example,
`cache_dir=/tmp/hf,token=user_auth_token` could be used for tokenizer `bigcode/starcoder`.

The tokenization transform utilizes the pre-trained tokenizer to tokenize each row (assuming a document) in the input table
to each row in the output folder. There are four columns in the output tables named `tokens,document_id,document_length,token_count`. 
The `tokens` stores the sequence of token_ids returned by the tokenizer in tokenizing the document. The `document_id` (or the name specified in `--tkn_doc_id_column`) stores the document id,
while `document_length,token_count` respectively stores the length of the document and the total token count. 
The tokenizer will skip empty rows/documents in the input table or rows returned no tokens or failure by the tokenizer. 
The count of such rows will be stored in the `num_empty_rows` of the `metadata` file.

For some tokenizers, their tokenization process could be slow for long documents with millions of characters.
In such case, parameter `--tkn_chunk_size` should be used to specify the length to spit a document into chunks
(for `en` text, this parameter should be set to `20000`, equivalently to 15 pages) . 
The tokenizer will tokenize each chunk individually and concatenate their returned token_ids. 
The default value for `--tkn_chunk_size` is `0` 
which tokenizes each document as a whole no matter how long it is. 


## Running
You can run the [tokenization_local.py](src/tokenization_local.py) to
transform all parquet files (some are in sub-directories) in [test input data](test-data/ds01/input) 
to [output](output) directory. This directory will contain both sub-directories and the transformed (tokenized)
parquet files and the `metadata.json` file. It will skip empty parquet files in folder [dataset=empty](test-data/ds01/input/lang=en/dataset=empty) 
<pre>
% make venv
% source venv/bin/activate
(venv) % cd src
(venv) % python tokenization_local.py
17:07:48 INFO - Running locally
17:07:48 INFO - Using local configuration with: input_folder - /Users/xdang/00proj/04-FM/01_code/fm-data-engineering/transforms/universal/tokenization/test-data/input output_folder - /Users/xdang/00proj/04-FM/01_code/fm-data-engineering/transforms/universal/tokenization/output
17:07:48 INFO - Not using data sets, checkpointing False, max files -1
17:07:48 INFO - number of workers 5 worker options {'num_cpus': 0.8}
17:07:48 INFO - pipeline id pipeline_id; number workers 5
17:07:48 INFO - job details {'job category': 'preprocessing', 'job name': 'Tokenization', 'job type': 'ray', 'job id': 'job_id'}
17:07:48 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
17:07:48 INFO - actor creation delay 0
2024-03-20 17:07:50,700	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265
(orchestrate pid=91756) None of PyTorch, TensorFlow >= 2.0, or Flax have been found. Models won't be available and only tokenizers, configuration and file/data utilities can be used.
(orchestrate pid=91756) 17:07:51 INFO - orchestrator started at 2024-03-20 17:07:51
(orchestrate pid=91756) 17:07:51 INFO - Number of files is 3, source profile {'max_file_size': 0.0026502609252929688, 'min_file_size': 0.0024614334106445312, 'total_file_size': 0.007695198059082031}
(orchestrate pid=91756) 17:07:51 INFO - Cluster resources: {'cpus': 10, 'gpus': 0, 'memory': 40.44677734375, 'object_store': 2.0}
(orchestrate pid=91756) 17:07:51 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=91756) 17:07:51 INFO - Completed 0 files in 4.398822784423828e-06 min. Waiting for completion
17:07:53 INFO - Completed orchestrator
(orchestrate pid=91756) 17:07:53 INFO - Completed processing in 0.022802833716074625 min
17:08:03 INFO - Completed execution in 0.237752366065979 min, execution result 0
(TransformTableProcessor pid=91764) None of PyTorch, TensorFlow >= 2.0, or Flax have been found. Models won't be available and only tokenizers, configuration and file/data utilities can be used. [repeated 5x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(venv) % deactivate
% ls -R ../output
lang=en		metadata.json	pq03.parquet

../output/lang=en:
pq01.parquet	pq02.parquet
%
</pre>



### Launched Command Line Options 
When running the transform with the Ray launcher,
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
```
  --run_locally RUN_LOCALLY
                        running ray local flag
  --tkn_tokenizer TKN_TOKENIZER
                        Tokenizer used for tokenization. It also can be a path to a pre-trained tokenizer. By defaut, `hf-internal-testing/llama-tokenizer` from HuggingFace is used
  --tkn_tokenizer_args TKN_TOKENIZER_ARGS
                        Arguments for tokenizer. For example, `cache_dir=/tmp/hf,use_auth_token=Your_HF_authentication_token` could be arguments for tokenizer `bigcode/starcoder` from HuggingFace
  --tkn_doc_id_column TKN_DOC_ID_COLUMN
                        Column contains document id which values should be unique across dataset
  --tkn_doc_content_column TKN_DOC_CONTENT_COLUMN
                        Column contains document content
  --tkn_text_lang TKN_TEXT_LANG
                        Specify language used in the text content for better text splitting if needed
  --tkn_chunk_size TKN_CHUNK_SIZE
                        Specify >0 value to tokenize each row/doc in chunks of characters (rounded in words)
  --s3_cred S3_CRED     AST string of options for cos credentials. Only required for COS or Lakehouse.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: S3 url
                        Example: { 'access_key': 'AFDSASDFASDFDSF ', 'secret_key': 'XSDFYZZZ', 'url': 's3:/cos-optimal-llm-pile/test/' }
  --s3_config S3_CONFIG
                        AST string containing input/output paths.
                        input_path: Path to input folder of files to be processed
                        output_path: Path to output folder of processed files
                        Example: { 'input_path': '/cos-optimal-llm-pile/bluepile-processing/xh/opensource/input', 'output_path': '/cos-optimal-llm-pile/bluepile-processing/xh/opensource/output' }
 
  --local_config LOCAL_CONFIG
                        ast string containing input/output folders using local fs.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': './input', 'output_folder': '/tmp/output' }
  --max_files MAX_FILES
                        Max amount of files to process
  --checkpointing CHECKPOINTING
                        checkpointing flag
  --data_sets DATA_SETS
                        List of data sets
  --num_workers NUM_WORKERS
                        number of workers
  --worker_options WORKER_OPTIONS
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
