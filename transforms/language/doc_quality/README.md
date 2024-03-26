# Document Quality Annotations
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The document quality transform maps an input table to an output table utilizing a doc quality annotator.
The input table must contain at least two columns, by default named `document_id` and `contents`, and can be specified through --docq_doc_id_column and --docq_doc_content_column respectively. 
The content stores its corresponding document content.
The doc quality transformer will add document quality metrics to the input table. This release consists of eight metrics, 
including the criteria described in [Deepmind's Gopher paper](https://arxiv.org/pdf/2112.11446.pdf) and the [perplexity
estimation implementation by Kenneth Heafield](https://github.com/kpu/kenlm). 

 A pre-trained kenLM+sp must be specified through `--model_dir` parameter 
which is a folder containing the pre-trained kenLM+sp.
The document quality transform will use the pre-trained kenLM+sp to calculate the perplexity score
of each row in the input table to each row in the output folder under the `metakenlm_docq_perplex_score` column.

## Code references
### Metrics descriptions
- Gopher (Deepmind): filter docs that
    + do not contain between 50 and 100,000 words
    + mean word length is outside the range of 3 to 10 characters; 
    + symbol-to-word ratio > 0.1 for either the hash symbol or the ellipsis; 
    + \> 90% of lines starting with a bullet point, 
    + \> 30% ending with an ellipsis. 
    + Require that 80% of words in a document contain at least one alphabetic character, 
    and apply a "stop word" filter, to remove documents that do NOT contain at least TWO of the following English words: 
    the, be, to, of, and, that, have, with; this adequately deals with ostensibly English documents 
    that contain no coherent English text.


- Perplexity score (KenLM+sp) suggested in Gopher
The smaller the perplexity score, the closer is the text to the targeted
domain (i.e., en Wikipedia). Journalistic and well written content. Distribution of perplexity for different languages may have different
shapes.

## Running
You can run the [doc_quality_local.py](src/docquality_local.py) to
transform all parquet files in [test input data](test-data/input) 
to [output](output) directory. This directory will contain both the new
annotated parquet files and the `metadata.json` file.
<pre>
% make venv
% source venv/bin/activate
(venv) % cd src
(venv) % python docquality_local.py
17:19:06 INFO - Running locally
17:19:06 INFO - Using local configuration with: input_folder - /Users/hajaremami/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/test-data/input output_folder - /Users/hajaremami/GUF_hajar/fm-data-engineering/transforms/language/doc_quality/output
17:19:06 INFO - Not using data sets, checkpointing False, max files -1
17:19:06 INFO - number of workers 5 worker options {'num_cpus': 0.8}
17:19:06 INFO - pipeline id pipeline_id; number workers 5
17:19:06 INFO - job details {'job category': 'preprocessing', 'job name': 'DocQuality', 'job type': 'ray', 'job id': 'job_id'}
17:19:06 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
17:19:06 INFO - actor creation delay 0
2024-03-26 17:19:08,070 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
(orchestrate pid=65150) 17:19:09 INFO - orchestrator started at 2024-03-26 17:19:09
(orchestrate pid=65150) 17:19:09 INFO - Number of files is 1, source profile {'max_file_size': 0.0009870529174804688, 'min_file_size': 0.0009870529174804688, 'total_file_size': 0.0009870529174804688}
(orchestrate pid=65150) 17:19:09 INFO - Cluster resources: {'cpus': 10, 'gpus': 0, 'memory': 36.96530609205365, 'object_store': 2.0}
(orchestrate pid=65150) 17:19:09 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=65150) 17:19:09 INFO - Completed 0 files in 3.250439961751302e-06 min. Waiting for completion
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == PATH TO PRETRAINED KenLM and SENTENCEPIECE: ../lm_sp/
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == LANGUAGE: en
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == STRIP ACCENT: True
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == CONVERT TO LOWER CASE: True
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == CONVERT DIGITS TO ZERO: True
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,096 - perplexity - INFO - == LEVEL OF REPLACING PUNCTUATION: 1
(TransformTableProcessor pid=65193) 2024-03-26 17:19:10,735 - perplexity - INFO - == PRE-TRAINED SENTENCE PIECE: ../lm_sp/en.sp.model
(orchestrate pid=65150) 17:19:12 INFO - Completed processing in 0.06200236479441325 min
17:19:22 INFO - Completed execution in 0.27724766731262207 min, execution result 0
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == PATH TO PRETRAINED KenLM and SENTENCEPIECE: ../lm_sp/ [repeated 4x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == LANGUAGE: en [repeated 4x across cluster]
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == STRIP ACCENT: True [repeated 4x across cluster]
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == CONVERT TO LOWER CASE: True [repeated 4x across cluster]
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == CONVERT DIGITS TO ZERO: True [repeated 4x across cluster]
(TransformTableProcessor pid=65194) 2024-03-26 17:19:10,139 - perplexity - INFO - == LEVEL OF REPLACING PUNCTUATION: 1 [repeated 4x across cluster]
(TransformTableProcessor pid=65194) 2024-03-26 17:19:12,878 - perplexity - INFO - == PRE-TRAINED SENTENCE PIECE: ../lm_sp/en.sp.model [repeated 4x across cluster]

(venv) % deactivate
% ls -R ../output
metadata.json   test1.parquet
%
</pre>



### Launched Command Line Options 
When running the transform with the Ray launcher,
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
```
  --run_locally RUN_LOCALLY
                        running ray local flag
  --ft_lang FT_LANG
                        Specify language used in the text content if needed. By defaut, "en" is used
  --docq_doc_content_column DOCQ_DOC_CONTENT_COLUMN
                        Column contains document content
  --bad_word_filepath BAD_WORD_FILEPATH
                        a path to bad word file. By defaut, `../test-data/docq/ldnoobw/` is used
  --model_dir MODEL_DIR 
                        a path to kenLM model. By defaut, `../lm_sp/` is used
  --s3_cred S3_CRED     AST string of options for cos credentials. Only required for COS or Lakehouse.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: S3 url
                        Example: { 'access_key': 'AFDSASDFASDFDSF ', 'secret_key': 'XSDFYZZZ', 'url': 's3:/cos-optimal-llm-pile/test/' }
  --s3_config S3_CONFIG
                        AST string containing input/output paths.
                        input_path: Path to input folder of files to be processed
                        output_path: Path to output folder of processed files
                        Example: { 'input_path': 'cos-optimal-llm-pile/test/hajar/input/', 'output_path': 'cos-optimal-llm-pile/test/hajar/output/' }
 
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
