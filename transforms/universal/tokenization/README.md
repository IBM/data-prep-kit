# Data Tokenization
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The data tokenization transform maps an input table to an output table using a pre-trained tokenizer.
The input table must contain at least two columns, by default named `document_id` and `contents`,
and can be specified through `--tkn_doc_id_column` and `--tkn_doc_content_column` respectively.
The `document_id` should be unique within the dataset (across all rows) and the `contents` stores
its corresponding document content.

A pre-trained tokenizer must be specified through `--tkn_tokenizer_path` parameter 
which can either be to a folder containing the pre-trained tokenizer, or a ready-for-download tokenizer
from Huggingface compatible with `AutoTokenizer` library (also from Huggingface).

The tokenization transform will use the pre-trained tokenizer to tokenize each row in the input table
to each row in the output folder as a sequence of token_ids under the `tokens` column. 
The document id and the token count are respectively stored in the `document_id` (or name specified in `--tkn_doc_id_column`) and `token_count`. 
The tokenizer will skip empty rows in the input table or rows leading to failure and track their counting in the `metadata`.


The parameter `--tkn_chunk_size` is used when each document is tokenized by chunks (of characters). Its defaut value is `0` 
which tokenize each document as a whole no matter how long it is. Chunks are round up by words, that means, the last word in a chunk
will not be split into half. Though this works for most languages having spaces among words, there is a very preliminary version
for languages having no space among words such as `ja` and it is highly recommended to customize such script for each particular language (specified via `--tkn_text_lang`)

## Running
You can run the [tokenization_local.py](src/tokenization_local.py) to
transform all three parquet files (some are in a sub-directory) in [test input data](test-data/input) 
to [output](output) directory. This directory will contain both sub-directory and the new three
tokenized parquet files and the `metadata.json` file.
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
