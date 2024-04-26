<p align="Left"> Distributed tokenization module for data sets using any Hugging Face compatible tokenizer.
    <br> 
</p>

## 📝 Table of Contents
- [Summary](#Summary)
- [Running](#Running)
- [CLI Options](#cli_options)

# Data Tokenization
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The data tokenization transform maps a (non-empty) input table to an output table using a pre-trained tokenizer.
The input table must contain at least two columns, by default named `document_id` and `contents`. Different column names can be specified through `--tkn_doc_id_column` and `--tkn_doc_content_column` respectively.
The value of each `document_id` should be unique within the dataset and the `contents` stores
its corresponding document content.

A pre-trained tokenizer must be specified through the `--tkn_tokenizer` parameter,
which can be the name of a ready-for-download tokenizer
from Hugging Face such as `hf-internal-testing/llama-tokenizer`, `bigcode/starcoder` or any others that can loaded by the Hugging Face `AutoTokenizer` library.
The `--tkn_tokenizer_args` parameter can be further used to specify extra arguments for the corresponding tokenizer. For example,
`use_auth_token=<your token>` could be used when loading HuggingFace tokenizers like `bigcode/starcoder`, that require an access token to be provided.

The tokenization transform utilizes the pre-trained tokenizer to tokenize each row (assuming a document) in the input table
to each row in the output folder. There are four columns in the output tables named `tokens,document_id,document_length,token_count`. 
The `tokens` stores the sequence of token_ids returned by the tokenizer in tokenizing the document. The `document_id` (or the name specified in `--tkn_doc_id_column`) stores the document id,
while `document_length,token_count` respectively stores the length of the document and the total token count. 
The tokenizer will skip empty rows/documents in the input table or rows returning no tokens or failure by the tokenizer.
The count of such rows will be stored in the `num_empty_rows` of the `metadata` file.

For some tokenizers, their tokenization process could be slow for long documents with millions of characters.
In such cases, the `--tkn_chunk_size` parameter can be used to specify the maximum length of chunks to tokenize at one time. For `en` text, this parameter should be set to `20000`, equivalently to 15 pages.
The tokenizer will tokenize each chunk individually and concatenate their returned token_ids. 
The default value for `--tkn_chunk_size` is `0` which tokenizes each document as a whole no matter how long it is. 


## Running

### CLI Options
When running the transform with the Ray launcher,
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
```
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
```

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/tokenization_transform.py using command line args
* `run-local-sample` - runs src/tokenization_local.py
* `run-local-ray-sample` - runs src/tokenization_local_ray.py
* `run-s3-ray-sample` - runs src/tokenization_s3_ray.py

These targes will activate the virtual environment and set up any configuration needed.
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
