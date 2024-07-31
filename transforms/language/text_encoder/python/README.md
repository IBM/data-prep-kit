# Text Encoder Transform 

## Summary 
This transform is using sentence encoder models to create embeddings of the text in each row of the input .parquet table.

## Running

### Parameters

The transform can be tuned with the following parameters.


| Parameter  | Default  | Description  |
|------------|----------|--------------|
| `model_name`                    | `BAAI/bge-small-en-v1.5` | The HF model to use for encoding the text. |
| `content_column_name`           | `contents` | Name of the column containing the text to be encoded. |
| `output_embeddings_column_name` | `embeddings` | Column name to store the embeddings in the output table. |
| `output_path_column_name`       | `doc_path` | Column name to store the document path of the chunk in the output table. |

When invoking the CLI, the parameters must be set as `--text_encoder_<name>`, e.g. `--text_encoder_column_name_key=myoutput`.


### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/text_encoder_transform.py using command line args
* `run-local-sample` - runs src/text_encoder_local.py

These targets will activate the virtual environment and set up any configuration needed.
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

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
