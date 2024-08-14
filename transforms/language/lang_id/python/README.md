# Language Identification Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform will identify language of each text with confidence score with fasttext language identification model. [ref](https://huggingface.co/facebook/fasttext-language-identification)

## Configuration and command line Options

The set of dictionary keys holding [LangIdentificationTransform](src/lang_id_transform.py) 
configuration for values are as follows:

| Key name  | Default  | Description |
|------------|----------|--------------|
| _model_credential_ | _unset_ | specifies the credential you use to get model. This will be huggingface token. [Guide to get huggingface token](https://huggingface.co/docs/hub/security-tokens) |
| _model_kind_ | _unset_ | specifies what kind of model you want to use for language identification. Currently, only `fasttext` is available. |
| _model_url_ | _unset_ |  specifies url that model locates. For fasttext, this will be repo nme of the model, like `facebook/fasttext-language-identification` |
| _content_column_name_ | `contents` | specifies name of the column containing documents |
| _output_lang_column_name_ | `lang` | specifies name of the output column to hold predicted language code |
| _output_score_column_name_ | `score` | specifies name of the output column to hold score of prediction |

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --lang_id_model_credential LANG_ID_MODEL_CREDENTIAL   the credential you use to get model. This will be huggingface token.
  --lang_id_model_kind LANG_ID_MODEL_KIND   what kind of model you want to use for language identification. Currently, only `fasttext` is available.
  --lang_id_model_url LANG_ID_MODEL_URL   url that model locates. For fasttext, this will be repo name of the model, like `facebook/fasttext-language-identification`
  --lang_id_content_column_name LANG_ID_CONTENT_COLUMN_NAME   A name of the column containing documents
  --lang_id_output_lang_column_name LANG_ID_OUTPUT_LANG_COLUMN_NAME   Column name to store identified language
  --lang_id_output_score_column_name LANG_ID_OUTPUT_SCORE_COLUMN_NAME   Column name to store the score of language identification
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/lang_id_transform.py using command line args
* `run-local-sample` - runs src/lang_id_local.py

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

## Troubleshooting guide

For M1 Mac user, if you see following error during make command, `error: command '/usr/bin/clang' failed with exit code 1`, you may better follow [this step](https://freeman.vc/notes/installing-fasttext-on-an-m1-mac)


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
