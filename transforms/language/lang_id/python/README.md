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

* _lang_id_model_credential_ - specifies the credential you use to get model. This will be huggingface token. [Guide to get huggingface token](https://huggingface.co/docs/hub/security-tokens)
* _lang_id_model_kind_ - specifies what kind of model you want to use for language identification. Currently, only `fasttext` is available.
* _lang_id_model_url_ - specifies url that model locates. For fasttext, this will be repo name of the model, like `facebook/fasttext-language-identification`
* _lang_id_content_column_name_ - specifies name of the column containing documents

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


### Transforming local data 

Beginning with version 0.2.1, most/all python transform images are built with directories for mounting local data for processing.
Those directories are `/home/dpk/input` and `/home/dpk/output`.

After using `make image` to build the transform image, you can process the data 
in the `/home/me/input` directory and place it in the `/home/me/output` directory, for example,  using the 0.2.1 tagged image as follows:

```shell
docker run  --rm -v /home/me/input:/home/dpk/input -v /home/me/output:/home/dpk/output lang_id-python:0.2.1 	\
	python lang_id_transform_python.py --data_local_config "{ 'input_folder' : '/home/dpk/input', 'output_folder' : '/home/dpk/output'}"
```

You may also use the pre-built images on quay.io using `quay.io/dataprep1/data-prep-kit//lang_id-python:0.2.1` as the image name.

