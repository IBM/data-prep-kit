# Document Quality Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform will calculate several metrics related to document, which are usuful to see the quality of document. 

In this transform, following metrics will be included:
- Gopher (Deepmind): filter docs that
  - do not contain between 50 and 100,000 words
  - mean word length is outside the range of 3 to 10 characters;
  - symbol-to-word ratio > 0.1 for either the hash symbol or the ellipsis;
  - > 90% of lines starting with a bullet point,
  - > 30% ending with an ellipsis.
  - Require that 80% of words in a document contain at least one alphabetic character, and apply a "stop word" filter, to remove documents that do NOT contain at least TWO of the following English words: the, be, to, of, and, that, have, with; this adequately deals with ostensibly English documents that contain no coherent English text.
- Perplexity score (KenLM+sentencepiece) suggested in Gopher The smaller the perplexity score, the closer is the text to the targeted domain (i.e., en Wikipedia). Journalistic and well written content. Distribution of perplexity for different languages may have different shapes.



## Configuration and command line Options

The set of dictionary keys holding [DocQualityTransform](src/doc_quality_transform.py) 
configuration for values are as follows:

* _text_lang_ - specifies language used in the text content. By defaut, "en" is used.
* _doc_content_column_ - specifies column name that contains document text.
* _doc_id_column_ - specifies column name that contains document id.
* _bad_word_filepath_ - specifies a path to bad word file: local folder (file or directory) that points to bad word file.
* _model_path_ - specifies a path to model: local folder (file or directory) that points to model. If it exists in local file system, model will be loaded from there. If it does not exist, the value specified here will be ignored and try to find model in s3 using _s3_cred_.
* _s3_cred_ - AST string of options for cos credentials retrieve kenLM model from s3.
* _model_module_name_ - specifies a class name that uses model. The class should extend perplexity_models.PerplexityModel.
* _perplex_score_digit_ - specifies a digit of perplexity score.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --docq_text_lang DOCQ_TEXT_LANG   language used in the text content. By defaut, "en" is used
  --docq_doc_content_column DOCQ_DOC_CONTENT_COLUMN   column name that contain document text
  --docq_doc_id_colum DOCQ_DOC_ID_COLUMN   column name that contains document id
  --docq_bad_word_filepath DOCQ_BAD_WORD_FILEPATH   path to bad word file: local folder (file or directory) that points to bad word file
  --docq_model_path   path to model: path (local or s3) to model
  --docq_model_module_name   module name that has subclass of PerplexityModel to use
  --docq_perplex_score_digit   digit of perplexity score
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/doc_quality_transform.py using command line args
* `run-local-sample` - runs src/doc_quality_local.py

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


### Transforming local data 

Beginning with version 0.2.1, most/all python transform images are built with directories for mounting local data for processing.
Those directories are `/home/dpk/input` and `/home/dpk/output`.

After using `make image` to build the transform image, you can process the data 
in the `/home/me/input` directory and place it in the `/home/me/output` directory, for example,  using the 0.2.1 tagged image as follows:

```shell
docker run  --rm -v /home/me/input:/home/dpk/input -v /home/me/output:/home/dpk/output doc_quality-python:0.2.1 	\
	python doc_quality_transform_python.py --data_local_config "{ 'input_folder' : '/home/dpk/input', 'output_folder' : '/home/dpk/output'}" --docq_bad_word_filepath /path/to/badword --docq_model_path /path/to/model	--docq_model_module_name perplexity_transformer_model
```

You may also use the pre-built images on quay.io using `quay.io/dataprep1/data-prep-kit//noop-python:0.2.1` as the image name.


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.


## Troubleshooting guide

For M1 Mac user, if you see following error during make command, `error: command '/usr/bin/clang' failed with exit code 1`, you may better follow [this step](https://freeman.vc/notes/installing-fasttext-on-an-m1-mac)