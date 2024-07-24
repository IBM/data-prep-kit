# Document Quality Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform will calculate and annotate several metrics related to document, which are usuful to see the quality of document. 

In this transform, following metrics will be included:
- Gopher (Deepmind): With this metric, you can filter docs that
  - do not contain between 50 and 100,000 words
  - mean word length is outside the range of 3 to 10 characters;
  - symbol-to-word ratio > 0.1 for either the hash symbol or the ellipsis;
  - > 90% of lines starting with a bullet point,
  - > 30% ending with an ellipsis.
  - Require that 80% of words in a document contain at least one alphabetic character, and apply a "stop word" filter, to remove documents that do NOT contain at least TWO of the following English words: the, be, to, of, and, that, have, with; this adequately deals with ostensibly English documents that contain no coherent English text.



## Configuration and command line Options

The set of dictionary keys holding [DocQualityTransform](src/doc_quality_transform.py) 
configuration for values are as follows:

* _text_lang_ - specifies language used in the text content. By default, "en" is used.
* _doc_content_column_ - specifies column name that contains document text.
* _bad_word_filepath_ - specifies a path to bad word file: local folder (file or directory) that points to bad word file.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --docq_text_lang DOCQ_TEXT_LANG   language used in the text content. By default, "en" is used
  --docq_doc_content_column DOCQ_DOC_CONTENT_COLUMN   column name that contain document text
  --docq_bad_word_filepath DOCQ_BAD_WORD_FILEPATH   path to bad word file: local folder (file or directory) that points to bad word file
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


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.


## Troubleshooting guide

For M1 Mac user, if you see following error during make command, `error: command '/usr/bin/clang' failed with exit code 1`, you may better follow [this step](https://freeman.vc/notes/installing-fasttext-on-an-m1-mac)