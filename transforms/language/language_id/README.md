# Language Identification

Please see the set of
[transform project conventions](../../transform-conventions.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

Identify language of each text with confidence score with fasttext language identification model. [ref](https://huggingface.co/facebook/fasttext-language-identification)


## Transform runtime

[Transform runtime](src/lang_id_transform.py) is responsible for language identifcation actors and sending their 
handles to the transforms themselves
Additionally it enhances statistics information with the information about number of records per detected language.

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build
```

to build it

## Configuration and command line Options

The set of dictionary keys holding [LangIdentificationTransform](src/lang_id_transform.py)
configuration for values are as follows:

* lang_id_model_credential - specifies the credential you use to get model. This will be huggingface token.
* lang_id_model_kind - specifies what kind of model you want to use for language identification. Currently, only `fasttext` is available
* lang_id_model_url - specifies url that model locates. For fasttext, this will be repo name of the model, like `facebook/fasttext-language-identification`
* lang_id_content_column_name - specifies name of the column containing documents

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/lang_id_local.py), [s3](src/lang_id_s3.py) and [lakehouse](src/lang_id_lakehouse.py)

# Release notes