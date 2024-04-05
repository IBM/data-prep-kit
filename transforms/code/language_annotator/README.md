# Select Language

This is a transform which can be used while preprocessing code data. It allows the
user to specify the programming languages for which the data should be retained. It adds a new
annotation column which can specify boolean True/False based on whether the rows belong to the
specified programming languages. The rows which belongs to the programming languages which are
not selected are annotated as False.

It requires a text file specifying the allowed languages. It is specified by the
commandline param `lang_select_allowed_langs_file`. A sample file is included at `data/allowed-code-languages.lst`.
The column specifying programming languages is to be specified by
commandline params `lang_select_language_column`.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* _lang_select_allowed_langs_file_ - specifies the location of the list of supported languages
* _lang_select_language_column_ - specifies the name of the column containing the language
* _lang_select_output_column_ - specifies the name of the annotation column appended to the parquet. 
* _lang_select_return_known_ - specifies whether to return supported or unsupported languages

## Running

We provide a demo of the transform usage for [local file system](src/lang_annotator_local.py)

# Release notes

