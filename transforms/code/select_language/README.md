# Select Language

This is a transform which can be used while preprocessing code data. It allows the
user to specify the programming languages for which the data should be retained.
The data which belongs to the programming languages which are not selected is dropped.

It requires a text file specifying the allowed languages. It is specified by the
commandline param `ls_allowed_langs_file`. A sample file is included at `data/allowed-code-languages.lst`.
The column specifying programming languages is to be specified by
commandline params `ls_language_column`.

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _ls_allowed_langs_file_ - specifies the location of the list of supported languages
* _ls_language_column_ - specifies the name of the column containing the language
* _ls_return_known_ - specifies whether to return supported or unsupported languages

## Running

We provide a demo of the transform usage for [local file system](src/langselect_local.py)

# Release notes

