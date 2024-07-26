# Ededup Python Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This is a python version of ededup

## Configuration and command line Options

The set of dictionary keys holding [EdedupPythonTransform](src/ededup_transform_python.py) 
configuration for values are as follows:

* _ededup_doc_column_ - specifies the name of the column containing a document

the `transform()` method of `EdedupPythonTransform` filters out duplicate documents. 

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
 --ededup_doc_column EDEDUP_DOC_COLUMN
                        name of the column containing document 
```
These correspond to the configuration keys described above.


To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
