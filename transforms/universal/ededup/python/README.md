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
* _ededup_use_snapshot_ - specifies that ededup execution starts from a set of already seen hashes. This can be used
  for the incremental ededup execution
* _snapshot_directory_ - specifies a directory from which transforms are read. If this is not specified, a default

the `transform()` method of `EdedupPythonTransform` filters out duplicate documents. 

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --ededup_doc_column EDEDUP_DOC_COLUMN
                        key for accessing data
  --ededup_use_snapshot EDEDUP_USE_SNAPSHOT
                        flag to continue from snapshot
  --ededup_snapshot_directory EDEDUP_SNAPSHOT_DIRECTORY
                        location of snapshot files  
```
These correspond to the configuration keys described above.


To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
