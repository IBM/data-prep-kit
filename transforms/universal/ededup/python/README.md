# Ededup Python Transform 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

Also see [here](../ray/README.md) on details of implementation

## Summary 
This is a python version of ededup

## Configuration and command line Options

See [common](../README.md) ededup parameters

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --ededup_doc_column EDEDUP_DOC_COLUMN
                        name of the column containing document
  --ededup_doc_id_column EDEDUP_DOC_ID_COLUMN
                        name of the column containing document id
  --ededup_use_snapshot EDEDUP_USE_SNAPSHOT
                        flag to continue from snapshot
  --ededup_snapshot_directory EDEDUP_SNAPSHOT_DIRECTORY
                        location of snapshot files  
```
These correspond to the configuration keys described above.

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
