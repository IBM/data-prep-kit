# Document ID Python Annotator

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build 
```

## Configuration and command line Options

The set of dictionary keys defined in [DocIDTransform](src/doc_id_transform_ray.py)
configuration for values are as follows:

* _doc_column_ - specifies name of the column containing the document (required for ID generation)
* _hash_column_ - specifies name of the column created to hold the string document id, if None, id is not generated
* _int_id_column_ - specifies name of the column created to hold the integer document id, if None, id is not generated
* _start_id_ - an id from which ID generator starts () 

At least one of _hash_column_ or _int_id_column_ must be specified.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
[the options provided by the ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).
```
  --doc_id_doc_column DOC_ID_DOC_COLUMN
                        doc column name
  --doc_id_hash_column DOC_ID_HASH_COLUMN
                        Compute document hash and place in the given named column
  --doc_id_int_column DOC_ID_INT_COLUMN
                        Compute unique integer id and place in the given named column
  --doc_id_start_id DOC_ID_START_ID
                        starting integer id
```
These correspond to the configuration keys described above.


To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
