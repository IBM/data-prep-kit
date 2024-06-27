# Document ID Annotator

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This transform annotates documents with document "ids".
It supports the following transformations of the original data:
* Adding document hash: this enables the addition of a document hash-based id to the data.
The hash is calculated with `hashlib.sha256(doc.encode("utf-8")).hexdigest()`. 
To enable this annotation, set `hash_column` to the name of the column, 
where you want to store it.
* Adding integer document id: this allows the addition of an integer document id to the data that 
is unique across all rows in all tables provided to the `transform()` method. 
To enable this annotation, set `int_id_column` to the name of the column, where you want 
to store it. 

Document IDs are generally useful for tracking annotations to specific documents. Additionally 
[fuzzy deduping](../fdedup) relies on integer IDs to be present. If your dataset does not have
document ID column(s), you can use this transform to create ones.

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build 
```

## Driver options

## Configuration and command line Options

The set of dictionary keys defined in [DocIDTransform](src/doc_id_transform.py)
configuration for values are as follows:

* _doc_column_ - specifies name of the column containing the document (required for ID generation)
* _hash_column_ - specifies name of the column created to hold the string document id, if None, id is not generated
* _int_id_column_ - specifies name of the column created to hold the integer document id, if None, id is not generated

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
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/doc_id_transform_ray.py using command line args
* `run-local-sample` - runs src/doc_id_local.py
* `run-local-python-only-sample` - runs src/doc_id_local_ray.py
* `run-s3-sample` - runs src/doc_id_s3_ray.py
    * Requires prior installation of minio, depending on your platform (e.g., from [here](https://min.io/docs/minio/macos/index.html)
     and [here](https://min.io/docs/minio/linux/index.html) 
     and invocation of `make minio-start` to load data into local minio for S3 access.

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
