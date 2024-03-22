# Document ID Annotator

Please see the set of
[transform project conventions](../../README.md)
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

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/doc_id_local_ray.py), [s3](src/doc_id_s3.py) 

```shell
$ source venv/bin/activate
(venv) $ export PYTHONPATH=src
(venv) $ python src/doc_id_local.py
input table: pyarrow.Table
Unnamed: 0.1: int64
Unnamed: 0: int64
document_id: string
document: string
title: string
contents: string
...
output metadata : {}
hashed column : [
  [
    "841...99b6",
    "ebf...1777",
    "79c...fa1e",
    "79c...fa1e",
    "ebf...1777"
  ]
]
index column : [
  [
    0,
    1,
    2,
    3,
    4
  ]
]
(venv) $
```

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
```
  --doc_id_doc_column DOC_ID_DOC_COLUMN
                        doc column name
  --doc_id_hash_column DOC_ID_HASH_COLUMN
                        Compute document hash and place in the given named column
  --doc_id_int_column DOC_ID_INT_COLUMN
                        Compute unique integer id and place in the given named column
```
These correspond to the configuration keys described above.