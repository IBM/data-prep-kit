# Schema modification

This is a fairly simple transformer that supports tables' schema modification. It
supports the following transformations of the original data:
* Adding document id: this allows to add string document id to the data, calculated as
  `hashlib.sha256(doc.encode("utf-8")).hexdigest()`. These ids are stored in the column defined by `id_column`.
  To enable this set `id_column` to the name of the column, where you want to store it
* Adding integer document id: this allows to add an integer document id to the data. These ids are stored in
  the column defined by `int_id_column`. To enable this set `int_id_column` to the name of the column, where you want 
  to store it. This will create a globally unique integer ID across all rows in all files 
* Drop columns: this allows to drop some of the columns from the sources table. To enable this set a list of columns,
  see
  [here](https://www.codethebest.com/python/python-argparse-list-of-strings-implementation-steps/?utm_content=cmp-true)  
  for how to specify the list as a value to `columns_to_remove` to a list of column names, that you want to remove

Implementation of the unique ID is based on the singleton actor `IDGenerator`, which dishes unique id sets to transforms. 
When processing a given table, transform sends the actor number of rows, of the table it is processing and gets back
the starting integer ID for the table.

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build to build it
```

## Driver options

In addition to the "standard" options described
[here](../../../data-processing-lib/doc/launcher-options.md) transformer defines the following additional parameters:

```shell
    "doc_column": "contents",
    "id_column": "id_column",
    "int_id_column": "int_id_column",
    "columns_to_remove": <list of space separated columns_to_remove>
```

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/schema_local.py), [s3](src/schema_s3.py) and [lakehouse](src/schema_lakehouse.py)

# Release notes

If document ID generation is required, the doc column should be specified, otherwise the error will be returned
and the execution will not proceed.