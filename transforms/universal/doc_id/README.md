# Doc ID Transform 

The Document ID transforms adds a document identification (unique integers and content hashes), which later can be 
used in de-duplication operations, per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [pythom](python/README.md) - enables the running of the base python transformation
  in a Python runtime
* [ray](ray/README.md) - enables the running of the base python transformation
  in a Ray runtime
* [spark](spark/README.md) - enables the running of a spark-based transformation
in a Spark runtime. 
* [kfp](kfp_ray/README.md) - enables running the ray docker image 
in a kubernetes cluster using a generated `yaml` file.

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

