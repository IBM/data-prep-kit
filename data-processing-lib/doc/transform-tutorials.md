# Transform Tutorials

All transforms operate on a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
read for it by the RayWorker and produce zero or more transformed tables.
The transformed tables are then written out by the RayWorker - the transform need not
worry about I/O associated with the tables.
This means the Transform itself need only be concerned with the conversion of one
in memory table at a time.  

In support of this model the class 
[AbstractTableTransform](../src/data_processing/transform/table_transform.py) 
is expected to be extended when implementing a transform.
The following methods are defined:
* ```__init__(self, config:dict)``` - an initializer through which the transform can be created 
with implementation-specific configuration.  For example, the location of a model, maximum number of
rows in a table, column(s) to use, etc. 
* ```transform(self, table:pyarrow.Table) -> tuple(list[pyarrow.Table], dict)``` - this method is responsible
for the actual transformation of a given table to zero or more output tables, and optional 
metadata regarding the transformation applied.  Zero tables might be returned when
merging tables across calls to `transform()` and more than 1 table might be returned
when splitting tables by size or other criteria.
  * _output tables list_ - the RayWork handles the various number of returned tables as follows: 
    * 0 - no file will be written out and the input file name will not be used in the output directory.
    * 1 - one parquet file will be written to the output directory with 
    * N - N parquet files are written to the output with `_<index>` appended to the base file name
      * _dict_ - is a dictionary of transform-specific data keyed to numeric values.  The RayWorker will
         accumlute/add dictionaries across all calls to transform across all RayWorkers.  As an example, a
         transform might wish to track the number of instances of PII entities detected and might return 
        this as `{ "entities" : 1234 }`.
* ```flush() -> tuple(list[pyarrow.Table], dict)``` - this is provided for transforms that
make use of buffering (e.g. coalesce) across calls to `transform()` and need to be flushed
of all buffered data at the end of processing of input tables.  The return values are handled
in the same was as the return values for `transform()`.  Since most transforms will likely
not need this feature, a default implementation is provided to return an empty list and empty dictionary.

Some transforms will require use of the Ray environment, for example,
to create additional works, establish a share memory object, etc.  
To support such transforms, the 
[DefaultTableTransformRuntime](../src/data_processing/ray/transform_runtime.py)
class is provided and defines methods that allow passing additional
configuration data to the transform's initializer.  The key method for 
doing this is
* `set_enviornment(self,daf:DataAccessFactory, stats:ActorHandle, files:list) -> dict`
The returned dictionary is used as the dictionary to pass to the transform's initializer.

With these basic concepts in mind, we start with a simple example and 
progress to more complex transforms:
* [Simplest transform](simplest-transform-tutorial.md)
Here we will take a simple example to show the basics of creating a simple transform
that takes a single input Table, and produces a single Table.
* [Advanced transform](advanced-transform-tutorial.md)
* [Porting from GUF 0.1.6](transform-porting.md)

Once a transform has been built, testing can be enabled with the testing framework:
* [Transform Testing](testing-transforms.md) - shows how to test a transform
independent of the ray framework.
* [End-to-End Testing](testing-e2e-transform.md) - shows how to test the
transform running in the ray environment.