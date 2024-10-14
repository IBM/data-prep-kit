# Transforms 

Transform is a basic integration unit of DPK that can be executed in any of the supported by the DPK 
runtimes ([Python](python-runtime.md), [Ray](ray-runtime.md) and [Spark](spark-runtime.md)). All transforms 
are derived from the 
[AbstractTransform class](../python/src/data_processing/transform/abstract_transform.py). Theis class
provides no functionality and is used as just a marker that a given class implements transform.
There are currently two types of transforms defined in DPK:

* [AbstractBinaryTransform](../python/src/data_processing/transform/binary_transform.py) which is a base 
class for all data transforms. Data transforms convert a file of data producing zero or more data files 
and metadata. A specific class of the binary transform is 
[AbstractTableTransform](../python/src/data_processing/transform/table_transform.py) that consumes and produces
data files containing [pyarrow tables](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
* [AbstractFolderTransform](../python/src/data_processing/transform/folder_transform.py) which is a base
class consuming a folder (that can contain an arbitrary set of files, that need to be processed together)
and proces zero or more data files and metadata.


In the discussion that follows, we'll focus on the transformation of pyarrow Tables
using the `AbstractTableTransform` class (see below), supported by Ray Spark and Python runtimes.

#### AbstractTableTransform class
[AbstractTableTransform](../python/src/data_processing/transform/table_transform.py) 
is expected to be extended when implementing a transform of pyarrow Tables.
In general, when possible a transform should be independent of the runtime in
which it runs, and the mechanism used to define its configuration
(e.g., the `TransformConfiguration` class below, or other mechanism).
That said, some transforms may require facilities provided by the runtime
(shared memory, distribution, etc.), but as a starting point, think of the
transform as an independent operator.

The following methods are defined:

* ```__init__(self, config:dict)``` - an initializer through which the transform can be created 
with implementation-specific configuration.  For example, the location of a model, maximum number of
rows in a table, column(s) to use, etc.   Error checking of configuration should be done here.
* ```transform(self, table:pyarrow.Table) -> tuple(list[pyarrow.Table], dict)``` - this method is responsible
for the actual transformation of a given table to zero or more output tables, and optional 
metadata regarding the transformation applied.  Zero tables might be returned when
merging tables across calls to `transform()` and more than 1 table might be returned
when splitting tables by size or other criteria.
  * _output tables list_ - the RayWork handles the various number of returned tables as follows: 
    * 0 - no file will be written out and the input file name will not be used in the output directory.
    * 1 - one parquet file will be written to the output directory with 
    * N - N parquet files are written to the output with `_<index>` appended to the base file name
  * _dict_ - is a dictionary of transform-specific data keyed to numeric values.  A statistics component will
         accumulate/add dictionaries across all calls to transform across all calls to all transforms running
         in a given _runtime_ (see below). As an example, a
         transform might wish to track the number of instances of PII entities detected and might return 
         this as `{ "entities" : 1234 }`.
* ```flush() -> tuple(list[pyarrow.Table], dict)``` - this is provided for transforms that
make use of buffering (e.g. to resize the tables) across calls 
to `transform()` and need to be flushed of all buffered data at the end of processing of input tables.  
The return values are handled the same waa as the return values for `transform()`.  Since most transforms will likely
not need this feature, a default implementation is provided to return an empty list and empty dictionary.
 
#### TransformConfiguration class
The [TransformConfiguration](../python/src/data_processing/transform/transform_configuration.py)
serves as an interface and must be implemented by the any `AbstractTableTransform`
implementation to enable running within and runtime or from a command line to capture 
transform configuration.  It provides the following configuration:

* the transform class to be used,
* command line arguments used to initialize the Transform Runtime and generally, the Transform.
* Transform Runtime class to use
* transform short name 

It is expected that transforms are initialized with a fixed name, the class of its corresponding
`AbstractTableTransform` implementation and optionally the configuration keys that should not
be exposed as metadata for a run.
To support command line configuration, the `TransformConfiguration` extends the
[CLIArgumentProvider](../python/src/data_processing/utils/cli_utils.py) class.
The set of methods of interest are

* ```__init__(self, name:str, transform_class:type[AbstractTableTransform], list[str]:remove_from_metadata )``` - sets the required fields
* ```add_input_params(self, parser:ArgumentParser)``` - adds transform-specific command line options that will
be made available in the dictionary provided to the transform's initializer.
* ```apply_input_params(self, args: argparse.Namespace)``` - verifies  and captures the relevant transform parameters.
* ```get_input_params(self ) -> dict[str,Anny]``` - returns the dictionary of configuration values that
should be used to initialize the transform.


