# Transform Tutorials

All transforms operate on a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
and produce zero or more transformed tables and transform specific metadata.
The Transform itself need only be concerned with the conversion of one in memory table at a time.  

When running in the Ray worker (i.e. [TransformTableProcessor](../src/data_processing/runtime/ray/transform_table_processor.py) ), the input
tables are read from parquet files and the transform table(s) is/are stored in destination parquet files.
Metadata accumulated across calls to all transforms is stored in the destination.

### Transform Basics
In support of this model the there are two primary classes
defined and discussed below

#### AbstractTableTransform class
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
The [TransformConfiguration](../src/data_processing/transform/transform_configuration.py)
serves as an interface and must be implemented by the any `AbstractTableTransform`
implementation to provide the following configuration:
* the transform class to be used,
* command line arguments used to initialize the Transform Runtime and generally, the Transform.
* Transform Runtime class to use
* transform short name 
It is expected that transforms are initialized with a fixed name, the class of its corresponding
`AbstractTableTransform` implementation and optionally the configuration keys that should not
be exposed as metadata for a run.
To support command line configuration, the `TransformConfiguration` extends the
[CLIArgumentProvider](../src/data_processing/utils/cli_utils.py) class.
The set of methods of interest are
* ```__init__(self, name:str, transform_class:type[AbstractTableTransform], list[str]:remove_from_metadata )``` - sets the required fields
* ```add_input_params(self, parser:ArgumentParser)``` - adds transform-specific command line options that will
be made available in the dictionary provided to the transform's initializer.
* ```apply_input_params(self, args: argparse.Namespace)``` - verifies  and captures the relevant transform parameters.
* ```get_input_params(self ) -> dict[str,Anny]``` - returns the dictionary of configuration values that
should be used to initialize the transform.

### Runtimes
Runtimes provide a mechanism to run a transform over a set of input files to produce a set of
output files.  Each runtime is started using a _launcher_.  
The available runtimes are discussed [here](transform-runtimes.md].
