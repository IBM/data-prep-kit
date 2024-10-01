# SP Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform implements semantic profiling of a code dataset. Given an input dataset 
as a pyarrow table with the UBSRs of code data points, this transform extracts the libraries
and obtains their semantic mapping by consulting the IKB. The semantic concepts obatined per data
point are then added as a new column in the input dataset. Those libraries which are not present in the
IKB are recorded in a separate 'null_libs' file for offline processing. This file is passed as an input
to the [offline path](src/offline_path/) which reads the libraries and obtains their semantic categories 
from predefined set by prompting an LLM. The examples passed into the prompt are present in the [examples folder](src/examples/)

## Configuration and command line Options

The set of dictionary keys holding [SPTransform](src/sp_transform.py) 
configuration for values are as follows:

* _sp_ikb_file_ - This is the path to the IKB file which is a CSV file and by default located in the [IKB](src/ikb/) folder.
                  It contains three columns - Library, Language, Category. The set of categories is defined in the 
                  [concept map file](src/concept_map/).
* _sp_null_libs_file_ - This is the path to the null_libs file which is also a CSV file containing two columns - 
                        Library, Language. Its default value is src/ikb/null_libs.csv.

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --sp_ikb_file        SP_IKB_FILE
                       Path to the IKB file
  --sp_null_libs_file  SP_NULL_LIBS_FILE   
                       Path to the file to store the libraries for which no match could be found in the IKB
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/sp_transform.py using command line args
* `run-local-sample` - runs src/sp_local.py

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

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.

