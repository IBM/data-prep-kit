# SP Transform 
Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transform implements the higher order concept profiler of a given code dataset.
An user can specify the concepts which are of interest to the downstream usecase. These 
concepts can be a complex combination of syntactic and semantic criteria. Based on this, 
the input table containing the UBSRs, base syntactic, and semantic concepts is queried to generate 
the required results. The current implementation implements a single metric - code-to-comment ratio.
However, this is easily extensible. Examples of other higher order concepts are cyclomatic complexity 
of all python samples in the dataset and Line Coverage of all samples in a given semantic category.

## Configuration and command line Options

The set of dictionary keys holding [HOSPTransform](src/hosp_transform.py) 
configuration for values are as follows:

* _hosp_metrics_list_ - specifies the list of metrics that the user requires in the profiling report.
The list of metrics has to be predefined and their corresponding implementation logic have to be implemented 
apriori in the code.


## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
```
  --hosp_metrics_list HOSP_METRICS_LIST
                      List of metrics specified by the user for the profiling report.
```
These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/hosp_transform.py using command line args
* `run-local-sample` - runs src/hosp_local.py

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

