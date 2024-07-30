# Dataset statistics
Please see the set of [data profiler project conventions](../../../README.md#tr-data-profiler-conventions)
for details on general project conventions, profiler configuration,
testing and IDE set up.

## Summary 
The data profiler for dataset statistics generates overall stats about the datasets
* Instruction pairs
* Word counts
* Character counts
* Programming languages
* Human languages
* …

The output profile of the dataset is generated and stored `tools/data-profiler/output/dataset_stats.json`.

### Launcher's Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/dataset_stats_profiler.py using command line args
* `run-local-sample` - runs src/dataset_stats_local.py

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
To see results of the profiler.

The output profile of the dataset is generated and stored `tools/data-profiler/output/dataset_stats.json`.
