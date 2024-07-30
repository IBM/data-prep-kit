# Syntactic_construct_extractor
Please see the set of [data profiler project conventions](../../../README.md#tr-data-profiler-conventions)
for details on general project conventions, profiler configuration,
testing and IDE set up.

## Summary 
Generates overall stats about the code snippets in the the Datasets
* Packages in the code data
* Modules in the code data
The output profile of the dataset is generated and stored `tools/data-profiler/output/syntactic_construct_extractor.json`.

### Launcher's Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/syntactic_construct_extractor_profiler.py using command line args
* `run-local-sample` - runs src/syntactic_construct_extractor_local.py

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

The output profile of the dataset is generated and stored `tools/data-profiler/output/syntactic_construct_extractor.json`.
