# Base Syntactic Concept Extractor Transform 


## Configuration and command line Options

The set of dictionary keys holding [SyntacticConceptExtractorTransform](src/syntactic_concept_extractor_transform.py) 
configuration for values are as follows:

* content - specifies the column name in the dataframe that has the code snippet
* language - specifies the programming languages of the code snippet

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

### Running the samples
To run the samples, use the following `make` targets

* `run-local-sample` - runs src/syntactic_concept_extractor_local.py
* `run-local-python-sample` - runs src/syntactic_concept_extractor_local_python.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-local-sample
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
