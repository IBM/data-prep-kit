# Header cleanser
Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This module is designed to detect and remove license and copyright information from code files. It leverages the [ScanCode Toolkit](https://pypi.org/project/scancode-toolkit/) to accurately identify and process licenses and copyrights in various programming languages.

After locating the position of license or copyright in the input code/sample, this module delete/remove those lines and returns the updated code as parquet file.

## Configuration and command line Options

The set of dictionary keys holding configuration for values are as follows:

* contents_column_name - used to define input column name. Default value is 'contents'.
* license - write 'true' to remove license from input data else 'false'. By default set as 'true'.
* copyright - write 'true' to remove copyright from input data else 'false'. by default set as 'true'.

## Running
You can run the [header_cleanser_local.py](src/header_cleanser_local.py) (python-only implementation) or [header_cleanser_local_ray.py](ray/src/header_cleanser_local_ray.py) (ray-based  implementation) to transform the `test1.parquet` file in [test input data](test-data/input) to an `output` directory.  The directory will contain both the new annotated `test1.parquet` file and the `metadata.json` file.

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).
* --header_cleanser_contents_column_name - set the contents_column_name configuration key.
* --header_cleanser_license - set the license configuration key.
* --header_cleanser_copyright - set the copyright configuration key. 

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/header_cleanser_transform_python.py using command line args
* `run-local-python-sample` - runs src/header_cleanser_local_python.py
* `run-local-sample` - runs src/header_cleanser_local.py

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

### Transforming local data 

Beginning with version 0.2.1, most/all python transform images are built with directories for mounting local data for processing.
Those directories are `/home/dpk/input` and `/home/dpk/output`.

After using `make image` to build the transform image, you can process the data 
in the `/home/me/input` directory and place it in the `/home/me/output` directory, for example,  using the 0.2.1 tagged image as follows:

```shell
docker run  --rm -v /home/me/input:/home/dpk/input -v /home/me/output:/home/dpk/output header_cleanser-python:0.2.1 	\
	python header_cleanser_transform_python.py --data_local_config "{ 'input_folder' : '/home/dpk/input', 'output_folder' : '/home/dpk/output'}"
```

You may also use the pre-built images on quay.io using `quay.io/dataprep1/data-prep-kit//header_cleanser-python:0.2.1` as the image name.