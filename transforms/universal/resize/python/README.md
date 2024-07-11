# Split  files

Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This is a simple transformer that is resizing the input tables to a specified size. 
* resizing based on in-memory size of the tables.
* resized based on the number of rows in the tables. 

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build 
```

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py)
configuration for values are as follows:

* _max_rows_per_table_ - specifies max documents per table
* _max_mbytes_per_table - specifies max size of table, according to the _size_type_ value.
* _size_type_ - indicates how table size is measured. Can be one of
    * memory - table size is measure by the in-process memory used by the table
    * disk - table size is estimated as the on-disk size of the parquet files.  This is an estimate only
        as files are generally compressed on disk and so may not be exact due varying compression ratios.
        This is the default.

Only one of the _max_rows_per_table_ and _max_mbytes_per_table_ may be used.

## Running

We also provide several demos of the transform usage for different data storage options, including
[local file system](src/resize_local_ray.py), [s3](src/resize_s3_ray.py) and [lakehouse](src/resize_lakehouse.py)


### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md) and map to the configuration keys above.

```
  --resize_max_rows_per_table RESIZE_MAX_ROWS_PER_TABLE
                        Max number of rows per table
  --resize_max_mbytes_per_table RESIZE_MAX_MBYTES_PER_TABLE
                        Max table size (MB). Size is measured according to the --resize_size_type parameter
  --resize_size_type {disk,memory}
                        Determines how memory is measured when using the --resize_max_mbytes_per_table option.
                        'memory' measures the in-process memory footprint and 
                        'disk' makes an estimate of the resulting parquet file size.
```

### Transforming local data 

Beginning with version 0.2.1, most/all python transform images are built with directories for mounting local data for processing.
Those directories are `/home/dpk/input` and `/home/dpk/output`.

After using `make image` to build the transform image, you can process the data 
in the `/home/me/input` directory and place it in the `/home/me/output` directory, for example,  using the 0.2.1 tagged image as follows:

```shell
docker run  --rm -v /home/me/input:/home/dpk/input -v /home/me/output:/home/dpk/output resize-python:0.2.1 	\
	python resize_transform_python.py --data_local_config "{ 'input_folder' : '/home/dpk/input', 'output_folder' : '/home/dpk/output'}"
```

You may also use the pre-built images on quay.io using `quay.io/dataprep1/data-prep-kit//resize-python:0.2.1` as the image name.


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
