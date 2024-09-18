# Data Access Factory 


## Introduction
[Data Access Factory(DAF)](../python/src/data_processing/data_access/data_access_factory.py) provides a mechanism to create 
[DataAccess](../python/src/data_processing/data_access/data_access.py) 
implementations that support
the processing of input data files and the expected destination
of the processed files.
The `DataAccessFactory` is most often configured using command line arguments
to specify the type of `DataAccess` instance to create
(see `--data_*` options [here](python-launcher-options.md).
Currently,  it supports
[DataAccessLocal](../python/src/data_processing/data_access/data_access_local.py)
and 
[DataAccessS3](../python/src/data_processing/data_access/data_access_s3.py)
implementations.

You can use DAF and the resulting DataAccess implementation in your transform logic to
read and write extra file(s), for example, write log or metadata files.

This document explains how to initialize and use DAF to write a file using a `DataAccess` instance. 

## Data Access 
Each Data Access implementation supports the notion of processing a
set of input files to produce a set of output files, generally in a 1:1 mapping, 
although this is not strictly required.
With this in mind, the following function is provided: 
* Input file identification by 
    * input folder 
    * sub-directory selection (aka data sets))
    * file extension
    * files extensions to checkpoint
    * maximum count
    * random sampling
* Output file identification (for a given input)
* Checkpointing  - determines the set of input files that need processing 
(i.e. which do not have corresponding output files). In the case of parquet files, where
inputs and outputs are parquet this comparison is fairly simple. In the case of binary
files it is a little bit more involved as input and output files may have different extensions.
in this case you need to specify both `files extensions` and `files extensions to checkpoint`  
* Reading and writing of files.

Each transform runtime uses a DataAccessFactory to create a DataAccess instance which
is then used to identify and process the target input data.
Transforms may use this the runtime instance or can use their own DataAccessFactory.
This might be needed if reading or writing other files to/from other locations.

## Creating DAF instance

```python
from data_processing.data_access import DataAccessFactory
daf = DataAccessFactory("myprefix_", False)
```
The first parameter `cli_arg_prefix` is prefix used to look for parameter names 
starting with prefix `myprefix_`. Generally the prefix used is specific to the
transform.

## Preparing and setting parameters 
```python
from argparse import Namespace

s3_cred = {
    "access_key": "XXXX",
    "secret_key": "XXX",
    "url": "https://s3.XXX",
}

s3_conf={
    'input_folder': '<COS Location of input>', 
    'output_folder': 'cos-optimal-llm-pile/somekey'
}

args = Namespace(
    myprefix_s3_cred=s3_cred,
    myprefix_s3_config=s3_conf,
)
assert daf.apply_input_params(args)

```
`apply_input_params` will extract and use parameters from `args` with 
prefix `myprefix_`(which is `myprefix_s3_cred` and `myprefix_s3_config` in this example).

The above is equivalent to passing the following on the command line to a runtime launcher
```shell
... --myprefix_s3_cred '{ "access_key": "XXXX", "secret_key": "XXX", "url": "https:/s3.XXX" }'\
    --myprefix_s3_config '{ "input_folder": "<COS Location of input>", "cos-optimal-llm-pile/somekey" }'
```

## Create DataAccess and write file 
```python
data_access = daf.create_data_access()
data_access.save_file(f"data/report.log", "success")
```

Call to `create_data_access` will create the `DataAccess` instance (`DataAccessS3` in this case) .
`save_file` will write a new file at `data/report.log` with content `success`.

When writing a transform, the `DataAccessFactory` is generally created in the
transform's configuration class and passed to the transform's initializer by the runtime. 
See [this section](transform-external-resources.md) on accessing external resources for details.