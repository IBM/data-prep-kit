# Data Access Factory 


## Introduction
[Data Access Factory(DAF)](../python/src/data_processing/data_access/data_access_factory.py) provides a abstraction and implmentation to read and write independent of file system.

Currently it supports Local and S3 file system.

You can use DAF in your transform logic to read and write extra file(s), for example, write log or meta data files.

This document explains how to intialize and use DAF to write a file to COS


## Creating DAF instance
```
from data_processing.data_access import DataAccessFactory
daf = DataAccessFactory("data_", False)
```

The first parameter `cli_arg_prefix` is prefix used to look for parameter names starting with prefix `data_`.

## Preparing and setting parameters 
```
from argparse import Namespace

s3_cred = {
    "access_key": "XXXX",
    "secret_key": "XXX",
    "url": "https://s3.XXX",
}

s3_conf={
    'input_folder': '<COS Location of input>', 
    'output_folder': 'cos-optimal-llm-pile/bluepile-processing/ssb/doc_id_ededup_fdedup_langid_filtered_tokenized/threshold=0_9/'
}

args = Namespace(
    data_s3_cred=s3_cred,
    data_s3_config=s3_conf,
)
assert daf.apply_input_params(args)

```
`apply_input_params` will extract and use parameters from `args` with prefix `data_`( which is `data_s3_cred` and `data_s3_config` in this example).

## Create instance and write file to S3 bucket
```
data_access: DataAccessFactory = daf.create_data_access()
assert isinstance(data_access,data_processing.data_access.data_access_s3.DataAccessS3)
data_access.save_file(f"cos_bucket/data/report.log", "success")
```

Call to `create_data_access` will actually establish connection with S3. `save_file` will write a new file at `cos_bucket/data/report.log` with content `success`.
DAF instance can be initialized in your transformer's `__init__` method.