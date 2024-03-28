# RAW TO PARQUET CONVERSION SCRIPT 

## Summary 
This Python script is designed to convert raw data files, particularly ZIP files, into Parquet format. It is built to handle concurrent processing of multiple files using multiprocessing for efficient execution.
Each file contained within the ZIP is transformed into a distinct row within the Parquet dataset, adhering to the below schema.

**title:**

- **Description:** Path to the file within the ZIP archive.
- **Example:** `"title": "data/file.txt"`

**document:**

- **Description:** Name of the ZIP file containing the current file.
- **Example:** `"document": "example.zip"`

**contents:**

- **Description:** Content of the file, converted to a string.
- **Example:** `"contents": "This is the content of the file."`

**document_id:**

- **Description:** Unique identifier generated for each file.
- **Example:** `"document_id": "b1e4a879-41c5-4a6d-a4a8-0d7a53ec7e8f"`

**ext:**

- **Description:** File extension extracted from the file path.
- **Example:** `"ext": ".txt"`

**hash:**

- **Description:** sha256 hash value computed from the file content string.
- **Example:** `"hash": "a1b2c3d4"`

**size:**

- **Description:** Size of the file content in bytes.
- **Example:** `"size": 1024`

**date_acquired:**

- **Description:** Timestamp indicating when the file was processed.
- **Example:** `"date_acquired": "2024-03-25T12:00:00"`

**snapshot:** (optional)

- **Description:** Name indicating which dataset it belong to.
- **Example:** `"snapshot": "code"`

**lang:** (optional)

- **Description:** Programming language detected using the file extension.
- **Example:** `"lang": "Java"`



## Configuration and command line Options

The set of dictionary keys holding [RAW_DATA_TO_PARQUET](src/raw_data_to_parquet.py) 
configuration for values are as follows:

* detect_programming_lang (bool): if set to True,enables the detection of the programming language based on the file extension. 
* snapshot (str): Each row in the dataset will be labeled with this snapshot name.

## Running

We provide several demos of the script usage for different data storage options: 


#[local file system](src/local.py)
This script processes data stored locally on the system. It sets up parameters for local file paths and invokes the run() function from raw_data_to_parquet.py to convert raw data files to Parquet format.

Run the script without any command-line arguments.

```
make venv
source venv/bin/activate
cd src
python local.py
```



#[s3](src/s3.py) 
This script is designed to process data stored on an S3 bucket. It sets up necessary parameters for accessing the S3 bucket and invokes the run() function from raw_data_to_parquet.py to convert raw data files to Parquet format.

Before running, ensure to set up S3 credentials used in s3.py script as environment variables. 
Eg:
```
export DPF_S3_ACCESS_KEY="xxx"
export DPF_S3_SECRET_KEY="xxx"
```

Run the script without any command-line arguments.

```
make venv
source venv/bin/activate
cd src
python s3.py
```

The output directory will contain both the new
genrated parquet files  and the `metadata.json` file.

The metadata.json file contains following essential information regarding the processing of raw data files to Parquet format:

`total_files_given`: Total number of raw data files provided for processing.
`total_files_processed`: Number of files successfully processed and converted to Parquet format.
`total_files_failed_to_processed`: Count of files that encountered processing errors and failed conversion.
`total_no_of_rows`: Aggregate count of rows across all successfully processed files.
`total_bytes_in_memory`: Total memory usage in bytes for all processed data files.
`failure_details`: Information about files that failed processing, including their paths and error messages.



In addition, there are some useful `make` targets (see conventions above):
* `make venv` - creates the virtual environment.
* `make test` - runs the tests in [test](test) directory
* `make build` - to build the docker image
* `make help` - displays the available `make` targets and help text.





