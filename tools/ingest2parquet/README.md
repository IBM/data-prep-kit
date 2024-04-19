# INGEST2PARQUET  

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
- **Example:** `"snapshot": "github"`

**programming_language:** (optional)

- **Description:** Programming language detected using the file extension.
- **Example:** `"programming_language": "Java"`

**domain:** (optional)

- **Description:** Name indicating which domain it belong to, whether code, natural language etc..
- **Example:** `"domain": "code"`



## Configuration and command line Options

The set of dictionary keys holding [ingest2parquet](src/ingest2parquet.py) 
configuration for values are as follows:
```
  --detect_programming_lang DETECT_PROGRAMMING_LANG
                        generate programming language from the file extension
  --snapshot SNAPSHOT
                        Name the dataset
  --domain DOMAIN
                        To identify whether data is code or natural language
  --data_s3_cred DATA_S3_CRED
                        AST string of options for cos credentials. Only required for COS.
                        access_key: access key help text secret_key: secret key help text url: S3 url Example:
                        { 'access_key': 'AFDSASDFASDFDSF ', 'secret_key': 'XSDFYZZZ', 'url': 's3:/cos-optimal-
                        llm-pile/test/' }
  --data_s3_config DATA_S3_CONFIG
                        AST string containing input/output paths. input_path: Path to input folder of files to
                        be processed output_path: Path to output folder of processed files Example: {
                        'input_path': '/cos-optimal-llm-pile/bluepile-
                        processing/rel0_8/cc15_30_preproc_ededup', 'output_path': '/cos-optimal-llm-
                        pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed' }

  --data_local_config DATA_LOCAL_CONFIG
                        ast string containing input/output folders using local fs. input_folder: Path to input
                        folder of files to be processed output_folder: Path to output folder of processed
                        files Example: { 'input_folder': './input', 'output_folder': '/tmp/output' }
  --data_max_files DATA_MAX_FILES
                        Max amount of files to process
  --data_checkpointing DATA_CHECKPOINTING
                        checkpointing flag
  --data_data_sets DATA_DATA_SETS
                        List of data sets
  --data_files_to_use DATA_FILES_TO_USE
                        list of files extensions to choose
  --data_num_samples DATA_NUM_SAMPLES
                        number of random files to process
```
## Running

We provide several demos of the script usage for different data storage options: 


#[local file system](src/ingest2parquet_local.py)
This script processes data stored locally on the system. It sets up parameters for local file paths and invokes the run() function from ingest2parquet.py to convert raw data files to Parquet format.

**Run the script without any command-line arguments.**

```
make venv
source venv/bin/activate
cd src
python ingest2parquet_local.py
```

**Run the script via command-line** 

```
python ingest2parquet.py \
    --detect_programming_lang True \
    --snapshot github \
    --domain code \
    --data_local_config '{"input_folder": "../test-data/input", "output_folder": "../test-data/output"}' \
    --data_files_to_use '[".zip"]'
```



#[s3](src/s3.py) 
This script is designed to process data stored on an S3 bucket. It sets up necessary parameters for accessing the S3 bucket and invokes the run() function from ingest2parquet.py to convert raw data files to Parquet format.

To execute the script with S3 functionality, we utilize minio. Please consult the documentation on setting up minio for further guidance: [using_s3_transformers](../../data-processing-lib/doc/using_s3_transformers.md)

**Run the script without any command-line arguments.**

```
make venv
source venv/bin/activate
cd src
python s3.py
```

**Run the script via command-line** 

```
python ingest2parquet.py \
    --detect_programming_lang True \
    --snapshot github \
    --domain code \
    --data_s3_cred '{"access_key": "localminioaccesskey", "secret_key": "localminiosecretkey", "url": "http://localhost:9000"}' \
    --data_s3_config '{"input_folder": "test/ingest2parquet/input", "output_folder": "test/ingest2parquet/output"}' \
    --data_files_to_use '[".zip"]'
```

The output directory will contain both the new
genrated parquet files  and the `metadata.json` file.

## Metadata Fields

The metadata.json file contains following essential information regarding the processing of raw data files to Parquet format:

`total_files_given`: Total number of raw data files provided for processing.
`total_files_processed`: Number of files successfully processed and converted to Parquet format.
`total_files_failed_to_processed`: Count of files that encountered processing errors and failed conversion.
`total_no_of_rows`: Aggregate count of rows across all successfully processed files.
`total_bytes_in_memory`: Total memory usage in bytes for all processed data files.
`failure_details`: Information about files that failed processing, including their paths and error messages.

## Building the Docker Image
```
% make image 
```
## Run using docker image

```
docker run -it -v $(pwd)/test-data/input:/test-data/input -v $(pwd)/test-data/output:/test-data/output quay.io/dataprep1/data-prep-lab/ingest2parquet:0.1 sh -c "python ingest2parquet.py \
    --detect_programming_lang True \
    --snapshot github \
    --domain code \
    --data_local_config '{\"input_folder\": \"/test-data/input\", \"output_folder\":\"/test-data/output\"}' \
    --data_files_to_use '[\".zip\"]'"

```

In addition, there are some useful `make` targets (see conventions above):
* `make venv` - creates the virtual environment.
* `make test` - runs the tests in [test](test) directory
* `make build` - to build the docker image
* `make help` - displays the available `make` targets and help text.




