# Code2Parquet 

## Summary 
This code2parquet transform is designed to convert raw particularly ZIP files contain programming files (.py, .c, .java, etc) , 
into Parquet format. 
As a transform It is built to handle concurrent processing of Ray-based
multiple files using multiprocessing for efficient execution.
Each file contained within the ZIP is transformed into a distinct row within the Parquet dataset, adhering to the below schema.

**title:** (string)

- **Description:** Path to the file within the ZIP archive.
- **Example:** `"title": "data/file.txt"`

**document:** (string)

- **Description:** Name of the ZIP file containing the current file.
- **Example:** `"document": "example.zip"`

**contents:** (string)

- **Description:** Content of the file, converted to a string.
- **Example:** `"contents": "This is the content of the file."`

**document_id:** (string)

- **Description:** Unique identifier generated for each file.
- **Example:** `"document_id": "b1e4a879-41c5-4a6d-a4a8-0d7a53ec7e8f"`

**ext:** (string)

- **Description:** File extension extracted from the file path.
- **Example:** `"ext": ".txt"`

**hash:** (string)

- **Description:** sha256 hash value computed from the file content string.
- **Example:** `"hash": "a1b2c3d4"`

**size:** (int64)

- **Description:** Size of the file content in bytes.
- **Example:** `"size": 1024`

**date_acquired:** (string)

- **Description:** Timestamp indicating when the file was processed.
- **Example:** `"date_acquired": "2024-03-25T12:00:00"`

**snapshot:** (string)(optional)

- **Description:** Name indicating which dataset it belong to.
- **Example:** `"snapshot": "github"`

**programming_language:** (string)(optional)

- **Description:** Programming language detected using the file extension.
- **Example:** `"programming_language": "Java"`

**domain:** (string)(optional)

- **Description:** Name indicating which domain it belong to, whether code, natural language etc..
- **Example:** `"domain": "code"`



## Configuration and command line Options

The set of dictionary keys holding [code2parquet](src/code2parquet.py) 
configuration for values are as follows:
```
  supported_langs_file CODE2PARQUET_SUPPORTED_LANGS_FILE
                        Path to file containing the list of supported languages
  detect_programming_lang CODE2PARQUET_DETECT_PROGRAMMING_LANG
                        generate programming lang
  s3_cred CODE2PARQUET_S3_CRED
                        AST string of options for s3 credentials. Only required for S3 data access.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: optional s3 url
                        region: optional s3 region
                        Example: { 'access_key': 'access', 'secret_key': 'secret', 
                        'url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud', 
                        'region': 'us-east-1' }
```

## Running

### Launched Command Line Options
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to
[the options provided by the launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).

```shell
  --code2parquet_supported_langs_file CODE2PARQUET_SUPPORTED_LANGS_FILE
                        Path to file containing the list of supported languages
  --code2parquet_detect_programming_lang CODE2PARQUET_DETECT_PROGRAMMING_LANG
                        generate programming lang
 ```

These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/code2parquet_transform_ray.py using command line args
* `run-local-sample` - runs src/code2parquet.py
* `run-s3-sample` - runs src/code2parquet.py
    * Requires prior installation of minio, depending on your platform (e.g., from [here](https://min.io/docs/minio/macos/index.html)
     and [here](https://min.io/docs/minio/linux/index.html) 
     and invocation of `make minio-start` to load data into local minio for S3 access.

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
---------------------------------


