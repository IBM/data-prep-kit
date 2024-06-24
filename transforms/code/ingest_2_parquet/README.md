# Ingest to Parquet

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary
This module is designed to convert raw data files, particularly ZIP files, into Parquet format.Each file contained within the ZIP is transformed into a distinct row within the Parquet dataset, adhering to the below schema.

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

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
the options provided by the [ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md)
and the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --ingest_detect_programming_lang_key 
                        generate programming language from the file extension
  --ingest_snapshot_key 
                        Name the dataset
  --ingest_domain_key 
                        To identify whether data is code or natural language
  --ingest_supported_langs_file_key
                        Specify the path to the JSON file that maps programming languages to their respective file extensions. In this file, each key represents a programming language, and the corresponding value is a list of its extensions. You can find the file at ray/test-data/languages/lang_extensions.json for reference.

```

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-ray-sample` - runs src/ingest_2_parquet_transform_ray.py using command line args
* `run-local-sample` - runs src/ingest_2_parquet_local.py
* `run-local-ray-sample` - runs src/ingest_2_parquet_local_ray.py
* `run-s3-ray-sample`- runs src/ingest_2_parquet_s3_ray.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-ray-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.

