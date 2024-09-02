# Zip2Parquet 

## Summary 
This zip2parquet transform is designed to convert ZIP files containing a set of files
into arrow table. If these files contain code data (`code_data` flag) we additionally 
determine programming language (.py, .c, .java, etc).

Each file contained within the ZIP is transformed into a distinct row within the Parquet dataset, adhering to the below schema.

**title:** (string)

- **Description:** Path to the file within the ZIP archive.
- **Example:** `"title": "data/file.txt"`

**document:** (string)

- **Description:** Name of the ZIP file containing the current file.
- **Example:** `"document": "example.zip"`

**repo_name:**

- **Description:** The name of the repository to which the code belongs. This should match the name of the zip file containing the repository.
- **Example:** `"repo_name": "example"`

**contents:** (string)

- **Description:** Content of the file, converted to a string.
- **Example:** `"contents": "This is the content of the file."`

**document_id:** (string)

- **Description:** Unique identifier computed as a uuid. 
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

**domain:** (string)(optional)

- **Description:** Name indicating which domain it belong to, whether code, natural language etc..
- **Example:** `"domain": "code"`

**programming_language:** (string)(optional) - only if code_data is set to True

- **Description:** Programming language detected using the file extension.
- **Example:** `"programming_language": "Java"`


## Configuration 

The set of dictionary keys holding [zip2parquet](src/zip2parquet_transform.py) 
configuration for values are as follows:

The transform can be configured with the following key/value pairs
from the configuration dictionary.
* `code_data` - a flag defining whether to treat data as code or plain context. Default
  is code.
* `programming_language_column` - name of the column where programming language information
is stored - default `programming_language`. Only used if `code_data` is True
* `supported_languages` - a dictionary mapping file extensions to language names.
* `supported_langs_file` - used if `supported_languages` key is not provided,
  and specifies the path to a JSON file containing the mapping of languages
  to extensions. The json file is expected to contain a dictionary of
  languages names as keys, with values being a list of strings specifying the
  associated extensions. As an example, see 
  [lang_extensions](test-data/languages/lang_extensions.json) .
* `data_access_factory` - used to create the DataAccess instance used to read
the file specified in `supported_langs_file`.
* `detect_programming_lang` - a flag that indicates if the language:extension mappings
  should be applied in a new column value named `programming_language`.
* `domain` - optional value assigned to the imported data in the 'domain' column.
* `snapshot` -  optional value assigned to the imported data in the 'snapshot' column.

## Running

### Launched Command Line Options
The following command line arguments are available in addition to
the options provided by
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --zip2parquet_code_data ZIP2PARQUET_CODE_DATA
                        flag to process files as code
  --zip2parquet_programming_language_column ZIP2PARQUET_PROGRAMMING_LANGUAGE_COLUMN
                        Path to file containing the list of supported languages
  --zip2parquet_supported_langs_file ZIP2PARQUET_SUPPORTED_LANGS_FILE
                        Path to file containing the list of supported languages
  --zip2parquet_detect_programming_lang ZIP2PARQUET_DETECT_PROGRAMMING_LANG
                        Infer the programming lang from the file extension using the file of supported languages
  --zip2parquet_snapshot ZIP2PARQUET_SNAPSHOT
                        Snapshot value assigned to all imported documents.
  --zip2parquet_domain ZIP2PARQUET_DOMAIN
                        Domain value assigned to all imported documents.
  --zip2parquet_s3_cred ZIP2PARQUET_S3_CRED
                        AST string of options for s3 credentials. Only required for S3 data access.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: optional s3 url
                        region: optional s3 region
                        Example: { 'access_key': 'access', 'secret_key': 'secret', 
                        'url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud', 
                        'region': 'us-east-1' }
```

These correspond to the configuration keys described above.

### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/zip2parquet_transform_ray.py using command line args
* `run-local-sample` - runs src/zip2parquet.py
* `run-s3-sample` - runs src/zip2parquet.py
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


### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
