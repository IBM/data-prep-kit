# URL Block List Annotator 
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The block listing annotator/transform maps an input table to an output table
by  using a list of domains that are intended to be blocked 
(i.e. ultimately removed from the tables).
The input table contains a column, by default named `title`,
that holds the source url for the content in a given row.
The output table is annotated to include a new column,
named `blocklisted` by default, that contains the name
of the blocked domain.  If the value of the source url 
does not match any of the blocked domains, it will be empty.

## Configuration and command line Options

The set of dictionary keys holding [BlockListTransform](src/blocklist_transform.py) 
configuration for values are as follows:

* _bl_annotation_column_name_ - specifies the name of the table column into which the annotation is placed.
This column is **added** to the output tables.  The default is 
* _bl_source_url_column_name_ - specifies the name of the table column holding the URL from which the document was retrieved.
* _bl_blocked_domain_list_path_ - specifies the directory holding files matching 
the regular expression `domains*`.
 
Additionally, a set of data access-specific arguments are provided that enable
the specification of the location of domain list files, so that these
files could be stored in the local file system or in S3 storage, for example.
The arguments are as follows (and generally match the TransformLauncher's 
data access arguments but with the `bl_' prefix).

* _bl_local_config_ - specifies the input and outout folders, although these are not used by the transform.
* _bl_s3_config_ - specifies the input and output paths in s3.
* _bl_s3_credentials_ - provides credentials to access the s3 storage. 

See the Command Line options below for specifics on these.

## Running
You can run the [blocklist_local.py](src/blocklist_local.py) to
transform the `test1.parquet` file in [test input data](test-data/input) 
to an `output` directory.  The directory will contain both the new
annotated `test1.parquet` file and the `metadata.json` file.
<pre>
% make venv
% source venv/bin/activate
(venv) % cd src
(venv) % python blocklist_local.py
16:22:03 INFO - Running locally
16:22:03 INFO - Using local configuration with: input_folder - /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/input output_folder - /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/output
16:22:03 INFO - Not using data sets, checkpointing False, max files -1
number of workers 5 worker options {'num_cpus': 0.8}
pipeline id pipeline_id; number workers 5
job details {'job category': 'preprocessing', 'job name': 'blocklist', 'job type': 'ray', 'job id': 'job_id'}
code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
actor creation delay 0
16:22:03 INFO - running locally creating Ray cluster
2024-03-06 16:22:05,596	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
(orchestrate pid=33991) Completed 0 files in 2.296765645345052e-06 min. Waiting for completion
(orchestrate pid=33991) 16:22:06 INFO - Number of files is 1, source profile {'max_file_size': 0.0007181167602539062, 'min_file_size': 0.0007181167602539062, 'total_file_size': 0.0007181167602539062}
(orchestrate pid=33991) 16:22:06 INFO - Cluster resources: {'cpus': 10, 'gpus': 0, 'memory': 14.576927185058594, 'object_store': 2.0}
(orchestrate pid=33991) 16:22:06 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=33991) 16:22:06 INFO - Reading domain list from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains 
(orchestrate pid=33991) 16:22:06 INFO - Adding 5 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/gambling/domains
(orchestrate pid=33991) 16:22:06 INFO - Adding 5 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/gambling/domains.24733
(orchestrate pid=33991) 16:22:06 INFO - Adding 4 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/gambling/domains.9309
(orchestrate pid=33991) 16:22:06 INFO - Adding 3 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/arjel/domains
(orchestrate pid=33991) 16:22:06 INFO - Adding 4 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/phishing/domains
(orchestrate pid=33991) 16:22:06 INFO - Adding 10 domains from /Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains/phishing/domains1.gz
(orchestrate pid=33991) 16:22:06 INFO - Added 27 domains to domain list
(orchestrate pid=33991) 16:22:06 INFO - __domain_refs = ObjectRef(00ef45ccd0112571ffffffffffffffffffffffff0100000002e1f505)
(orchestrate pid=33991) Completed processing in 0.014582331975301106 min
(orchestrate pid=33991) 16:22:07 INFO - done flushing in 0.001355886459350586 sec
(TransformTableProcessor pid=33998) 16:22:07 INFO - Blocklist config:{'__domain_refs': ObjectRef(00ef45ccd0112571ffffffffffffffffffffffff0100000002e1f505), 'bl_blocked_domain_list_path': '/Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains', 'bl_annotation_column_name': 'blocklisted', 'bl_source_url_column_name': 'title', 'data_access': <data_processing.data_access.data_access_local.DataAccessLocal object at 0x118914520>}
16:22:17 INFO - Completed execution in 0.2283053994178772 min, execution result 0
(TransformTableProcessor pid=34000) 16:22:07 INFO - Blocklist config:{'__domain_refs': ObjectRef(00ef45ccd0112571ffffffffffffffffffffffff0100000002e1f505), 'bl_blocked_domain_list_path': '/Users/dawood/git/fm-data-engineering/transforms/universal/blocklisting/test-data/domains', 'bl_annotation_column_name': 'blocklisted', 'bl_source_url_column_name': 'title', 'data_access': <data_processing.data_access.data_access_local.DataAccessLocal object at 0x109704580>}
(venv) % deactivate
% ls ../output
metadata.json	test1.parquet
%
</pre>

### Building the Docker Image
```shell
% make image 
...
% podman images
REPOSITORY                            TAG                    IMAGE ID      CREATED         SIZE
localhost/blocklisting                0.1.0                  f6d4fbad1ab3  9 minutes ago   1.14 GB
%

````
In addition, there are some useful `make` targets (see conventions above)
or use `make help` to see a list of available targets.

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
```
--bl_blocked_domain_list_path BL_BLOCKED_DOMAIN_LIST_PATH
                        COS URL or local folder (file or directory) that points to the list of block listed domains.  If not running in Ray, this must be a local folder.
--bl_annotation_column_name BL_ANNOTATION_COLUMN_NAME
                        Name of the table column that contains the block listed domains
--bl_source_url_column_name BL_SOURCE_URL_COLUMN_NAME
                        Name of the table column that has the document download URL
--bl_s3_cred BL_S3_CRED
                        AST string of options for cos credentials. Only required for COS or Lakehouse.
                        access_key: access key help text
                        secret_key: secret key help text
                        url: S3 url
                        Example: { 'access_key': 'AFDSASDFASDFDSF ', 'secret_key': 'XSDFYZZZ', 'url': 's3:/cos-optimal-llm-pile/test/' }
--bl_s3_config BL_S3_CONFIG
                        AST string containing input/output paths.
                        input_path: Path to input folder of files to be processed
                        output_path: Path to output folder of processed files
                        Example: { 'input_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup', 'output_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed' }
--bl_lh_config BL_LH_CONFIG
                        AST string containing input/output using lakehouse.
                        input_table: Path to input folder of files to be processed
                        input_dataset: Path to outpu folder of processed files
                        input_version: Version number to be associated with the input.
                        output_table: Name of table into which data is written
                        output_path: Path to output folder of processed files
                        token: The token to use for Lakehouse authentication
                        lh_environment: Operational environment. One of STAGING or PROD
                        Example: { 'input_table': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup', 'input_dataset': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed', 'input_version': '1.0', 'output_table': 'ededup', 'output_path': '/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed', 'token': 'AASDFZDF', 'lh_environment': 'STAGING' }
--bl_local_config BL_LOCAL_CONFIG
                        ast string containing input/output folders using local fs.
                        input_folder: Path to input folder of files to be processed
                        output_folder: Path to output folder of processed files
                        Example: { 'input_folder': './input', 'output_folder': '/tmp/output' }

```



