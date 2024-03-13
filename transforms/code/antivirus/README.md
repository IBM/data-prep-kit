# Malware Annotation Transform 
Please see the set of
[transform project conventions](../../README.md#Transform-Project-Conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This filter scans the `contents` column of an input dataset using ClamAV, and outputs corresponding tables containing `virus_detection` column.

If a virus is detected, the `virus_detection` column contains the detected virus signature name; otherwise `null` (`None`).

## Configuration and Command Line Options

The set of dictionary keys holding [AntivirusTransform](src/antivirus_transform.py) 
configuration for values are as follows:

* _input_column_ - specifies the input column's name to scan. (default: `contents`)
* _output_column_ - specifies the output column's name of the detected virus signature name. (default: `virus_detection`)
* _clamd_socket_ - specifies the socket path for ClamAV. (default: `/var/run/clamav/clamd.ctl`)

## Running
You can run the [antivirus_local.py](src/antivirus_local.py) to
transform the `sample.parquet` file in [test input data](test-data/input) 
to an `output` directory.  The directory will contain both the new
annotated `sample.parquet` file and the `metadata.json` file.
<pre>
% export DOCKER=docker
% make -e venv
% source venv/bin/activate
7bb01ab8a5afdd299bf7a57a4d81f0ee02131c159f03fbf47b2054003a3481b2
(venv) % docker ps
CONTAINER ID   IMAGE                 COMMAND                  CREATED         STATUS         PORTS                    NAMES
7bb01ab8a5af   clamav-local:latest   "/bin/bash -c 'fresh…"   3 seconds ago   Up 2 seconds   0.0.0.0:3310->3310/tcp   clamav
(venv) % cd src
(venv) % python antivirus_local.py
00:51:25 INFO - Running locally
00:51:25 INFO - antivirus parameters are : {'input_column': 'contents', 'output_column': 'virus_detection', 'clamd_socket': '../.tmp/clamd.ctl'}
00:51:25 INFO - Using local configuration with: input_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input output_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/output
00:51:25 INFO - Not using data sets, checkpointing False, max files -1
number of workers 5 worker options {'num_cpus': 0.8}
pipeline id pipeline_id; number workers 5
job details {'job category': 'preprocessing', 'job name': 'Antivirus', 'job type': 'ray', 'job id': 'job_id'}
code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
actor creation delay 0
00:51:25 INFO - running locally creating Ray cluster
2024-03-13 00:51:30,906 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265
00:51:32 INFO - Starting orchestrator
(orchestrate pid=180576) 00:51:35 INFO - orchestrator started at 2024-03-13 00:51:35
(orchestrate pid=180576) 00:51:35 INFO - Number of files is 1, source profile {'max_file_size': 0.00240325927734375, 'min_file_size': 0.00240325927734375, 'total_file_size': 0.00240325927734375}
(orchestrate pid=180576) 00:51:35 INFO - Cluster resources: {'cpus': 8, 'gpus': 0, 'memory': 6.961594391614199, 'object_store': 3.4807971948757768}
(orchestrate pid=180576) 00:51:35 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=180576) 00:51:36 INFO - Completed 0 files in 0.000544595718383789 min. Waiting for completion
(TransformTableProcessor pid=180905) 00:51:39 INFO - Using unix socket: ../.tmp/clamd.ctl
(orchestrate pid=180576) 00:51:40 INFO - Completed processing in 0.06612720489501953 min
(TransformTableProcessor pid=180905) 00:51:40 WARNING - Exception 'NoneType' object is not iterable processing file /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input/sample.parquet: Traceback (most recent call last):
(TransformTableProcessor pid=180905)   File "/home/tkyg/fm-data-engineering/data-processing-lib/src/data_processing/ray/transform_table_processor.py", line 64, in process_table
(TransformTableProcessor pid=180905)     out_tables, stats = self.transform.transform(table=table)
(TransformTableProcessor pid=180905)   File "/home/tkyg/fm-data-engineering/transforms/code/antivirus/src/antivirus_transform.py", line 72, in transform
(TransformTableProcessor pid=180905)     table.append_column(self.output_column, virus_detection)
(TransformTableProcessor pid=180905)   File "pyarrow/table.pxi", line 4483, in pyarrow.lib.Table.append_column
(TransformTableProcessor pid=180905)   File "pyarrow/table.pxi", line 4432, in pyarrow.lib.Table.add_column
(TransformTableProcessor pid=180905)   File "pyarrow/table.pxi", line 1380, in pyarrow.lib.chunked_array
(TransformTableProcessor pid=180905)   File "pyarrow/array.pxi", line 344, in pyarrow.lib.array
(TransformTableProcessor pid=180905)   File "pyarrow/array.pxi", line 42, in pyarrow.lib._sequence_to_array
(TransformTableProcessor pid=180905)   File "pyarrow/error.pxi", line 154, in pyarrow.lib.pyarrow_internal_check_status
(TransformTableProcessor pid=180905)   File "pyarrow/types.pxi", line 88, in pyarrow.lib._datatype_to_pep3118
(TransformTableProcessor pid=180905) TypeError: 'NoneType' object is not iterable
(TransformTableProcessor pid=180905)
00:51:40 INFO - Completed orchestrator
00:51:50 INFO - Completed execution in 0.4105185270309448 min, execution result 0
(TransformTableProcessor pid=180903) 00:51:40 INFO - Using unix socket: ../.tmp/clamd.ctl [repeated 4x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(antivirus) tkyg@tkyg21:~/granite/fm-data-engineering/transforms/code/antivirus/src$ python antivirus_local.py
00:53:27 INFO - Running locally
00:53:27 INFO - antivirus parameters are : {'input_column': 'contents', 'output_column': 'virus_detection', 'clamd_socket': '../.tmp/clamd.ctl'}
00:53:27 INFO - Using local configuration with: input_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input output_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/output
00:53:27 INFO - Not using data sets, checkpointing False, max files -1
number of workers 5 worker options {'num_cpus': 0.8}
pipeline id pipeline_id; number workers 5
job details {'job category': 'preprocessing', 'job name': 'Antivirus', 'job type': 'ray', 'job id': 'job_id'}
code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
actor creation delay 0
00:53:27 INFO - running locally creating Ray cluster
2024-03-13 00:53:33,295 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265
00:53:35 INFO - Starting orchestrator
(orchestrate pid=181325) 00:53:38 INFO - orchestrator started at 2024-03-13 00:53:38
(orchestrate pid=181325) 00:53:38 INFO - Number of files is 1, source profile {'max_file_size': 0.00240325927734375, 'min_file_size': 0.00240325927734375, 'total_file_size': 0.00240325927734375}
(orchestrate pid=181325) 00:53:38 INFO - Cluster resources: {'cpus': 8, 'gpus': 0, 'memory': 6.959992218762636, 'object_store': 3.4799961084499955}
(orchestrate pid=181325) 00:53:38 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=181325) 00:53:38 INFO - Completed 0 files in 4.4290224711100264e-05 min. Waiting for completion
(TransformTableProcessor pid=181646) 00:53:42 INFO - Using unix socket: ../.tmp/clamd.ctl
(orchestrate pid=181325) 00:53:43 INFO - Completed processing in 0.06886642773946126 min
00:53:43 INFO - Completed orchestrator
00:53:53 INFO - Completed execution in 0.43177722295125326 min, execution result 0
(TransformTableProcessor pid=181649) 00:53:43 INFO - Using unix socket: ../.tmp/clamd.ctl [repeated 4x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(venv) % deactivate
7bb01ab8a5af
% docker ps
CONTAINER ID   IMAGE           COMMAND                  CREATED       STATUS       PORTS     NAMES
% ls ../test-data/output/
metadata.json	sample.parquet
%
</pre>

In addition, there are some useful `make` targets (see conventions above):
* `make venv` - creates the virtual environment.
* `make test` - runs the tests in [test](test) directory
* `make build` - to build the docker image
* `make help` - displays the available `make` targets and help text.





