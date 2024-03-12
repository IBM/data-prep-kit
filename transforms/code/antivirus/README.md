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
* _network_socket_ - specifies the network socket value. e.g. `localhost:3310` As a default, the unix socket will be used. If you want to run this filter with a ClamAV server like a docker container, you need to specifies this option.

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
03:01:05 INFO - Running locally
03:01:05 INFO - antivirus parameters are : {'input_column': 'contents', 'output_column': 'virus_detection', 'use_network_socket': True, 'network_socket_host': 'localhost', 'network_socket_port': 3310}
03:01:05 INFO - Using local configuration with: input_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input output_folder - /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/output
03:01:05 INFO - Not using data sets, checkpointing False, max files -1
number of workers 5 worker options {'num_cpus': 0.8}
pipeline id pipeline_id; number workers 5
job details {'job category': 'preprocessing', 'job name': 'Antivirus', 'job type': 'ray', 'job id': 'job_id'}
code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
actor creation delay 0
03:01:05 INFO - running locally creating Ray cluster
2024-03-12 03:01:10,703 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265
03:01:12 INFO - Starting orchestrator
(orchestrate pid=131278) 03:01:15 INFO - orchestrator started at 2024-03-12 03:01:15
(orchestrate pid=131278) 03:01:15 INFO - Number of files is 1, source profile {'max_file_size': 0.00240325927734375, 'min_file_size': 0.00240325927734375, 'total_file_size': 0.00240325927734375}
(orchestrate pid=131278) 03:01:15 INFO - Cluster resources: {'cpus': 8, 'gpus': 0, 'memory': 6.625856782309711, 'object_store': 3.3129283897578716}
(orchestrate pid=131278) 03:01:15 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=131278) 03:01:15 INFO - Creating actors
(orchestrate pid=131278) 03:01:15 INFO - Begin processing files
(orchestrate pid=131278) 03:01:15 INFO - Completed 0 files in 2.0261605580647787e-05 min. Waiting for completion
(TransformTableProcessor pid=131602) 03:01:19 INFO - Using network scoket: localhost:3310
(orchestrate pid=131278) 03:01:20 INFO - Completed processing in 0.0731032133102417 min
(orchestrate pid=131278) 03:01:20 INFO - Done processing files, waiting for flush() completion.
(orchestrate pid=131278) 03:01:20 INFO - done flushing in 0.010388851165771484 sec
(orchestrate pid=131278) 03:01:20 INFO - Computing execution stats
(TransformTableProcessor pid=131600) 03:01:20 INFO - Begin flushing transform
(TransformTableProcessor pid=131600) 03:01:20 INFO - Done flushing transform
(TransformTableProcessor pid=131600) 03:01:20 INFO - Transform did not produce a transformed table for file
(TransformTableProcessor pid=131603) 03:01:20 INFO - Begin processing file /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input/sample.parquet
(TransformTableProcessor pid=131603) 03:01:20 INFO - Begin transforming table from /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input/sample.parquet
(TransformTableProcessor pid=131603) 03:01:20 INFO - Done transforming table from /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input/sample.parquet
(TransformTableProcessor pid=131603) 03:01:20 INFO - Writing transformed file /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/input/sample.parquet to /home/tkyg/fm-data-engineering/transforms/code/antivirus/test-data/output/sample.parquet
03:01:20 INFO - Completed orchestrator
(orchestrate pid=131278) 03:01:20 INFO - Building job metadata
(orchestrate pid=131278) 03:01:20 INFO - Saved job metadata.
03:01:30 INFO - Completed execution in 0.41824158032735187 min, execution result 0
(TransformTableProcessor pid=131601) 03:01:20 INFO - Using network scoket: localhost:3310 [repeated 4x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(TransformTableProcessor pid=131601) 03:01:20 INFO - Begin flushing transform [repeated 4x across cluster]
(TransformTableProcessor pid=131601) 03:01:20 INFO - Done flushing transform [repeated 4x across cluster]
(TransformTableProcessor pid=131601) 03:01:20 INFO - Transform did not produce a transformed table for file   [repeated 4x across cluster]
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





