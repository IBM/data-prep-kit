# Malware Annotation Transform 
Please see the set of
[transform project conventions](../../README.md#Transform-Project-Conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This filter scans the 'contents' column of an input table using [ClamAV](https://www.clamav.net/), and outputs corresponding tables containing 'virus_detection' column (by default).

If a virus is detected, the 'virus_detection' column contains the detected virus signature name; otherwise [null](https://arrow.apache.org/docs/python/generated/pyarrow.null.html).

## Configuration and Command Line Options

The set of dictionary keys holding [AntivirusTransform](src/antivirus_transform.py) 
configuration for values are as follows:

* _antivirus_input_column_ - specifies the input column's name to scan. (default: `contents`)
* _antivirus_output_column_ - specifies the output column's name of the detected virus signature name. (default: `virus_detection`)
* _antivirus_clamd_socket_ - specifies the socket path for ClamAV. (default: `/var/run/clamav/clamd.ctl`)

## For local running/testing on Mac

For testing and running this transform on local, we are using a unix socket shared with a docker container.
However, docker for mac doesn't support a shared unix socket.
For Mac users, ClamAV will be set up by running `make venv`.
If thet script doesn't work for you, please ensure that you have installed `clamd` command, and it runs with a local unix socket: `/var/run/clamav/clamd.ctl`.

Example for manual set up for Mac:

1. Install ClamAV with Homebrew
    ```sh
    brew install clamav
    ```
1. Copy and edit config files.
    ```sh
    cp $(brew --prefix)/etc/clamav/clamd.conf.sample $(brew --prefix)/etc/clamav/clamd.conf
    sed -i '' -e 's/^Example/# Example/' $(brew --prefix)/etc/clamav/clamd.conf
    echo "DatabaseDirectory /var/lib/clamav" >> $(brew --prefix)/etc/clamav/clamd.conf
    echo "LocalSocket /var/run/clamav/clamd.ctl" >> $(brew --prefix)/etc/clamav/clamd.conf
    cp $(brew --prefix)/etc/clamav/freshclam.conf.sample $(brew --prefix)/etc/clamav/freshclam.conf
    sed -i '' -e 's/^Example/# Example/' $(brew --prefix)/etc/clamav/freshclam.conf
    echo "DatabaseDirectory /var/lib/clamav" >> $(brew --prefix)/etc/clamav/freshclam.conf
    ```
1. Create a directory for a local unix socket
    ```sh
    sudo mkdir -p /var/run/clamav
    sudo chown $(id -u):$(id -g) /var/run/clamav
    ```
1. Create a direcotry for a database of ClamAV
    ```sh
    sudo mkdir -p /var/lib/clamav
    sudo chown $(id -u):$(id -g) /var/lib/clamav
    ```
1. Update a database of ClamAV
    ```sh
    freshclam
    ```
1. Edit `venv/bin/activate`, and add following lines to start `clamd` by `source venv/bin/activate`
    ```sh
    if [ ! -e /var/run/clamav/clamd.ctl ]; then
        clamd --config-file=$(brew --prefix)/etc/clamav/clamd.conf
    fi
    ```

## Running
You can run the [antivirus_local_ray.py](src/antivirus_local_ray.py) to
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
19:29:22 INFO - Running locally
19:29:22 INFO - antivirus parameters are : {'antivirus_input_column': 'contents', 'antivirus_output_column': 'virus_detection', 'antivirus_clamd_socket': '../.tmp/clamd.ctl'}
19:29:22 INFO - Using local configuration with: input_folder - /home/tkyg/granite/fm-data-engineering/transforms/code/antivirus/test-data/input output_folder - /home/tkyg/granite/fm-data-engineering/transforms/code/antivirus/output
19:29:22 INFO - Not using data sets, checkpointing False, max files -1
19:29:22 INFO - number of workers 5 worker options {'num_cpus': 0.8}
19:29:22 INFO - pipeline id pipeline_id; number workers 5
19:29:22 INFO - job details {'job category': 'preprocessing', 'job name': 'Antivirus', 'job type': 'ray', 'job id': 'job_id'}
19:29:22 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
19:29:22 INFO - actor creation delay 0
2024-03-14 19:29:27,801 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265
(orchestrate pid=220478) 19:29:32 INFO - orchestrator started at 2024-03-14 19:29:32
(orchestrate pid=220478) 19:29:32 INFO - Number of files is 1, source profile {'max_file_size': 0.00240325927734375, 'min_file_size': 0.00240325927734375, 'total_file_size': 0.00240325927734375}
(orchestrate pid=220478) 19:29:32 INFO - Cluster resources: {'cpus': 8, 'gpus': 0, 'memory': 6.4991294872015715, 'object_store': 3.249564742669463}
(orchestrate pid=220478) 19:29:32 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=220478) 19:29:32 INFO - Completed 0 files in 3.562370936075846e-05 min. Waiting for completion
(orchestrate pid=220478) 19:29:35 INFO - Completed processing in 0.05764161348342896 min
(TransformTableProcessor pid=220802) 19:29:35 INFO - Using unix socket: ../.tmp/clamd.ctl
19:29:36 INFO - Completed orchestrator
19:29:46 INFO - Completed execution in 0.38614246050516765 min, execution result 0
(TransformTableProcessor pid=220799) 19:29:35 INFO - Using unix socket: ../.tmp/clamd.ctl [repeated 4x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(venv) % deactivate
7bb01ab8a5af
% docker ps
CONTAINER ID   IMAGE           COMMAND                  CREATED       STATUS       PORTS     NAMES
% ls ../output/
metadata.json	sample.parquet
%
</pre>

In addition, there are some useful `make` targets (see conventions above):
* `make venv` - creates the virtual environment.
* `make test` - runs the tests in [test](test) directory
* `make build` - to build the docker image
* `make help` - displays the available `make` targets and help text.





