# NOOP Transform 
Please see the set of
[transform project conventions](../../transform-conventions.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
This transforms serves as a template for transform writers as it does
not perform any transformations on the input (i.e., a no-operation transform).
As such it simply copies the input parquet files to the output directory.
It shows the basics of creating a simple 1:1 table transform.
It also implements a single configuration value to show how configuration
of the transform is implemented.

## Configuration and command line Options

The set of dictionary keys holding [NOOPTransform](src/noop_transform.py) 
configuration for values are as follows:

* _noop_sleep_sec_ - specifies the number of seconds to sleep during table transformation. 

## Running
You can run the [noop_local_ray.py](src/noop_local_ray.py) to
transform the `test1.parquet` file in [test input data](test-data/input) 
to an `output` directory.  The directory will contain both the new
annotated `test1.parquet` file and the `metadata.json` file.
<pre>
% make venv
% source venv/bin/activate
(venv) % cd src
(venv) % python noop_local_ray.py
18:36:22 INFO - Running locally
noop parameters are : {'sleep': 5}
18:36:22 INFO - Using local configuration with: input_folder - /Users/dawood/git/fm-data-engineering/transforms/universal/noop/test-data output_folder - /Users/dawood/git/fm-data-engineering/transforms/universal/noop/output
18:36:22 INFO - Not using data sets, checkpointing False, max files -1
number of workers 5 worker options {'num_cpus': 0.8}
pipeline id pipeline_id; number workers 5
job details {'job category': 'preprocessing', 'job name': 'NOOP', 'job type': 'ray', 'job id': 'job_id'}
code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
actor creation delay 0
18:36:22 INFO - running locally creating Ray cluster
2024-03-06 18:36:25,305	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
(orchestrate pid=40827) 18:36:26 INFO - Number of files is 3, source profile {'max_file_size': 0.034458160400390625, 'min_file_size': 0.034458160400390625, 'total_file_size': 0.10337448120117188}
(orchestrate pid=40827) 18:36:26 INFO - Cluster resources: {'cpus': 10, 'gpus': 0, 'memory': 14.333731079474092, 'object_store': 2.0}
(orchestrate pid=40827) 18:36:26 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=40827) Completed 0 files in 5.348523457845052e-06 min. Waiting for completion
(TransformTableProcessor pid=40842) 18:36:27 INFO - Sleep for 5 seconds
(orchestrate pid=40827) 18:36:32 INFO - done flushing in 0.0018157958984375 sec
(orchestrate pid=40827) Completed processing in 0.10258894761403402 min
(TransformTableProcessor pid=40842) 18:36:32 INFO - Sleep completed - continue
18:36:42 INFO - Completed execution in 0.33264248371124266 min, execution result 0
(TransformTableProcessor pid=40843) 18:36:27 INFO - Sleep for 5 seconds
(venv) % deactivate
% ls ../output
metadata.json	test1.parquet
%
</pre>

In addition, there are some useful `make` targets (see conventions above):
* `make venv` - creates the virtual environment.
* `make test` - runs the tests in [test](test) directory
* `make build` - to build the docker image
* `make help` - displays the available `make` targets and help text.





