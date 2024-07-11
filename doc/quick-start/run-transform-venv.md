# Running a Transform in a Virtual Environment 
Here we address a simple use case of applying a single transform to a 
set of parquet files. 
We'll use the noop transform as an example, but in general, this process
will work for any of the transforms contained in the repository.
Additionally, what follows uses the 
[python runtime](../../data-processing-lib/doc/python-runtime.md)
(e.g., noop/python directory), 
but the examples below should also work for the
[ray](../../data-processing-lib/doc/ray-runtime.md)
(noop/ray directory)
or
[spark ](../../data-processing-lib/doc/spark-runtime.md)
(noop/spark directory)
runtimes.

### Creating the Virtual Environment
Each transform project contains a Makefile that will assist in building
the virtual environment in a directory named `venv`.
To create the virtual environment for the `noop` transform:

```shell
cd transforms/univeral/noop/python
make venv 
```
Note, if needed, you can override the default `python` command used 
in `make venv` above, with for example:

```shell
make PYTHON=python3.10 venv
```

### Local Data
To process data in the `/home/me/input` directory and write it
to the `/home/me/output` directory, activate the virtual environment
and then call the transform referencing these directories.
So for example, using the `noop` transform 
to read parquet files from `/home/me/input`:

```shell
cd transforms/univeral/noop/python
source venv/bin/activate
python src/noop_transform_python.py \
    --data_local_config "{ \
	    'input_folder'  : '/home/me/input', \
	    'output_folder' : '/home/me/output' \
	    }"
deactivate
```

### S3-located Data
When processing data located in S3 buckets, one can use the same 
approach and specify different `--data_s3_*` configuration as follows: 

```shell
cd transforms/univerals/noop/python
source venv/bin/activate
python src/noop_transform_python.py \
	--data_s3_cred "{ \
	    'access_key'  : '...', \
	    'secret_key' : '...', \
	    'url' : '...', \
	    }"  \
	--data_s3_config "{ \
	    'input_folder'  : '...', \
	    'output_folder' : '...', \
	    }"  
```
