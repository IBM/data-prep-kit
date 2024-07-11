# Running a Transform Image

Here we address a simple use case of applying a single transform to a 
set of parquet files.
We'll use one of the pre-built docker images from the repository
to process the data.
Additionally, what follows uses the 
[python runtime](../../data-processing-lib/doc/python-runtime.md)
image, but the examples below should also work for the
[ray](../../data-processing-lib/doc/ray-runtime.md)
or
[spark](../../data-processing-lib/doc/spark-runtime.md)
runtime images (`-ray` or `-spark` image name suffix instead of `-python`).

### Getting an image
You may build the transform locally in the repository, for example,
```shell
cd transforms/universal/noop/python
make image
docker images | grep noop
```
produces
```
quay.io/dataprep1/data-prep-kit/noop-python                     latest   aac55fa3d82d  5 seconds ago  355 MB
```
Or, you can use the pre-built images (latest, or 0.2.1 or later tags) 
on quay.io found at [https://quay.io/user/dataprep1](https://quay.io/user/dataprep1).

### Local Data
Images built in this repository
include directories for mounting input and output directories. 
Those directories are `/home/dpk/input` and `/home/dpk/output`.

To process data in the `/home/me/input` directory and write it
to the `/home/me/output` directory, we mount these directories into
the image at the above mount points.
So for example, using the locally built `noop` transform:

```shell
docker run  --rm 
    -v /home/me/input:/home/dpk/input \
    -v /home/me/output:/home/dpk/output \
    noop-python:latest 	\
	python noop_transform_python.py \
	--data_local_config "{ \
	    'input_folder'  : '/home/dpk/input', \
	    'output_folder' : '/home/dpk/output' \
	    }"

```
To run the quay.io located transform instead, substitute 
`quay.io/dataprep1/data-prep-kit/noop-python:latest`
for `noop-python:latest`, as follows:
```shell
docker run  --rm 
    -v /home/me/input:/home/dpk/input \
    -v /home/me/output:/home/dpk/output \
    quay.io/dataprep1/data-prep-kit/noop-python:latest 	\
	python noop_transform_python.py \
	--data_local_config "{ \
	    'input_folder'  : '/home/dpk/input', \
	    'output_folder' : '/home/dpk/output' \
	    }"

```

### S3-located Data
When processing data located in S3 buckets, one can use the same image
and specify different `--data_s3_*` configuration as follows: 

```shell
docker run  --rm 
    noop-python:latest 	\
	python noop_transform_python.py \
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
