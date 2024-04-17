# Data Processing Library
This provides a python framework for developing _transforms_
on data stored in files - currently parquet files are supported -
and running them in a [ray](https://ray.com) cluster.
Data files may be stored in the local file system or  COS/S3.
For more details see the [documentation](doc/overview.md).

### Virtual Environment
The project uses `pyproject.toml` and a Makefile for operations.
To do development you should establish the virtual environment
```shell
make venv
```
and then either activate
```shell
source venv/bin/activate
```
or set up your IDE to use the venv directory when developing in this project

## Library Artifact Build and Publish
To test, build and publish the library to artifactory
```shell
make test build publish
```
To up the version number, edit the Makefile to change VERSION and rerun
the above.  This will require committing both the `Makefile` and the
autotmatically updated `pyproject.toml` file.


