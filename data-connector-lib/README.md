# DPK Connector

DPK Connector is a scalable and compliant web crawler developed for data acquisition towards LLM development. It is built on [Scrapy](https://scrapy.org/).
For more details read [the documentation](doc/overview.md).

## Virtual Environment

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

To test, build and publish the library
```shell
make test build publish
```

To up the version number, edit the Makefile to change VERSION and rerun the above. This will require committing both the `Makefile` and the autotmatically updated `pyproject.toml` file.

## How to use

See [the overview](doc/overview.md).
