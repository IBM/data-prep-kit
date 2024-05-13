# KFP support library

This provides support for implementing KFP pipelines automating transform's execution.
It comprises 2 main modules

* [api server client](src/kfp_support/api_server_client/README.md) 
* [workflow support](src/kfp_support/workflow_support/README.md)

## Development

### Requirements
1. python 3.10 or later
2. git command line tools
3. [pre-commit](https://pre-commit.com/)
4. twine (pip install twine)
    * but on Mac you may have to include a dir in your PATH, such as `export PATH=$PATH:/Library/Frameworks/Python.framework/Versions/3.10/bin`

### Git
Simple clone the repo and set up the pre-commit hooks.
```shell
git clone git@github.com:IBM/data-prep-kit.git
cd kfp/kfp_support_lib
pre-commit install
```
If you don't have pre-commit, you can install from [here](https://pre-commit.com/)

## Library Artifact Build and Publish

The process of creating a release for `fm_data_processing_kfp` package  involves the following steps:

cd to the package directory.

update the version in [requirements.env](../requirements.env) file.

run `make build` and `make publish`.

## Testing

To run the package tests perform the following:

To begin with, establish a Kind cluster and deploy all required components by executing the makfefile command in the main directory of this repository. As an alternative, you can manually execute the instructions provided in the [README.md](../../kind/README.md) file.

```bash
make setup
```

The next step is to deploy the `data-prep-kit-kfp` package locally within a Python virtual environment.

```bash
make  build
```

lastly, execute the tests:

```bash
make test
```

### Cleanup

It is advisable to execute the following command prior to running `make test` once more. This will ensure that any 
previous test runs resources are removed before starting new tests.

```bash
kubectl delete workflows -n kubeflow --all
```


