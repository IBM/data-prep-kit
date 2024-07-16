# Shared Workflow Support

This provides support for implementing KFP pipelines automating transform's execution. This library is not dependent on 
KFP version. KFP dependent modules are in [kfp_v1_workflow_support](../kfp_v1_workflow_support) and 
[kfp_v2_workflow_support](../kfp_v2_workflow_support)

this module combines 2 inner modules

* [python apiserver client](src/python_apiserver_client/README.md), which is a copy of
[Kuberay API server-client python APIs](https://github.com/ray-project/kuberay/tree/master/clients/python-apiserver-client)
We added it into the project, because these APIs are not exposed by any PyPi.
* [runtime_utils](src/runtime_utils/README.md) 

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
cd kfp/kfp_support_lib/shared_workflow_support
pre-commit install
```
If you don't have pre-commit, you can install from [here](https://pre-commit.com/)

## Library Artifact Build and Publish

The process of creating a release for the package involves the following steps:

- cd to the package directory.
- update the `DPK_LIB_KFP_SHARED` version in [.make.versions](../../../.make.versions) file.
- run `make set-versions` and `make build` and `make publish`.

## Testing

To run the package tests perform the following:

To begin with, establish a Kind cluster and deploy all required components by executing the makfefile command in the main directory of this repository. As an alternative, you can manually execute the instructions provided in the [README.md](../../../scripts/k8s-setup/README.md) file.

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

