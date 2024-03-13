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
git clone git@github.ibm.com:ai-models-data/fm-data-engineering.git
cd kfp/kfp_support_lib
pre-commit install
```
If you don't have pre-commit, you can install from [here](https://pre-commit.com/)

## Library Artifact Build and Publish
