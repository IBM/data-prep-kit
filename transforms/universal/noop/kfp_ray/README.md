# NOOP Ray-base KubeFlow Pipeline Transformation 


## Summary 
This project allows execution of the [noop Ray transform](../ray) as a 
[KubeFlow Pipeline](https://www.kubeflow.org/docs/components/pipelines/overview/)

The detail pipeline is presented in the [Simplest Transform pipeline tutorial](../../../../kfp/doc/simple_transform_pipeline.md) 

## Generate
In order to generate a pipeline python file run
```shell
make workflow-generate
```
This will use the [pipeline_definitions.yaml](pipeline_definitions.yaml) to generate the python file of the pipeline. It uses the [pipeline generator](../../../../kfp/pipeline_generator/single-pipeline/) directory.

## Compilation

In order to compile pipeline definitions run
```shell
make workflow-build
```
from the directory. It creates a virtual environment (make workflow-venv) and after that compiles the pipeline 
definitions in the folder. The virtual environment is created once for all transformers. 

Note: the pipelines definitions can be compiled and executed on KFPv1 and KFPv2. Meantime, KFPv1 is our default. If you
prefer KFPv2, please do the following:
```shell
make clean
export KFPv2=1
make workflow-build
```

The next steps are described in [Deploying a pipeline](../../../../kfp/doc/simple_transform_pipeline.md#deploying-a-pipeline-)
and [Executing pipeline and watching execution results](../../../../kfp/doc/simple_transform_pipeline.md#executing-pipeline-and-watching-execution-results-)