

<h1 align="center">Data Prep LAB </h1>

<div align="center">

  [![Status](https://img.shields.io/badge/status-active-success.svg)]() 
  [![GitHub Issues](https://img.shields.io/github/issues/kylelobo/The-Documentation-Compendium.svg)](https://github.ibm.com/ai-models-data/fm-data-engineering/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kylelobo/The-Documentation-Compendium.svg)](https://github.ibm.com/ai-models-data/fm-data-engineering/pulls)
</div>

---

<p align="center"> Data Prep Lab is a cloud native ray based toolkit that allows a user to quickly prepare their data for building LLM applications using set of available transforms. This toolkit gives the user flexibility to run data prep from laptop-scale to cluster-scale, and provides automation via KFP pipelines. Moreover, a user can add in their own data prep module and scale it using ray without having to worry about ray internals. 
    <br> 
</p>

## 📝 Table of Contents
- [About](#about)
- [Setup](#setup)
- [Getting Started](#getting_started)
- [How to contribute](#contribute_steps)
- [Acknowledgments](#acknowledgement)

## &#x1F4D6; About <a name = "about"></a>

Data Prep LAB is an open source toolkit that enables a user to prepare their data for building LLM applications. This toolkit comes with a set of available modules (called as transforms hereafter) that the user can get started with easily to build their data pipelines. This toolkit also provides a framework, called as data processing library, that allows a user to quickly build in their own new transforms. Using the data processing library, a user can bring in their logic for data transformation and use the available framework to build a ray enabled scalable transform in a short period of time. 

Features of the toolkit: 
- Collection of [scalable transformations](transforms) to expedite user onboarding
- [Data processing library](data-processing-lib/README.md) designed to facilitate effortless addition of new scalable transformations
- Designed to operate efficiently and seamlessly from laptop-scale to cluster-scale to support data processing for any data size
- [Kube Flow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/) based automation 

Data modalities supported: \
Code Datasets (Release 0)  and Natural Language Datasets (Release 1)\
We support the starting point for code datasets as downloaded github repos as .zip files. 

### Toolkit Design: 
The toolkit is a python-based library that has ready to use scalable ray based transforms. We use the popular [parquet](https://arrow.apache.org/docs/python/parquet.html) format to store the data. Every parquet file follows a set [schema](https://github.ibm.com/ai-models-data/fm-data-engineering/tree/dev/tools/ingest2parquet). Data is converted from raw form (eg zip files for github repositories) to parquet files by [ingest2parquet](https://github.ibm.com/ai-models-data/fm-data-engineering/tree/dev/tools/ingest2parquet) tool that also adds the necessary fields in the schema.  A user can use one or more of the available transforms to process their data. 

#### Transform design: 
A transform can follow one of the two patterns: filter or annotator-filter pattern. When a transform acts as a filter, it processes the data and outputs the transformed data (example exact deduplication). In the annotator filter design pattern, a transform annotates the result of the processing by adding one more column to the parquet file. Filtering can then be done by a specific filtering module, whose job is to remove rows or columns as specified by user intent expressed as a SQL query. The annotator filter design allows a user to verify the results of the processing before actual filtering of the data. For a new module to be added, a user can pick the right design based on the orign based on the processing to be applied. More details [here](transforms/README.md). 

#### Scaling of transforms: 
The distributed infrastructure, based on [Ray](https://docs.ray.io/en/latest/index.html), is used to scale out the transformation process.

#### Bring Your Own Transform: 
One can add new transforms by bringing in their own processing logic and using the framework to build scalable transforms. More details [here](data-processing-lib/doc/overview.md). 

#### Automation: 
The toolkit also supports transform execution automation based on 
[Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)(KFP) and
tested on [Kind cluster](https://kind.sigs.k8s.io/). KFP implementation is based on [KubeRay Operator](https://docs.ray.io/en/master/cluster/kubernetes/getting-started.html)
for creating and managing Ray cluster and [KubeRay API server](https://github.com/ray-project/kuberay/tree/master/apiserver)
to interact with the KubeRay operator. An additional [framework](kfp/kfp_support_lib) along with the several
[kfp components](kfp/kfp_ray_components) is used to simplify pipelines implementation.




## &#x2699; Setup <a name = "setup"></a>


### Prerequisites

Python 3.10 or 3.11 \
pre-commit\
twine \
Docker/Podman


### Installation Steps

```shell
git clone git@github.ibm.com:ai-models-data/fm-data-engineering.git
cd fm-data-engineering
pip install pre-commit
pip install twine
pre-commit install
```

## &#x1F680; Getting Started <a name = "getting_started"></a>

There are various entry points that one can choose based on their usecase. Below are a few demos to get you started. 

### Run a single transform on local-ray
Get started by running the noop transform that performs an identity operation by following the [tutorial](data-processing-lib/doc/simplest-transform-tutorial.md) and [code](https://github.ibm.com/ai-models-data/fm-data-engineering/tree/dev/transforms/universal/noop) shared here. 

### Run a data pipeline on local-ray
Get started by building a data pipeline with our example pipeline (link to be added) that can run on a laptop. 

### Automate the pipeline
Link to the KFP tutorial to be added

### How to navigate the repository
See documentation on [repository structure and its use](doc/repo.md)

## &#x1F91D; How to contribute <a name = "contribute_steps"></a>
TBA

## &#x2B50; Acknowledgements <a name = "acknowledgement"></a>
Thanks to [BigCode Project](https://github.com/bigcode-project) that has been used to build the code quality module. 






