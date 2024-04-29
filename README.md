

<h1 align="center">Data Prep LAB </h1>

<div align="center">

  [![Status](https://img.shields.io/badge/status-active-success.svg)]() 
  [![GitHub Issues](https://img.shields.io/github/issues/kylelobo/The-Documentation-Compendium.svg)](https://github.com/IBM/data-prep-lab/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kylelobo/The-Documentation-Compendium.svg)](https://github.com/IBM/data-prep-lab/pulls)
</div>

---

Data Prep Lab is a cloud native [Ray](https://docs.ray.io/en/latest/index.html)
based toolkit that allows a user to quickly prepare their data for building LLM applications using a set of available transforms.
This toolkit gives the user flexibility to run data prep from laptop-scale to cluster-scale, 
and provides automation via [KFP pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/). Moreover, a user can add in their own data prep module and scale it 
using Ray without having to worry about Ray internals. 

## 📝 Table of Contents
- [About](#about)
- [Setup](#setup)
- [Getting Started](#getting_started)
- [How to contribute](#contribute_steps)
- [Acknowledgments](#acknowledgement)

## &#x1F4D6; About <a name = "about"></a>

Data Prep LAB is an toolkit that enables users to prepare their data for building LLM applications.
This toolkit comes with a set of available modules (known as transforms) that the user can get started 
with to easily build customized data pipelines.
This set of transforms is built on a framework, known as the data processing library, 
that allows a user to quickly build in their own new transforms and then scale them as needed.
Users can incorporate their logic for custom data transformation and then use the included Ray-based
distributed computing framework to scalably apply the transform to their data. 

Features of the toolkit: 

- Collection of [scalable transformations](transforms) to expedite user onboarding
- [Data processing library](data-processing-lib) designed to facilitate effortless addition and deployment of new scalable transformations
- Operate efficiently and seamlessly from laptop-scale to cluster-scale supporting data processing at any data size
- [Kube Flow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)-based [workflow automation](kfp) of transforms.

Data modalities supported: 

* Code - support for code datasets as downloaded .zip files of github repositories converted to . 
[parquet](https://arrow.apache.org/docs/python/parquet.html) files. 
* Language - Future releases will provide transforms specific to natural language, and like code transforms will operate on parquet files.

Support for additional data formats are expected. 

### Toolkit Design: 
The toolkit is a python-based library that has ready to use scalable Ray based transforms. 
We use the popular point [parquet](https://arrow.apache.org/docs/python/parquet.html) format to store the data (code or language). 
Every parquet file follows a set 
[schema](tools/ingest2parquet/).
Data is converted from raw form (eg zip files for github repositories) to parquet files by the
[ingest2parquet](tools/ingest2parquet/) 
tool that also adds the necessary fields in the schema.  
A user can use one or more of the [available transforms](transforms) to process their data. 

#### Transform design: 
A transform can follow one of the two patterns: filter or annotator pattern.
In the annotator design pattern, a transform adds information during the processing by adding one more column to the parquet file.
The annotator design also allows a user to verify the results of the processing before actual filtering of the data.
When a transform acts as a filter, it processes the data and outputs the transformed data (example exact deduplication).
A general purpose [SQL-based filter transform](transforms/filter) enables a powerful mechanism for identifying 
columns and rows of interest for downstream processing.
For a new module to be added, a user can pick the right design based on the processing to be applied. More details [here](transforms). 

#### Scaling of transforms: 
The distributed infrastructure, based on [Ray](https://docs.ray.io/en/latest/index.html), is used to scale out the transformation process.
A generalized workflow is shown [here](doc/data-processing.md).

#### Bring Your Own Transform: 
One can add new transforms by bringing in their own processing logic and using the framework to build scalable transforms.
More details on the data processing library [here](data-processing-lib/doc/overview.md). 

#### Automation: 
The toolkit also supports transform execution automation based on 
[Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)(KFP) and
tested on [Kind cluster](https://kind.sigs.k8s.io/). KFP implementation is based on [KubeRay Operator](https://docs.ray.io/en/master/cluster/kubernetes/getting-started.html)
for creating and managing Ray cluster and [KubeRay API server](https://github.com/ray-project/kuberay/tree/master/apiserver)
to interact with the KubeRay operator. An additional [framework](kfp/kfp_support_lib) along with the several
[kfp components](kfp/kfp_ray_components) is used to simplify pipelines implementation.


## &#x2699; Setup <a name = "setup"></a>

We tried the project on different hardware/software configurations(see [Apple/Mac considerations](doc/mac.md).)
We recommend using a laptop with at least 16GB of memory and 8 CPUs for development without KFP, 
and at least 32GB and preferably 16 CPUs if you plan to run KFP on Kind.

### Prerequisites

Python 3.10 or 3.11 
[pre-commit](https://pre-commit.com/)
twine 
Docker/Podman

### Installation Steps

```shell
git clone git@github.com:IBM/data-prep-lab.git
cd data-prep-lab
pip install pre-commit
pip install twine
pre-commit install
```
Additionally if you will be using local Minio for S3 testing you need to install `Minio` and `mc`.
Refer to [Minio install instructions](data-processing-lib/doc/using_s3_transformers.md) for more details.

## &#x1F680; Getting Started <a name = "getting_started"></a>

There are various entry points that one can choose based on their use case. Below are a few demos to get you started. 

### Run a single transform on local-ray
Get started by running the noop transform that performs an identity operation by following the 
[tutorial](data-processing-lib/doc/simplest-transform-tutorial.md) and associated 
[noop implementation](transforms/universal/noop). 

### Run a data pipeline on local-ray
Get started by building a data pipeline with our example pipeline (link to be added) that can run on a laptop. 

### Build your own sequence of transforms
Follow the documentation [here](doc/overview.md) to build your own pipelines. 

### Automate the pipeline
The data preprocessing can be automated by running transformers as a KubeFlow pipeline (KFP). 
See a simple transform pipeline [tutorial](kfp/doc/simple_transform_pipeline.md). Next releases of Data Prep LAB will 
demonstrate how several simple transform pipelines can be combined into a single KFP pipeline. Future releases of 
Data Prep LAB will demonstrate how multiple simple transform pipelines can be combined into a single KFP pipeline.

The project facilitates the creation of a local Kind cluster with all the required software and test data. 
To work with the Kind cluster and KFP, you need to install several pre-required software packages. Please refer to 
[Kind preinstalled software](./kind/README.md#preinstalled-software) for more details

When you have all packages installed, you can execute 

```bash
make setup
```
from this main package directory or from the `kind` directory.

Wnen you finish working with the cluster, you can destroy it by 
```bash
make clean
```

### How to navigate and use  the repository
See documentation on [repository structure and its use](doc/repo.md) 

## &#x1F91D; How to contribute <a name = "contribute_steps"></a>
See [contribution guide](CONTRIBUTING.md)


## &#x2B50; Acknowledgements <a name = "acknowledgement"></a>
Thanks to [BigCode Project](https://github.com/bigcode-project) that has been used to build the code quality module. 






