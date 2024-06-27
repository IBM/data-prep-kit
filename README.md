

<h1 align="center">Data Prep Kit</h1>

<div align="center"> 

<?  [![Status](https://img.shields.io/badge/status-active-success.svg)]() ?>
<?  [![GitHub Issues](https://img.shields.io/github/issues/kylelobo/The-Documentation-Compendium.svg)](https://github.com/IBM/data-prep-kit/issues) ?>
<?  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kylelobo/The-Documentation-Compendium.svg)](https://github.com/IBM/data-prep-kit/pulls) ?>
</div> 

---

Data Prep Kit is a community project to democratize and accelerate unstructured data preparation for LLM app developers. 
With the explosive growth of LLM-enabled use cases, developers are faced with the enormous challenge of preparing use case-specific unstructured data to fine-tune or instruct-tune the LLMs.
As the variety of use cases grows, so does the need to support:

- New modalities of data (code, language, speech, visual) 
- New ways of transforming the data to optimize the performance of the resulting LLMs for each specific use case.
- A large variety in the scale of data to be processed, from laptop-scale to datacenter-scale

Data Prep Kit offers implementations of commonly needed data transformations, called *modules*, for both Code and Language modalities.
The goal is to offer high-level APIs for developers to quickly get started in working with their data, without needing expertise in the underlying runtimes and frameworks.

## 📝 Table of Contents
- [About](#about)
- [Getting Started](#getting_started)
- [How to Contribute](#contribute_steps)
- [Acknowledgments](#acknowledgement)

## &#x1F4D6; About <a name = "about"></a>
Data Prep Kit is a toolkit for streamlining data preparation for developers looking to build LLM-enabled applications via fine-tuning or instruction-tuning.
Data Prep Kit contributes a set of modules that the developer can get started with to easily build data pipelines suitable for their use case.
These modules have been tested while producing pre-training datasets for the Granite open models, [here](https://huggingface.co/instructlab/granite-7b-lab) and [here](https://huggingface.co/ibm-granite). 

The modules are built on common frameworks (for Spark and Ray), called the *data processing library* that allows the developers to build new custom modules that readily scale across a variety of runtimes.
Eventually, Data Prep Kit will offer consistent APIs and configurations across the following underlying runtimes.

1. Python runtime
2. Ray runtime (local and distributed)
3. Spark runtime (local and distributed)
4. [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/) (local and distributed, wrapping Ray)

The current matrix for the combination of modules and supported runtimes is shown in the table below. 
Contributors are welcome to add new modules as well as add runtime support for existing modules!


|Modules                         | Python-only      | Ray              | Spark            | KFP on Ray             |
|------------------------------  |------------------|------------------|------------------|------------------------|
|No-op / template                |:white_check_mark:|:white_check_mark:|:white_check_mark:|:white_check_mark:      |
|Doc ID annotation               |:white_check_mark:|:white_check_mark:|:white_check_mark:|:white_check_mark:      |
|Programming language annnotation|:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      | 
|Exact dedup filter              |                  |:white_check_mark:|                  |:white_check_mark:      |
|Fuzzy dedup filter              |                  |:white_check_mark:|                  |:white_check_mark:      |
|Code quality annotation         |:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      |
|Malware annotation              |:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      |
|Filter on annotations           |:white_check_mark:|:white_check_mark:|:white_check_mark:|:white_check_mark:      |
|Language identification         |:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      |
|Code (from zip) to Parquet      |:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      |
|Profiler                        |                  |:white_check_mark:|                  |:white_check_mark:      |
|Tokenizer                       |:white_check_mark:|:white_check_mark:|                  |:white_check_mark:      |



Features of the toolkit: 

- It aims to accelerate unstructured data prep for the "long tail" of LLM use cases.
- It offers a growing set of module implementations across multiple runtimes, targeting laptop-scale to datacenter-scale processing.
- It provides a growing set of sample pipelines developed for real enterprise use cases.
- It provides the [Data processing library](data-processing-lib/ray) to enable contribution of new custom modules targeting new use cases.
- It uses [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)-based [workflow automation](kfp/doc/simple_transform_pipeline.md).

Data modalities supported: 

* Code - support for code datasets as downloaded .zip files of GitHub repositories converted to
[parquet](https://arrow.apache.org/docs/python/parquet.html) files. 
* Language - Future releases will provide transforms specific to natural language, and like the code transformations, will operate on parquet files.

Support for additional data modalities is expected in the future.

### Data Processing Library 
A Python-based library that has ready-to-use transforms that can be supported across a variety of runtimes.
We use the popular [parquet](https://arrow.apache.org/docs/python/parquet.html) format to store the data (code or language). 
Every parquet file follows a set 
[schema](tools/ingest2parquet/).
Data is converted from raw form (e.g., zip files for GitHub repositories) to parquet files by the
[code2parquet](/transforms/code/code2parquet) 
tool that also adds the necessary fields in the schema.  
A user can then use one or more of the [available transforms](transforms) to process their data. 

#### Transform Design
A transform can follow one of the two patterns: annotator or filter.

- **Annotator** An annotator transform adds information during the processing by adding one more columns to the parquet files.
The annotator design also allows a user to verify the results of the processing before the actual filtering of the data.

- **Filter** A filter transform processes the data and outputs the transformed data, e.g., exact deduplication.
A general purpose [SQL-based filter transform](transforms/universal/filter) enables a powerful mechanism for identifying columns and rows of interest for downstream processing.
For a new module to be added, a user can pick the right design based on the processing to be applied. More details [here](transforms). 

#### Scaling of Transforms
To enable processing of large data volumes leveraging multi-mode clusters, [Ray](https://docs.ray.io/en/latest/index.html) 
or [Spark](https://spark.apache.org) wrappers are provided, to readily scale out the Python implementations.
A generalized workflow is shown [here](doc/data-processing.md).

#### Bring Your Own Transform 
One can add new transforms by bringing in Python-based processing logic and using the Data Processing Library to build and contribute transforms.
More details on the data processing library are [here](data-processing-lib/doc/overview.md). 

#### Automation
The toolkit also supports transform execution automation based on 
[Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/) (KFP),
tested on a locally deployed [Kind cluster](https://kind.sigs.k8s.io/) and external OpenShift clusters. There is an 
automation to create a Kind cluster and deploy all required components on it.
The KFP implementation is based on the [KubeRay Operator](https://docs.ray.io/en/master/cluster/kubernetes/getting-started.html)
for creating and managing the Ray cluster and [KubeRay API server](https://github.com/ray-project/kuberay/tree/master/apiserver)
to interact with the KubeRay operator. An additional [framework](kfp/kfp_support_lib) along with several
[kfp components](kfp/kfp_ray_components) is used to simplify the pipeline implementation.

## &#x1F680; Getting Started <a name = "getting_started"></a>

There are various entry points that you can choose based on the use case. Each entry point has its pre-requirements and setup steps.
The common part of are:
#### Prerequisites
- Python 3.10 or 3.11 
-Docker/Podman

Two important development tools will also be installed using the steps below:
- [pre-commit](https://pre-commit.com/)
- [twine](https://twine.readthedocs.io/en/stable/) 

#### Installation Steps
```shell
pip install pre-commit
pip install twine
...
git clone git@github.com:IBM/data-prep-kit.git
cd data-prep-kit
pre-commit install
```
Please note that there are further installation steps for running the transforms in general, as documented [here](data-processing-lib/ray/README.md) and on a local Kind cluster or on an existing Kubernetes cluster, as documented [here](kfp/doc/setup.md).

Below are a few demos to get you started.
### Build Your Own Transforms
Follow the documentation [here](data-processing-lib/doc/overview.md) to build your own transform
and run it in either the python  or Ray runtimes. 

### Run a Single Transform on Local Ray
Get started by running the "noop" transform that performs an identity operation by following the 
[tutorial](data-processing-lib/doc/simplest-transform-tutorial.md) and associated 
[noop implementation](transforms/universal/noop). 

### Run a Jupyter notebook on Local Ray cluster
Get started by building a Jupiter notebook executing a sequence of Transforms with our  [example pipeline](./examples/) 
that can run on your machine. This implementation can also be extended to connect to the remote Ray cluster.

### Automate a Pipeline
The data preprocessing can be automated by running transformers as a Kubeflow pipeline (KFP). 
The project facilitates the creation of a local [Kind cluster](https://kind.sigs.k8s.io/) with all the required 
software and test data, or deployment of required software on an existing cluster. 
See [Set up a Kubernetes clusters for KFP execution](kfp/doc/setup.md)

A simple transform pipeline [tutorial](kfp/doc/simple_transform_pipeline.md) explains the pipeline creation and execution. 
In addition, if you want to combine several transformers in a single pipeline, you can look at [multi-steps pipeline](kfp/doc/multi_transform_pipeline.md) 

When you finish working with the cluster, and want to clean up or destroy it. See the 
[clean up the cluster](../kfp/doc/setup.md#cleanup)

### How to Navigate and Use the Repository
See the documentation on [repository structure and its use](doc/repo.md). 

## &#x1F91D; How to Contribute <a name = "contribute_steps"></a>
See the [contribution guide](CONTRIBUTING.md)


## &#x2B50; Acknowledgements <a name = "acknowledgement"></a>
Thanks to the [BigCode Project](https://github.com/bigcode-project), which served as the source for borrowing the code quality metrics.





