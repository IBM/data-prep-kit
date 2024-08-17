

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

Data Prep Kit offers implementations of commonly needed data preparation steps, called *modules* or *transforms*, for both Code and Language modalities.
The goal is to offer high-level APIs for developers to quickly get started in working with their data, without needing expertise in the underlying runtimes and frameworks.

## 📝 Table of Contents
- [About](#about)
- [Quick Start](doc/quick-start/quick-start.md)
- [Data Preparation Modules](#modules)
- [Data Processing Framework](#data-proc-lib)
- [Repository Use and Navigation](doc/repo.md)
- [How to Contribute](CONTRIBUTING.md)
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
4. Kubeflow Pipelines (local and distributed, wrapping Ray)

Features of the toolkit: 

- It aims to accelerate unstructured data prep for the "long tail" of LLM use cases.
- It offers a growing set of [module](transforms) implementations across multiple runtimes, targeting laptop-scale to datacenter-scale processing.
- It provides a growing set of [sample data processing pipelines](examples) that can be used for real enterprise use cases.
- It provides the [Data processing library](data-processing-lib/ray) to enable contribution of new custom modules targeting new use cases.
- It uses [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/v1/introduction/)-based [workflow automation](kfp/doc/simple_transform_pipeline.md).

Data modalities supported: 

* Code - support for code datasets as downloaded .zip files of GitHub repositories converted to
[parquet](https://arrow.apache.org/docs/python/parquet.html) files. 
* Language - supports for natural language datasets, and like the code transformations, will operate on parquet files.
* Universal - supports code and natrual langauge datasets, and can operate on with parquet files, zip archives, or individual HTML files.

Support for additional data modalities is expected in the future and additional data formats are welcome!

## Data Preparation Modules <a name = "modules"></a>
Matrix below shows the the combination of modules and supported runtimes. All the modules can be accessed [here](transforms) and can be combined to form data processing pipelines, as shown in the [examples](examples) folder. The modules are under three major categories: 1) Universal (apply to both code and language) 2) Language-only and 3) Code-only. We start with a set of modules for ingestion of various data formats.  


| Modules | Python-only | Ray | Spark | KFP on Ray |  
|----------------------------------|--------------------|------------------|------------------|------------------------|  
| **Universal (Code & Language)** | | | | |  
| [Exact dedup filter](/transforms/universal/ededup/ray/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Fuzzy dedup filter](/transforms/universal/fdedup/ray/README.md) | |:white_check_mark:| |:white_check_mark: |  
| [Unique ID annotation](/transforms/universal/doc_id/ray/README.md) | :white_check_mark: |:white_check_mark:|:white_check_mark:|:white_check_mark: |  
| [No-op / template](/transforms/universal/noop/python/README.md) | :white_check_mark: |:white_check_mark:|:white_check_mark:|:white_check_mark: |  
| [Filter on annotations](/transforms/universal/filter/python/README.md) | :white_check_mark: |:white_check_mark:|:white_check_mark:|:white_check_mark: |  
| [Profiler](/transforms/universal/profiler/ray/README.md) | |:white_check_mark:| |:white_check_mark: |  
| [Resize](/transforms/universal/resize/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Tokenizer](/transforms/universal/tokenization/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: | 
| **Language-only** | | | | |  
| [Language identification](/transforms/language/lang_id/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Document quality](/transforms/language/doc_quality/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Document chunking for RAG](/transforms/language/doc_chunk/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Text/Chunk encoder/embedding](/transforms/language/text_encoder/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [PII Annotator/Redactor](/transforms/universal/pii_redactor/python/README.md)| :white_check_mark:| :white_check_mark: | | :white_check_mark: |
| **Code-only** | | | | |  
| [Programming language annnotation](/transforms/code/proglang_select/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Code quality annotation](/transforms/code/code_quality/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Malware annotation](/transforms/code/malware/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Header cleanser](/transforms/code/header_cleanser/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [Semantic file ordering](/transforms/code/repo_level_ordering/ray/README.md) | |:white_check_mark:| | |  
| **Import/Export tables** | | | | |  
| [Code (from zip) to Parquet](/transforms/code/code2parquet/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |  
| [PDF to Parquet](/transforms/language/pdf2parquet/python/README.md) | :white_check_mark: |:white_check_mark:| |:white_check_mark: |



Contributors are welcome to add new modules as well as add runtime support for existing modules!


## Data Processing Framework <a name = "data-proc-lib"></a>
At the core of the framework, is a data processing library, that provides a systematic way to implement the data processing modules. The library is python-based and enables the application of "transforms" to a one or more input data files to produce one or more output data files. We use the popular [parquet](https://arrow.apache.org/docs/python/parquet.html) format to store the data (code or language). 
Every parquet file follows a set [schema](transforms/code/code2parquet/python/README.md). A user can use one or more transforms (or modules) as discussed above to process their data. 

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
One can add new transforms by bringing in Python-based processing logic and using the Data Processing Library to build and contribute transforms. We have provided an [example transform](transforms/universal/noop) that can serve as a template to add new simple transforms. 

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

A simple transform pipeline [tutorial](kfp/doc/simple_transform_pipeline.md) explains the pipeline creation and execution. 
In addition, if you want to combine several transformers in a single pipeline, you can look at [multi-steps pipeline](kfp/doc/multi_transform_pipeline.md) 

When you finish working with the cluster, and want to clean up or destroy it. See the 
[clean up the cluster](kfp/doc/setup.md#cleanup)

## Acknowledgements <a name = "acknowledgement"></a>
Thanks to the [BigCode Project](https://github.com/bigcode-project), which served as the source for borrowing few code quality metrics.





