<p align="center">
<img src="doc/LF_DataPrepKit_Logotype_Positive.png#gh-light-mode-only" width="50%" />
<img src="doc/LF_DataPrepKit_Logotype_Negative.png#gh-dark-mode-only" width="50%" />
</p>

<div align="center">

[![arXiv](https://img.shields.io/badge/arXiv-2409.18164-b31b1b.svg)](https://arxiv.org/abs/2409.18164)
[![Docs](https://img.shields.io/badge/docs-live-brightgreen)](https://data-prep-kit.github.io/data-prep-kit/)
[![PyPI version](https://img.shields.io/pypi/v/data-prep-toolkit-transforms)](https://pypi.org/project/data-prep-toolkit-transforms/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Apache 2.0](https://img.shields.io/github/license/data-prep-kit/data-prep-kit)](https://opensource.org/license/apache-2-0)
[![GitHub Issues](https://img.shields.io/github/issues/data-prep-kit/data-prep-kit.svg)](https://github.com/data-prep-kit/data-prep-kit/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/data-prep-kit/data-prep-kit.svg)](https://github.com/data-prep-kit/data-prep-kit/pulls)
  [![LF AI & Data](https://img.shields.io/badge/LF%20AI%20%26%20Data-003778?logo=linuxfoundation&logoColor=fff&color=0094ff&labelColor=003778)](https://lfaidata.foundation/projects/)
  [![OpenSSF Best Practices](https://www.bestpractices.dev/projects/10250/badge)](https://www.bestpractices.dev/projects/10250)

  </div>

Data-Prep-Kit accelerates unstructured data preparation for LLM app developers. Developers can use Data-Prep-Kit to cleanse, transform, and enrich use case-specific unstructured data to pre-train LLMs, fine-tune LLMs, instruct-tune LLMs, or build [Retrieval Augmented Generation (RAG)](https://github.com/data-prep-kit/data-prep-kit/blob/dev/examples/rag-html-1/README.md) applications for LLMs

Data-Prep-Kit can readily scale from a commodity laptop all the way to data center scale.


## Features <a name = "features"></a>

- The kit provides a growing set of [modules/transforms](#table) targeting laptop-scale to datacenter-scale processing.
- The data modalities supported _today_ are: Natural Language, Code, and Image. 
- The modules are built on common frameworks for Python and Ray runtimes for scaling up data processing.
- The kit provides a framework for developing custom transforms for processing Parquet files as well as ZIP, NDJSON, and JSONL file formats. 
- The kit provides examples of how a single transform can be deployed on Kubernetes clusters as a Python or a Ray job. Additionally, when multiple transforms are deployed in a sequence, the kit uses [Tekton](https://tekton.dev/) pipelines.


## Installation

The latest version of the Data-Prep-Kit is available on PyPi for Python 3.10, 3.11, 3.12, and 3.13. It can be installed using: 

```bash
pip install uv
uv pip install 'data-prep-toolkit-transforms[all]'
```

This will install all available transforms. 

For guidance on creating the virtual environment for installing the data prep kit, click [here](doc/quick-start/quick-start.md).

## &#x1F680; Getting Started <a name = "gettingstarted"></a>

### Fastest way to experience Data-Prep-Kit

With no setup necessary, let's use a Google Colab friendly notebook to try Data-Prep-Kit. This is a simple transform to extract content from PDF files: [examples/notebooks/Run_your_first_transform_colab.ipynb](examples/notebooks/Run_your_first_transform_colab.ipynb)  | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/data-prep-kit/data-prep-kit/blob/dev/examples/notebooks/Run_your_first_transform_colab.ipynb). ([Here](doc/google-colab.md) are some tips for running Data-Prep-Kit transforms on Google Colab. For this simple example, these tips are either already taken care of, or are not needed.)  The same notebook can be downloaded and run on the local machine, without cloning the repo or any other setup. 

### Examples

Now that you have run a single transform, the next step is to explore how to put these transforms 
together to run a data prep pipeline for end to end real enterprise use cases like fine-tuning a model or building a RAG application. 

We have a complete set of data processing [recipes](examples) for such use cases. 

We also have [a developer tutorial](doc/quick-start/contribute-your-own-transform.md) for contributing a new transform to the kit. 

For advanced users, [here](ADVANCED.md) is more information for adding your own transform, 
running transforms from the command line, scaling and automation and more. 
Also, repository structure and use are discussed [here](doc/repo.md).

### Using HuggingFace data files 

All the transforms in the kit include small sample data files for testing, but advanced users who want to download real data files from HuggingFace and use them in testing, can refer to [this](ADVANCED.md#using-data-from-huggingface). 


## Supported data transforms <a name="table"></a>


The matrix below shows the the combination of modules and supported runtimes. All the modules can be accessed [here](transforms) and can be combined to form data processing pipelines, as shown in the [examples](examples) folder. 

| Modules                                                                              |    Python-only     |        Ray         |     
|:-------------------------------------------------------------------------------------|:------------------:|:------------------:|
| **Data Ingestion**                                                                   |                    |                    |                    |                    |
| [Code (from zip) to Parquet](transforms/code/code2parquet/README.md) | :white_check_mark: | :white_check_mark: |
| [Docling to Parquet](transforms/language/docling2parquet/README.md)                 | :white_check_mark: | :white_check_mark: |
| [HTML to Parquet](transforms/language/html2parquet/README.md)               | :white_check_mark: | :white_check_mark: |
| [Web to Parquet](transforms/universal/web2parquet/README.md)                | :white_check_mark: |                    |       
| **Universal (Code & Language)**                                                      |                    |                    |
| [Exact dedup filter](transforms/universal/ededup/README.md)                      | :white_check_mark: | :white_check_mark: |
| [Fuzzy dedup filter](transforms/universal/fdedup/README.md)                      | :white_check_mark: | :white_check_mark: | 
| [Unique ID annotation](transforms/universal/doc_id/README.md)                    | :white_check_mark: | :white_check_mark: | 
| [Filter on annotations](transforms/universal/filter/README.md)                   | :white_check_mark: | :white_check_mark: | 
| [Profiler](transforms/universal/profiler/README.md)                       | :white_check_mark: | :white_check_mark: |
| [Resize](transforms/universal/resize/README.md)                           | :white_check_mark: | :white_check_mark: |
| [Hate, Abuse, Profanity (HAP)](transforms/universal/hap/README.md)               | :white_check_mark: | :white_check_mark: |
| [Tokenizer](transforms/universal/tokenization/README.md)                         | :white_check_mark: | :white_check_mark: |
| [Tokenization2Arrow](transforms/universal/tokenization/README-tkn2arrow.md)                         | :white_check_mark: | :white_check_mark: | 
| [Repetition removal](transforms/universal/rep_removal/README.md)                         | :white_check_mark: | :white_check_mark: |
| [Bloom filter](transforms/universal/bloom/README.md)                         | :white_check_mark: |  |
| [Collapse(column concatenation)](transforms/universal/collapse/README.md)                         | :white_check_mark: | :white_check_mark: |
| [Blocklist](transforms/universal/blocklist/README.md)                         | :white_check_mark: |  :white_check_mark: |
| [C4 annotator](transforms/universal/c4_annotator/README.md)                         | :white_check_mark: |  :white_check_mark: |
| [Fineweb quality annotator](transforms/universal/fineweb_quality_annotator/README.md)                         | :white_check_mark: |  :white_check_mark: |
| [Gopher repetition annotator](transforms/universal/gopher_repetition_annotator/README.md)                         | :white_check_mark: |  :white_check_mark: |
| [Opensearch](transforms/universal/opensearch/README.md)                         | :white_check_mark: |  :white_check_mark: |
| [Folder2Parquet](transforms/universal/folder2parquet/README.md)                         | :white_check_mark: |  |
**Language-only**                                                                    |                    |                    |                    |                    |
| [Language identification](transforms/language/lang_id/README.md)              | :white_check_mark: | :white_check_mark: |
| [Document quality](transforms/language/doc_quality/README.md)                 | :white_check_mark: | :white_check_mark: |
| [Document chunking for RAG](transforms/language/doc_chunk/README.md)          | :white_check_mark: | :white_check_mark: |
| [Text encoder](transforms/language/text_encoder/README.md)                    | :white_check_mark: | :white_check_mark: |
| [PII Annotator/Redactor](transforms/language/pii_redactor/README.md)          | :white_check_mark: | :white_check_mark: |
| [Similarity](transforms/language/similarity/README.md)                        | :white_check_mark: |                    |
| [GneissWeb classification](transforms/language/gneissweb_classification/README.md)          | :white_check_mark: | :white_check_mark: |
| [Readability scores](transforms/language/readability/README.md)          | :white_check_mark: | :white_check_mark: |
| [Extreme tokenized annotation](transforms/language/extreme_tokenized/README.md)          | :white_check_mark: | :white_check_mark: |
| [ML Filter](transforms/language/ml_filter/README.md)                         | :white_check_mark: | :white_check_mark: |
| [ML Enrichment(quality annotation)](transforms/language/enrichment/README.md)                         | :white_check_mark: | :white_check_mark: |
**Code-only**                                                                         |                    |                     |             |                    |
| [Programming language annotation](transforms/code/proglang_select/README.md)  | :white_check_mark: | :white_check_mark: |
| [Code quality annotation](transforms/code/code_quality/README.md)             | :white_check_mark: | :white_check_mark: |
| [Malware annotation](transforms/code/malware/python/README.md)                       | :white_check_mark: | :white_check_mark: |
| [Header cleanser](transforms/code/header_cleanser/python/README.md)                  | :white_check_mark: | :white_check_mark: |
| [Semantic file ordering](transforms/code/repo_level_ordering/ray/README.md)          |                    | :white_check_mark: |
| [License Select Annotation](transforms/code/license_select/README.md)         | :white_check_mark: | :white_check_mark: |
| [Code profiler](transforms/code/code_profiler/README.md)                             | :white_check_mark: | :white_check_mark: |
**Images**                                                                         |                    |                     |             |                    |
| [Faces](transforms/images/README.md)  | :white_check_mark: | :white_check_mark: |
| [NSFW(Not Safe For Work)](transforms/images/README.md)  | :white_check_mark: | :white_check_mark: |
| [People](transforms/images/README.md)  | :white_check_mark: | :white_check_mark: |
</details>

## Logging configuration
DPK uses a unified logger - `dpk`. It can be configured, by setting the following environment variables

| Variable name        | Default value | Description                                                                                                                            |
|----------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------|
| DPK_LOG_LEVEL        | INFO      | The loggger level                                                                                                                      |
| DPK_LOG_FILE         | None      | The path to the log file, if set the log message will be stored in the file                                                            |
| DPK_LOG_JSON_HANDLER | ""        | If set to any value of "true", "1", "yes", or "on" (case insensitive) the console logs will be in JSON format                          |
| DPK_LOG_PROPAGATION  | "" | If set to any value of "true", "1", "yes", or "on" (case insensitive), the logger will propagate all log messages to its parent logger |


## Contributing

Contributors are welcome to add new modules to expand to other data modalities as well as add runtime support for existing modules! Please read [this](CONTRIBUTING.md) for details.

## Get help and support

Please feel free to connect with us using the [discussion](https://github.com/data-prep-kit/data-prep-kit/discussions) section.

## MAINTAINERS

For a list of current maintainers, please [see](MAINTAINERS.md).

## CHANGELOG 

For the history of releases and changes, please [see](release-notes.md).

## Resources

[Papers, talks, presentations and tutorials](resources.md)

[Granite open source LLM models](https://huggingface.co/ibm-granite) 

[GneissWeb](https://research.ibm.com/blog/gneissweb-for-granite-training) 

## Citation <a name = "citations"></a>

If you use Data-Prep-Kit in your research, please cite our paper:

```bash
@misc{wood2024dataprepkitgettingdataready,
      title={Data-Prep-Kit: getting your data ready for LLM application development}, 
      author={David Wood and Boris Lublinsky and Alexy Roytman and Shivdeep Singh 
      and Constantin Adam and Abdulhamid Adebayo and Sungeun An and Yuan Chi Chang 
      and Xuan-Hong Dang and Nirmit Desai and Michele Dolfi and Hajar Emami-Gohari 
      and Revital Eres and Takuya Goto and Dhiraj Joshi and Yan Koyfman 
      and Mohammad Nassar and Hima Patel and Paramesvaran Selvam and Yousaf Shah  
      and Saptha Surendran and Daiki Tsuzuku and Petros Zerfos and Shahrokh Daijavad},
      year={2024},
      eprint={2409.18164},
      archivePrefix={arXiv},
      primaryClass={cs.AI},
      url={https://arxiv.org/abs/2409.18164}, 
}
```
## License

All source files must include a Copyright and License header. If you would like to see the detailed LICENSE click [here](LICENSE).

## LF AI & Data

Data-Prep-Kit is hosted as a project in the [LF AI & Data Foundation](https://lfaidata.foundation/projects/).

### IBM ❤️ Open Source AI

The project was started by the Data for AI Models team at IBM Research. 

Copyright © Data-Prep-Kit Framework - a Series of LF Projects, LLC.
