# Data Prep Kit Release notes

## Release 0.2.1 - 9/24/2024

### General 
1. Bug fixes across the repo
1. Added AI Alliance RAG demo, tutorials and notebooks and tips for running on google colab
1. Added new transforms and single package for transforms published to pypi
1. improved CI/CD with targeted workflow triggered on specific changes to specific modules
1. New enhancements for cutting a release


### data-prep-toolkit libraries (python, ray, spark) 

1. Restructure the repository to distinguish/separate runtime libraries
1. Split data-processing-lib/ray into python and ray
1. Spark runtime
1. updated pyarrow version
1. define required transform() method as abstract to AbstractTableTransform
1. Enables configuration of makefile to use src or pypi for data-prep-kit library dependencies 


### KFP Workloads 

1. Update kfp image version 
1. Enable kfp in GH action for testing randomly selected workflow and prevent kfp test for transforms that do not support it
1. Auto generate kfp pipelines
1. Combine the common KFP support code in a shared library
1. Update K8s cluster deployment and remove creation of clusterrolebinding in kubeflow installation


### Transforms

1. Added 7 new transdforms including: language identification, profiler, repo level ordering, doc quality, pdf2parquet, HTML2Parquet and PII Transform
1. Added ededup python implementation and incremental ededup 
1. Added fuzzy floating point comparison


## Release 0.2.0 - 6/27/2024

### General 
1. Many bug fixes across the repo, plus the following specifics.
1. Enhanced CI/CD and makefile improvements  include definition of top-level targets (clean, set-verions, build, publish, test)
1. Automation of release process branch/tag management
1. Documentation improvements 

### data-prep-toolkit libraries (python, ray, spark) 

1. Split libraries into 3 runtime-specific implementations
1. Fix missing final count of processed and add percentages
1. Improved fault tolerance in python and ray runtimes 
1. Report global DataAccess retry metric  
1. Support for binary data transforms
1. Updated to Ray version to 2.24
1. Updated to PyArrow version 16.1.0

### KFP Workloads 

1. Add KFP V2 support 
1. Create a distinct (timestamped) execution.log file for each retry
1. Support for multiple inputs/outputs

### Transforms

1. Added language/lang_id - detects language in documents
1. Added universal/profiler - counts works/tokens in documents
1. Converted ingest2parquet tool to transform named code2parquet
1. Split transforms, as appropriate, into python, ray and/or spark.
1. Added spark implementations of filter, doc_id and noop transforms.
1. Switch from using requirements.txt to pyproject.toml file for each transform runtime
1. Repository restructured to move kfp workflow definitions to associated transform project directory

## Release 0.1.1 - 5/24/2024

## Release 0.1.0 - 5/15/2024

## Release 0.1.0 - 5/08/2024

