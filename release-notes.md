# Data Prep Kit Release notes

## Release 0.2.0 - 6/28/2024

### data-prep-toolkit libraries (python, ray, spark) 

1. Split libraries into 3 runtime-specific implementations
1. Fix missing final count of processed and add percentages
1. Report Global Retry Metric 
1. Support for binary data transforms
1. Updated to Ray version to 2.24

### data-prep-toolkit-kfp libraries 

1. KFPv2 support with workarounds
1. Create a distinct (timestamped) execution.log file for each retry
1. Support for multiple inputs/outputs

### Transforms

1. Added language/lang_id - detects language in documents
1. Added universal/profiler - counts works/tokens in documents
1. Split transforms, as appropriate, into python, ray and/or spark.

## Release 0.1.1 - 5/24/2024

## Release 0.1.0 - 5/15/2024

## Release 0.1.0 - 5/08/2024

