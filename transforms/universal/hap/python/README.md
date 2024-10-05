# Hate, Abuse, and Profanity (HAP) Annotation
Please see the set of [transform project conventions](https://github.com/ian-cho/data-prep-kit/blob/dev/transforms/README.md) for details on general project conventions, transform configuration, testing and IDE set up.

## Prerequisite
This repository needs [NLTK](https://www.nltk.org/) and please refer to `requirements.txt`.

## Summary
The hap transform maps a non-empty input table to an output table with an added `hap_score` column. Each row in the table represents a document, and the hap transform performs the following three steps to calculate the hap score for each document:

* Sentence spliting: we use NLTK to split the document into sentence pieces.
* hap annotation: each sentence is assigned a hap score between 0 and 1, where 1 represents hap and 0 represents non-hap.
* Aggregation: the document hap score is determined by selecting the maximum hap score among its sentences.


## Configuration and command line Options
The set of dictionary keys holding [HAPTransformConfiguration](src/hap_transform.py) 
configuration for values are as follows:

* --model_name_or_path - specify the HAP model, which should be compatible with HuggingFace's AutoModelForSequenceClassification. Defaults to IBM's open-source toxicity classifier `ibm-granite/granite-guardian-hap-38m`.
* --batch_size - modify it based on the infrastructure capacity. Defaults to `128`.
* --max_length - the maximum length for the tokenizer. Defaults to `512`.
* --doc_text_column - the column name containing the document text in the input .parquet file. Defaults to `contents`.
* --annotation_column - the column name containing hap (toxicity) score in the output .parquet file. Defaults to `hap_score`.
  

## input format
The input is in .parquet format and contains the following columns:

| doc_id  | contents | 
|:------:|:------:|
| 1  |    GSC is very much a little Swiss Army knife for...   |
| 2  |    Here are only a few examples. And no, I'm not ...   |

## output format
The output is in .parquet format and includes an additional column, in addition to those in the input:

| doc_id  | contents | hap_score  |
|:------:|:------:|:-------------:|
| 1  |    GSC is very much a little Swiss Army knife for... | 0.002463     |
| 2  |    Here are only a few examples. And no, I'm not ... | 0.989713     |

## How to run
Place your input Parquet file in the `test-data/input/` directory. A sample file, `test1.parquet`, is available in this directory. Once done, run the script.

```python
python hap_local_python.py
```

You will obtain the output file `test1.parquet` in the output directory.

## Throughput 
The table below shows the throughput (tokens per second) of the HAP transform module, which primarily includes sentence splitting, HAP annotation, and HAP score aggregation. We herein compare two models:

* 4-layer lightweight toxicity classifier [ibm-granite/granite-guardian-hap-38m](https://huggingface.co/ibm-granite/granite-guardian-hap-38m)
* 12-layer toxicity classifier [ibm-granite/granite-guardian-hap-125m](https://huggingface.co/ibm-granite/granite-guardian-hap-125m)
 
We processed 6,000 documents (12 MB in Parquet file size) using the HAP transform module and reported the average CPU throughput over three trials.

| Model used in HAP transform module  | throughput (tokens per second) | 
|:------:|:------:|
| granite-guardian-hap-38m  |  6.16 k   |
| granite-guardian-hap-125m |  1.14 k   |





