# HAP Annotation
Please see the set of [transform project conventions](https://github.com/ian-cho/data-prep-kit/blob/dev/transforms/README.md) for details on general project conventions, transform configuration, testing and IDE set up.

## Prerequisite
This repo needs NLTK and please refer to `requirements.txt`.

## Summary
The hap transform maps a non-empty input table to an output table with an added `hap_score` column. Each row in the table represents a document, and the hap transform performs the following three steps to calculate the hap score for each document:

* Sentence spliting: we use NLTK to split the document into sentence pieces.
* Hap annotation: each sentence is assigned a hap score between 0 and 1, where 1 represents hap and 0 represents non-hap.
* Aggregation: the document hap score is determined by selecting the maximum hap score among its sentences.


## Configuration and command line Options
The set of dictionary keys holding [HAPTransformConfiguration](src/hap_transform.py) 
configuration for values are as follows:

* --model_name_or_path - specifies HAP model which should be compatable with HuggingFace's `AutoModelForSequenceClassification` 
* --batch_size - modify it based on the infrastructure capacity.
* --max_length - the maximum length for the tokenizer.



## input format
The input is in .parquet format and contains the following columns:

| doc_id  |   doc_text | 
|:------|:------|
| 1  |    GSC is very much a little Swiss Army knife for...   |
| 2  |    Here are only a few examples. And no, I'm not ...   |

## output format
The output is in .parquet format and includes an additional column, in addition to those in the input:

| doc_id  |   doc_text | hap_score   |
|:------|:------|:-------------|
| 1  |    GSC is very much a little Swiss Army knife for... | 0.002463     |
| 2  |    Here are only a few examples. And no, I'm not ... | 0.989713     |

## How to run
Place your input Parquet file in the `test-data/input/` directory. A sample file, `test1.parquet`, is available in this directory. Once done, run the script.

```python
python hap_local_python.py
```

You will obtain the output file `test1.parquet` in the output directory.






