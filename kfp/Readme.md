Map betweens transforms and KFP pipelines

| Transform                           |                                    KFP pipeline                                    |          
|-------------------------------------|:----------------------------------------------------------------------------------:|
| code/malware                        |                  [malware_wf.py](../transforms/code/malware/ray/kfp/v1/malware_wf.py)                   |
| code/code_quality                   |            [code_quality_wf.py](../transforms/code/code_quality/ray/kfp/v1/code_quality_wf.py)            |
| code/programming language_annotator | [proglang_select_wf.py](../transforms/code/proglang_select/ray/kfp/v1/proglang_select_wf.py) |
| universal/doc_id                    |                  [doc_id_wf.py](../transforms/universal/doc_id/ray/kfp/v1/doc_id_wf.py)                   |
| universal/ededup                    |                  [ededup_wf.py](../transforms/universal/ededup/ray/kfp/v1/ededup_wf.py)                   |
| universal/fdedup                    |                  [fdedup_wf.py](../transforms/universal/fdedup/ray/kfp/v1/fdedup_wf.py)                   |
| universal/filtering                 |              [filter_wf.py](../transforms/universal/filter/ray/kfp/v1/filter_wf.py)              |
| universal/noop                      |                     [noop_wf.py](../transforms/universal/noop/ray/kfp/v1/noop_wf.py)                      |
| universal/tokenization              |         [tokenization_wf.py](../transforms/universal/tokenization/ray/kfp/v1/tokenization_wf.py)          |


For more information you can find [here](./doc/simple_transform_pipeline.md) a toturial that shows how to build, compile, and execute a KFP pipeline for a simple transfotm.
