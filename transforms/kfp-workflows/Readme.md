Map betweens transforms and KFP pipelines

| Transform                           |                                    KFP pipeline                                    |          
|-------------------------------------|:----------------------------------------------------------------------------------:|
| code/malware                        |                  [antivirus_wf.py](../code/malware/ray/kfp-workflow/malware_wf.py)                   |
| code/code_quality                   |            [code_quality_wf.py](../code/code_quality/ray/kfp-workflow/code_quality_wf.py)            |
| code/programming language_annotator | [proglang_select_wf.py](../code/proglang_select/ray/kfp-workflow/proglang_select_wf.py) |
| universal/doc_id                    |                  [doc_id_wf.py](../universal/doc_id/ray/kfp-workflow/doc_id_wf.py)                   |
| universal/ededup                    |                  [ededup_wf.py](../universal/ededup/ray/kfp-workflow/ededup_wf.py)                   |
| universal/fdedup                    |                  [fdedup_wf.py](../universal/fdedup/ray/kfp-workflow/fdedup_wf.py)                   |
| universal/filtering                 |              [filter_wf.py](../universal/filter/ray/kfp-workflow/filter_wf.py)              |
| universal/noop                      |                     [noop_wf.py](../universal/noop/ray/kfp-workflow/noop_wf.py)                      |
| universal/tokenization              |         [tokenization_wf.py](../universal/tokenization/ray/kfp-workflow/tokenization_wf.py)          |


For more information you can find [here](../../kfp/doc/simple_transform_pipeline.md) a toturial that shows how to build, compile, and execute a KFP pipeline for a simple transfotm.
