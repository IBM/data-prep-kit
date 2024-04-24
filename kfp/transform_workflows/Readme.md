Map betweens transforms and KFP pipelines

| Transform                           |                                    KFP pipeline                                    |          
|-------------------------------------|:----------------------------------------------------------------------------------:|
| code/malware                        |                  [antivirus_wf.py](./code/malware/malware_wf.py)                   |
| code/code_quality                   |            [code_quality_wf.py](./code/code_quality/code_quality_wf.py)            |
| code/programming language_annotator | [programming language_annotator_wf.py](./code/proglang_match/proglang_match_wf.py) |
| universal/doc_id                    |                  [doc_id_wf.py](./universal/doc_id/doc_id_wf.py)                   |
| universal/ededup                    |                  [ededup_wf.py](./universal/ededup/ededup_wf.py)                   |
| universal/fdedup                    |                  [fdedup_wf.py](./universal/fdedup/fdedup_wf.py)                   |
| universal/filtering                 |              [filtering_wf.py](./universal/filtering/filtering_wf.py)              |
| universal/noop                      |                     [noop_wf.py](./universal/noop/noop_wf.py)                      |
| universal/tokenization              |         [tokenization_wf.py](./universal/tokenization/tokenization_wf.py)          |


For more information you can find [here](../doc/simple_transform_pipeline.md) a toturial that shows how to build, compile, and execute a KFP pipeline for a simple transfotm.
