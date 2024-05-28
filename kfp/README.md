# Automation with Kubeflow Pipelines 

## Map betweens transforms and KFP pipelines

| Transform                           |                                    KFP pipeline                                    |          
|-------------------------------------|:----------------------------------------------------------------------------------:|
| code/malware                        |                  [malware_wf.py](../transforms/code/malware/kfp_ray/v1/malware_wf.py)                   |
| code/code_quality                   |            [code_quality_wf.py](../transforms/code/code_quality/kfp_ray/v1/code_quality_wf.py)            |
| code/programming language_annotator | [proglang_select_wf.py](../transforms/code/proglang_select/kfp_ray/v1/proglang_select_wf.py) |
| universal/doc_id                    |                  [doc_id_wf.py](../transforms/universal/doc_id/kfp_ray/v1/doc_id_wf.py)                   |
| universal/ededup                    |                  [ededup_wf.py](../transforms/universal/ededup/kfp_ray/v1/ededup_wf.py)                   |
| universal/fdedup                    |                  [fdedup_wf.py](../transforms/universal/fdedup/kfp_ray/v1/fdedup_wf.py)                   |
| universal/filtering                 |              [filter_wf.py](../transforms/universal/filter/kfp_ray/v1/filter_wf.py)              |
| universal/noop                      |                     [noop_wf.py](../transforms/universal/noop/kfp_ray/v1/noop_wf.py)                      |
| universal/tokenization              |         [tokenization_wf.py](../transforms/universal/tokenization/kfp_ray/v1/tokenization_wf.py)          |


## Set up and working steps

- [Set up a Kubernetes clusters for KFP execution](./doc/setup.md)
- [Simple Transform pipeline tutorial](./doc/simple_transform_pipeline.md)
- [Execution several transformers](./doc/multi_transform_pipeline.md)
- [Clean up the cluster](./doc/setup#cleanup)