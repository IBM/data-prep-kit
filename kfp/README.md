# Automation with Kubeflow Pipelines 

## Map betweens transforms and KFP pipelines

| Transform                           |                                    KFP pipeline                                    |          
|-------------------------------------|:----------------------------------------------------------------------------------:|
| language/lang_id                  |                  [lang_id_wf.py](../transforms/language/lang_id/kfp_ray/lang_id_wf.py)                   |
| code/malware                        |                  [malware_wf.py](../transforms/code/malware/kfp_ray/malware_wf.py)                     |
| code/code2parquet                   |                  [code2parquet_wf.py](../transforms/code/code2parquet/kfp_ray/code2parquet_wf.py)                   |
| code/code_quality                   |            [code_quality_wf.py](../transforms/code/code_quality/kfp_ray/code_quality_wf.py)            |
| code/proglang_select                | [proglang_select_wf.py](../transforms/code/proglang_select/kfp_ray/proglang_select_wf.py)              |
| universal/doc_id                    |                  [doc_id_wf.py](../transforms/universal/doc_id/kfp_ray/doc_id_wf.py)                   |
| universal/ededup                    |                  [ededup_wf.py](../transforms/universal/ededup/kfp_ray/ededup_wf.py)                   |
| universal/fdedup                    |                  [fdedup_wf.py](../transforms/universal/fdedup/kfp_ray/fdedup_wf.py)                   |
| universal/filtering                 |              [filter_wf.py](../transforms/universal/filter/kfp_ray/filter_wf.py)                       |
| universal/noop                      |                     [noop_wf.py](../transforms/universal/noop/kfp_ray/noop_wf.py)                      |
| universal/profiler                  |                     [profiler_wf.py](../transforms/universal/profiler/kfp_ray/profiler_wf.py)          |
| universal/tokenization              |         [tokenization_wf.py](../transforms/universal/tokenization/kfp_ray/tokenization_wf.py)          |


## Set up and working steps

- [Set up a Kubernetes clusters for KFP execution](../kfp/doc/setup.md)
- [Simple Transform pipeline tutorial](../kfp/doc/simple_transform_pipeline.md)
- [Execution several transformers](../kfp/doc/multi_transform_pipeline.md)
- [Clean up the cluster](../kfp/doc/setup#cleanup)
