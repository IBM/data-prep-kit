Map betweens transforms and KFP pipelines

| Transform       | KFP pipeline           
| ------------- |:-------------:|
| code/antivirus  | [antivirus_wf.py](./code/antivirus/antivirus_wf.py)
| code/code_quality      | [code_quality_wf.py](./code/antivirus/code_quality_wf.py)
| code/language_annotator      | [language_filtering_wf.py](./code/language_filtering/language_filtering_wf.py)
| language/language_id  | [code/lang_id_wf.py](./language/language_id/lang_id_wf.py)
| universal/blocklisting  | [blocklisting_wf.py](./universal/blocklisting/blocklisting_wf.py)
| universal/ededup  | [ededup_wf.py](./universal/ededup/ededup_wf.py)
| universal/fdedup  | [fdedup_wf.py](./universal/fdedup/fdedup_wf.py)
| universal/filtering  | [filtering_wf.py](./universal/filtering/filtering_wf.py)
| universal/noop  | [noop_wf.py](./universal/noop/noop_wf.py)
| universal/tokenization  | [tokenization_wf.py](./universal/tokenization/tokenization_wf.py)


For more information you can find [here](../doc/simple_transform_pipeline.md) a toturial that shows how to build, compile, and execute a KFP pipeline for a simple transfotm.

In addition, you can try the automatic [KFP pipeline generator](./pipeline_generator/README.md).