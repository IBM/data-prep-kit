# Set REPOROOÞ
REPO_ROOT=$(cd ../../../ && pwd && cd - > /dev/null )
echo $REPO_ROOT


# Set PYTHONPATH for `data_processing_ray` library
export PYTHONPATH=$REPO_ROOT/data-processing-lib/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/data-processing-lib/ray/src:$REPO_ROOT/data-processing-lib/python/src

# Set PYTHONAPATH for transforms
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/malware/ray/src:$REPO_ROOT/transforms/code/malware/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/code_quality/ray/src:$REPO_ROOT/transforms/code/code_quality/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/code2parquet/ray/src:$REPO_ROOT/transforms/code/code2parquet/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/proglang_select/ray/src:$REPO_ROOT/transforms/code/proglang_select/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/ededup/ray/src:$REPO_ROOT/transforms/universal/ededup/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/fdedup/ray/src:$REPO_ROOT/transforms/universal/fdedup/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/filter/ray/src:$REPO_ROOT/transforms/universal/filter/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/doc_id/ray/src:$REPO_ROOT/transforms/universal/doc_id/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/tokenization/ray/src:$REPO_ROOT/transforms/universal/tokenization/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/tools/ingest2parquet/src/
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/ingest_2_parquet/ray/src:$REPO_ROOT/transforms/code/ingest_2_parquet/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/repo_level_ordering/ray/src/

. ./venv/bin/activate
jupyter notebook demo_with_launcher.ipynb
