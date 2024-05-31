# Set REPOROOÞ
REPO_ROOT=$(cd ../ && pwd && cd - > /dev/null )
echo $REPO_ROOT


# Set PYTHONPATH for `data_processing_ray` library
export PYTHONPATH=$REPO_ROOT/data-processing-lib/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/data-processing-lib/ray/src

# Set PYTHONAPATH for transforms
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/malware/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/code_quality/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/proglang_select/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/ededup/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/fdedup/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/filter/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/doc_id/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/tokenization/ray/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/tools/ingest2parquet/src/

. ./venv/bin/activate
jupyter notebook demo_with_launcher.ipynb
