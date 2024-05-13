# Set REPOROOÞ
REPO_ROOT=$(cd ../ && pwd && cd - > /dev/null )
echo $REPO_ROOT


# Set PYTHONPATH for `data_processing` library
export PYTHONPATH=$REPO_ROOT/data-processing-lib/src/

# Set PYTHONAPATH for transforms
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/malware/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/code_quality/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/code/proglang_select/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/ededup/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/fdedup/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/filter/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/doc_id/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/tokenization/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/tools/ingest2parquet/src/

. ./venv/bin/activate
jupyter notebook demo_with_launcher.ipynb
