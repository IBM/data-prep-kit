# Set REPOROOÞ
REPO_ROOT=$(cd ../../../ && pwd && cd - > /dev/null )
echo $REPO_ROOT


# Set PYTHONPATH for `data_processing_ray` library
export PYTHONPATH=$REPO_ROOT/data-processing-lib/python/src
export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/data-processing-lib/ray/src:$REPO_ROOT/data-processing-lib/python/src

# Set PYTHONAPATH for transforms
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/language/pdf2parquet/ray/src:$REPO_ROOT/transforms/language/pdf2parquet/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/language/doc_json_chunk/ray/src:$REPO_ROOT/transforms/language/doc_json_chunk/python/src
# # export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/language/text_encoder/ray/src:$REPO_ROOT/transforms/language/text_encoder/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/language/lang_id/ray/src:$REPO_ROOT/transforms/language/lang_id/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/language/doc_quality/ray/src:$REPO_ROOT/transforms/language/doc_quality/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/ededup/ray/src:$REPO_ROOT/transforms/universal/ededup/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/fdedup/ray/src:$REPO_ROOT/transforms/universal/fdedup/python/src
# export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/filter/ray/src:$REPO_ROOT/transforms/universal/filter/python/src
# # export PYTHONPATH=$PYTHONPATH:$REPO_ROOT/transforms/universal/doc_id/ray/src:$REPO_ROOT/transforms/universal/doc_id/python/src

. ./venv/bin/activate
jupyter notebook --no-browser demo_with_launcher.ipynb
