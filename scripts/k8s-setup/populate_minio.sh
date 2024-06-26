#!/usr/bin/env bash

if [ "$MINIO_SERVER" == "" ]; then
    MINIO_SERVER="http://localhost:8090"
fi

if [ "$MINIO_ACCESS_KEY" == "" ]; then
    MINIO_ACCESS_KEY="minio"
fi

if [ "$MINIO_SECRET_KEY" == "" ]; then
    MINIO_SECRET_KEY="minio123"
fi

echo "creating minio alias to $MINIO_SERVER"
mc alias set kfp $MINIO_SERVER $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

echo "creating test bucket"
mc mb kfp/test
echo "copying data"
# code modules
mc cp --recursive ${REPOROOT}/transforms/code/code_quality/ray/test-data/input/ kfp/test/code_quality/input
mc cp --recursive ${REPOROOT}/transforms/code/code2parquet/ray/test-data/input/data-processing-lib.zip kfp/test/code2parquet/input
mc cp --recursive ${REPOROOT}/transforms/code/code2parquet/ray/test-data/languages/ kfp/test/code2parquet/languages
mc cp --recursive ${REPOROOT}/transforms/code/proglang_select/ray/test-data/input/ kfp/test/proglang_select/input
mc cp --recursive ${REPOROOT}/transforms/code/proglang_select/ray/test-data/languages/ kfp/test/proglang_select/languages
mc cp --recursive ${REPOROOT}/transforms/code/malware/ray/test-data/input/ kfp/test/malware/input
# language
mc cp --recursive ${REPOROOT}/transforms/language/lang_id/ray/test-data/input/ kfp/test/lang_id/input
# universal
mc cp --recursive ${REPOROOT}/transforms/universal/doc_id/ray/test-data/input/ kfp/test/doc_id/input
mc cp --recursive ${REPOROOT}/transforms/universal/ededup/ray/test-data/input/ kfp/test/ededup/input
mc cp --recursive ${REPOROOT}/transforms/universal/fdedup/ray/test-data/input/ kfp/test/fdedup/input
mc cp --recursive ${REPOROOT}/transforms/universal/filter/ray/test-data/input/ kfp/test/filter/input
mc cp --recursive ${REPOROOT}/transforms/universal/noop/ray/test-data/input/ kfp/test/noop/input
mc cp --recursive ${REPOROOT}/transforms/universal/tokenization/ray/test-data/ds01/input/ kfp/test/tokenization/ds01/input
mc cp --recursive ${REPOROOT}/transforms/universal/profiler/ray/test-data/input/ kfp/test/profiler/input

