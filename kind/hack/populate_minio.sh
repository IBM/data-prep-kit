#!/usr/bin/env bash

if [ "$MINIO_SERVER" == "" ]; then
    MINIO_SERVER="http://localhost:8080"
fi

echo "creating minio alias to $MINIO_SERVER"
mc alias set kfp $MINIO_SERVER minio minio123

echo "creating test bucket"
mc mb kfp/test
echo "copying data"
# code modules
mc cp --recursive ${ROOT_DIR}/../transforms/code/code_quality/ray/test-data/input/ kfp/test/code_quality/input
mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/ray/test-data/input/ kfp/test/proglang_select/input
mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/ray/test-data/languages/ kfp/test/proglang_select/languages
mc cp --recursive ${ROOT_DIR}/../transforms/code/malware/ray/test-data/input/ kfp/test/malware/input
# universal
mc cp --recursive ${ROOT_DIR}/../transforms/universal/doc_id/ray/test-data/input/ kfp/test/doc_id/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/ededup/ray/test-data/input/ kfp/test/ededup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/fdedup/ray/test-data/input/ kfp/test/fdedup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/filter/ray/test-data/input/ kfp/test/filter/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/noop/ray/test-data/input/ kfp/test/noop/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/tokenization/ray/test-data/ds01/input/ kfp/test/tokenization/ds01/input

