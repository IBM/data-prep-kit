#!/usr/bin/env bash

echo "creating minio alias"
mc alias set kfp http://localhost:8080 minio minio123
echo "creating test bucket"
mc mb kfp/test
echo "copying data"
# code modules
mc cp --recursive ${ROOT_DIR}/../transforms/code/code_quality/test-data/input/ kfp/test/code_quality/input
mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/test-data/input/ kfp/test/proglang_select/input
mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/test-data/languages/ kfp/test/proglang_select/languages
mc cp --recursive ${ROOT_DIR}/../transforms/code/malware/test-data/input/ kfp/test/malware/input
# universal
mc cp --recursive ${ROOT_DIR}/../transforms/universal/doc_id/test-data/input/ kfp/test/doc_id/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/ededup/test-data/input/ kfp/test/ededup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/fdedup/test-data/input/ kfp/test/fdedup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/filter/test-data/input/ kfp/test/filter/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/noop/test-data/input/ kfp/test/noop/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/tokenization/test-data/ds01/input/ kfp/test/tokenization/ds01/input

