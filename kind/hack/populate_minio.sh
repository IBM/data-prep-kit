#!/usr/bin/env bash

echo "creating minio alias"
mc alias set kfp http://localhost:8080 minio minio123
echo "creating test bucket"
mc mb kfp/test
echo "copying data"
# code modules
mc cp --recursive ${ROOT_DIR}/../transforms/code/code_quality/test-data/input/ kfp/test/code_quality/input

mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/test-data/input/ kfp/test/language_annotator/input
mc cp --recursive ${ROOT_DIR}/../transforms/code/proglang_select/test-data/languages/ kfp/test/lang_annotator/languages
mc cp --recursive ${ROOT_DIR}/../transforms/code/malware/test-data/input/ kfp/test/malware/input
# language
mc cp --recursive ${ROOT_DIR}/../transforms/language/doc_quality/test-data/input/ kfp/test/doc_quality/input
mc cp --recursive ${ROOT_DIR}/../transforms/language/language_id/test-data/input/ kfp/test/lang_id/input
# universal
mc cp --recursive ${ROOT_DIR}/../transforms/universal/blocklist/test-data/input/ kfp/test/blocklist/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/blocklist/test-data/domains/ kfp/test/blocklist/domains
mc cp --recursive ${ROOT_DIR}/../transforms/universal/doc_id/test-data/input/ kfp/test/doc_id/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/ededup/test-data/input/ kfp/test/ededup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/fdedup/test-data/input/ kfp/test/fdedup/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/filter/test-data/input/ kfp/test/filter/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/noop/test-data/input/ kfp/test/noop/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/resize/test-data/input/ kfp/test/resize/input
mc cp --recursive ${ROOT_DIR}/../transforms/universal/tokenization/test-data/ds01/input/ kfp/test/tokenization/ds01/input

