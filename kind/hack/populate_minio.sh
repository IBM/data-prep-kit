#!/usr/bin/env bash

# Define variables
minio_url="http://localhost:8080"
minio_alias="kfp"
minio_access_key="minio"
minio_secret_key="minio123"
source_dir="${ROOT_DIR}/../transforms"

# Function to copy data
copy_data() {
    local module="$1"
    local src="$2"
    local dest="$3"

    echo "Copying data for $module"
    mc cp --recursive "${source_dir}/${module}/test-data/input/" "kfp/test/${dest}/input"
}

# Set Minio alias
echo "Creating Minio alias"
mc alias set "$minio_alias" "$minio_url" "$minio_access_key" "$minio_secret_key"

# Create test buckets
echo "Creating test buckets"
mc mb "$minio_alias/test/code_quality"
mc mb "$minio_alias/test/proglang_select"
mc mb "$minio_alias/test/malware"
mc mb "$minio_alias/test/doc_id"
mc mb "$minio_alias/test/ededup"
mc mb "$minio_alias/test/fdedup"
mc mb "$minio_alias/test/filter"
mc mb "$minio_alias/test/noop"
mc mb "$minio_alias/test/tokenization"

# Copy data for each module
copy_data "transforms/code/code_quality" "code_quality" "code_quality"
copy_data "transforms/code/proglang_select" "proglang_select" "proglang_select"
copy_data "transforms/code/malware" "malware" "malware"
copy_data "transforms/universal/doc_id" "doc_id" "doc_id"
copy_data "transforms/universal/ededup" "ededup" "ededup"
copy_data "transforms/universal/fdedup" "fdedup" "fdedup"
copy_data "transforms/universal/filter" "filter" "filter"
copy_data "transforms/universal/noop" "noop" "noop"
copy_data "transforms/universal/tokenization" "tokenization/ds01" "tokenization_ds01"