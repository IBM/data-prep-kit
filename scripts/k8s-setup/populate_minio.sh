#!/usr/bin/env bash


################################################################################
# SECURITY NOTE: LOCAL TESTING CREDENTIALS ONLY
#
# This script uses intentionally public test credentials (minio/minio123) for:
# - Local development on Kind clusters
# - CI/CD testing in GitHub Actions
# - Community contributor testing
#
# These credentials:
# - Only work with cluster-internal MinIO endpoints
# - Are used in ephemeral test environments only
# - NEVER contain production data or sensitive information
# - Are NOT production credentials
#
# DO NOT use these credentials for production deployments.
################################################################################


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
mc cp --recursive ${REPOROOT}/transforms/code/code_quality/test-data/input/ kfp/test/code_quality/input
mc cp --recursive ${REPOROOT}/transforms/code/code2parquet/test-data/input/data-processing-lib.zip kfp/test/code2parquet/input
mc cp --recursive ${REPOROOT}/transforms/code/code2parquet/test-data/languages/ kfp/test/code2parquet/languages
mc cp --recursive ${REPOROOT}/transforms/code/proglang_select/test-data/input/ kfp/test/proglang_select/input
mc cp --recursive ${REPOROOT}/transforms/code/proglang_select/test-data/languages/ kfp/test/proglang_select/languages
mc cp --recursive ${REPOROOT}/transforms/code/malware/test-data/input/ kfp/test/malware/input
mc cp --recursive ${REPOROOT}/transforms/code/header_cleanser/test-data/input/ kfp/test/header_cleanser/input
mc cp --recursive ${REPOROOT}/transforms/code/repo_level_order/test-data/input/ kfp/test/repo_level_ordering/input
mc cp --recursive ${REPOROOT}/transforms/code/license_select/test-data/input/ kfp/test/license_select/input
mc cp --recursive ${REPOROOT}/transforms/code/license_select/test-data/sample_approved_licenses.json kfp/test/license_select/
# language
mc cp --recursive ${REPOROOT}/transforms/language/lang_id/test-data/input/ kfp/test/lang_id/input
mc cp --recursive ${REPOROOT}/transforms/language/doc_quality/test-data/input/ kfp/test/doc_quality/input
mc cp --recursive ${REPOROOT}/transforms/language/docling2parquet/test-data/input/2206.01062.pdf kfp/test/docling2parquet/input
mc cp --recursive ${REPOROOT}/transforms/language/text_encoder/test-data/input/ kfp/test/text_encoder/input
mc cp --recursive ${REPOROOT}/transforms/language/doc_chunk/test-data/input/ kfp/test/doc_chunk/input
mc cp --recursive ${REPOROOT}/transforms/language/html2parquet/test-data/input/test1.html kfp/test/html2parquet/input
mc cp --recursive ${REPOROOT}/transforms/language/enrichment/test-data/input/ kfp/test/enrichment/input
mc cp --recursive ${REPOROOT}/transforms/language/ml_filter/test-data/input/ kfp/test/ml_filter/input
mc cp --recursive ${REPOROOT}/transforms/language/gneissweb_classification/test-data/input/ kfp/test/gneissweb_classification/input
mc cp --recursive ${REPOROOT}/transforms/language/extreme_tokenized/test-data/input/ kfp/test/extreme_tokenized/input
mc cp --recursive ${REPOROOT}/transforms/language/readability/test-data/input/ kfp/test/readability/input
# universal
mc cp --recursive ${REPOROOT}/transforms/universal/doc_id/test-data/input/ kfp/test/doc_id/input
mc cp --recursive ${REPOROOT}/transforms/universal/ededup/test-data/input/ kfp/test/ededup/input
mc cp --recursive ${REPOROOT}/transforms/universal/fdedup/ray/test-data/input/ kfp/test/fdedup/input
mc cp --recursive ${REPOROOT}/transforms/universal/filter/test-data/input/ kfp/test/filter/input
mc cp --recursive ${REPOROOT}/transforms/universal/noop/test-data/input/ kfp/test/noop/input
mc cp --recursive ${REPOROOT}/transforms/universal/tokenization/test-data/ds01/input/ kfp/test/tokenization/ds01/input
mc cp --recursive ${REPOROOT}/transforms/universal/profiler/test-data/input/ kfp/test/profiler/input
mc cp --recursive ${REPOROOT}/transforms/universal/resize/test-data/input/ kfp/test/resize/input
mc cp --recursive ${REPOROOT}/transforms/universal/hap/test-data/input/ kfp/test/hap/input
mc cp --recursive ${REPOROOT}/transforms/universal/rep_removal/test-data/input/ kfp/test/rep_removal/input
mc cp --recursive ${REPOROOT}/transforms/universal/blocklist/test-data/input/ kfp/test/blocklist/input
mc cp --recursive ${REPOROOT}/transforms/universal/blocklist/test-data/domains/ kfp/test/blocklist/domains
mc cp --recursive ${REPOROOT}/transforms/universal/fineweb_quality_annotator/test-data/input/ kfp/test/fineweb_quality_annotator/input
mc cp --recursive ${REPOROOT}/transforms/universal/gopher_repetition_annotator/test-data/input/ kfp/test/gopher_repetition_annotator/input
