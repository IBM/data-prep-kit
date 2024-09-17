
# Directories in the transforms/universal directory for which we want to generate test workflows
UNIVERSAL_TRANSFORMS=doc_id ededup fdedup filter html2parquet noop profiler resize tokenization
# Directories in the transforms/code directory for which we want to generate test workflows
CODE_TRANSFORMS=code2parquet code_quality header_cleanser malware proglang_select repo_level_ordering
# Directories in the transforms/language directory for which we want to generate test workflows
LANG_TRANSFORMS=doc_chunk doc_quality lang_id pdf2parquet pii_redactor text_encoder


transform-tests:
	for i in $(CODE_TRANSFORMS); do	\
	    cat test-transform.template | sed -e "s?@TARGET_TRANSFORM_DIR@?transforms/code/$$i?g" > test-code-$$i.yml;	\
	done
	for i in $(LANG_TRANSFORMS); do	\
	    cat test-transform.template | sed -e "s?@TARGET_TRANSFORM_DIR@?transforms/language/$$i?g" > test-language-$$i.yml;	\
	done
	for i in $(UNIVERSAL_TRANSFORMS); do	\
	    cat test-transform.template | sed -e "s?@TARGET_TRANSFORM_DIR@?transforms/universal/$$i?g" > test-universal-$$i.yml;	\
	done

