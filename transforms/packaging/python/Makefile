# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.

# $(REPOROOT)/.make.versions file contains the versions

include $(REPOROOT)/transforms/.make.transforms
include ../.make.packaging

PACKAGING_RUN_TIME=python
DPK_TRNASFORM_REV=0.2.1.dev1

TRANSFORMS_NAMES = code/code_quality \
    	code/code2parquet \
    	code/header_cleanser \
		code/code_quality \
		code/proglang_select \
		language/doc_chunk \
		language/doc_quality \
		language/lang_id \
		language/pdf2parquet \
		language/text_encoder \
        language/pii_redactor \
		universal/ededup \
		universal/filter \
		universal/resize \
		universal/tokenization \
        universal/html2parquet

test-with-pypi:
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms==$(DPK_TRNASFORM_REV) 
	$(MAKE) test-src
	@# Help: Load wheel from pypi and run all unit tests: final step in verification after deploying to pypi)




