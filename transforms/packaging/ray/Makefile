# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.

# $(REPOROOT)/.make.versions file contains the versions

include $(REPOROOT)/transforms/.make.transforms
include ../.make.packaging

PACKAGING_RUN_TIME=ray
DPK_TRNASFORM_REV=0.2.1.dev0

## Ray Transforms:  `find . -name src | grep ray/src`
TRANSFORMS_NAMES = code/proglang_select \
	code/header_cleanser \
	code/code_quality \
	code/repo_level_ordering \
	code/code2parquet \
	language/doc_quality \
	language/doc_chunk \
	language/lang_id \
	language/text_encoder \
	language/pdf2parquet \
	language/pii_redactor \
	universal/fdedup \
	universal/tokenization \
	universal/ededup \
	universal/profiler \
	universal/doc_id \
	universal/filter \
	universal/resize

test-with-local-python:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	cd ../python; \
	$(PYTHON) -m pip install . ; \
	cd ../ray; \
	$(PYTHON) -m pip install . ; \
	$(PYTHON) -m pip install data_prep_toolkit_transforms_ray==$(DPK_TRNASFORM_REV)
	$(MAKE) test-src

test-with-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms_ray==$(DPK_TRNASFORM_REV) 
	$(MAKE) test-src

