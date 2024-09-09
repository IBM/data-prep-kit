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

# Excluded from build
# ./code/malware/ray
# ./language/pii_redactor/ray


## Ray Transforms:  `find . -name src | grep ray/src`
TRANSFORMS_NAMES = code/proglang_select \
	code/header_cleanser \
	code/code_quality \
	code/repo_level_ordering \
	code/code2parquet \
	language/doc_chunk \
	language/doc_quality \
	language/lang_id \
	language/text_encoder \
	language/pdf2parquet \
	universal/fdedup \
	universal/tokenization \
	universal/ededup \
	universal/profiler \
	universal/doc_id \
	universal/filter \
	universal/resize

# doc chunk has conflict dependencies with pdf2parquet that need to be resolved
# doc_chunk depends on docling>=1.8.2,<2.0.0
# pdf2parquet depends on docling==1.7.0


test-src:: .transforms.setup venv
#	source venv/bin/activate && cd ../../../data-processing-lib/python && $(PYTHON) -m pip install .
#	source venv/bin/activate && cd ../../../data-processing-lib/ray && $(PYTHON) -m pip install .
	source venv/bin/activate && cd ../python && $(MAKE) src && $(PYTHON) -m pip install .
	source venv/bin/activate &&  cd ../ray && $(MAKE) src && $(PYTHON) -m pip install .
	$(MAKE) run-ut
	@# Help: Do any default transform setup before running make src and setting up a test environment

test-with-python-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate &&  cd ../ray && $(MAKE) src && $(PYTHON) -m pip install .
	$(MAKE) test-src

test-with-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms_ray==$(DPK_TRNASFORM_REV) 
	$(MAKE) test-src

