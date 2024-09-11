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


#Excluded List
# ./code/malware
# ./universal/html2parquet
# ./universal/profiler      # Missing implementation 
# ./universal/fdedup 	    # Missing implementation
#  code/repo_level_ordering # Missing implementation


TRANSFORMS_NAMES = code/code_quality \
	    	code/code2parquet \
    		code/header_cleanser \
		code/proglang_select \
		language/doc_chunk \
		language/doc_quality \
		language/lang_id \
		language/pdf2parquet \
		language/pii_redactor \
		language/text_encoder \
		universal/ededup \
		universal/filter \
		universal/resize \
		universal/tokenization \
		universal/doc_id


# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(DPK_TRANSFORMS_VERSION) TOML_VERSION=$(DPK_TRANSFORMS_VERSION) .transforms.set-versions 

test-src:: .transforms.setup venv
	source venv/bin/activate && $(PYTHON) -m pip install ../../../data-processing-lib/python
	source venv/bin/activate && $(MAKE) src && $(PYTHON) -m pip install .
	$(MAKE) run-ut
	@# Help: Do any default transform setup before running make src and setting up a test environment

test-with-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms==$(DPK_TRANSFORMS_VERSION) 
	$(MAKE) run-ut
	@# Help: Load wheel from pypi and run all unit tests: final step in verification after deploying to pypi)




