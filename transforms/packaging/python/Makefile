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
# ./language/pii_redactor
# ./universal/html2parquet

TRANSFORMS_NAMES = code/code_quality \
	    	code/code2parquet \
    		code/header_cleanser \
		code/proglang_select \
		language/doc_chunk \
		language/doc_quality \
		language/lang_id \
		language/pdf2parquet \
		language/text_encoder \
		universal/ededup \
		universal/filter \
		universal/resize \
		universal/tokenization \
		universal/doc_id

# language/doc_chunk has conflict dependencies with pdf2parquet that need to be resolved
# doc_chunk depends on docling>=1.8.2,<2.0.0
# pdf2parquet depends on docling==1.7.0


test-src:: .transforms.setup venv
#	source venv/bin/activate && cd ../../../data-processing-lib/python && $(PYTHON) -m pip install .
	source venv/bin/activate && cd ../python && $(MAKE) src && $(PYTHON) -m pip install .
	$(MAKE) run-ut
	@# Help: Do any default transform setup before running make src and setting up a test environment

test-with-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms==$(DPK_TRNASFORM_REV) 
	$(MAKE) run-ut
	@# Help: Load wheel from pypi and run all unit tests: final step in verification after deploying to pypi)




