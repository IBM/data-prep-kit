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

ifeq ($(T_SET), all)
# Cannot combine language/html2parquet with pdf2parquet due to:
#The conflict is caused by:
#    docling-ibm-models 1.1.7 depends on lxml<5.0.0 and >=4.9.1
#    trafilatura 1.12.0 depends on lxml>=5.2.2; platform_system != "Darwin" or python_version > "3.8"
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
		universal/tokenization \
		universal/ededup \
		/universal/doc_id	\
		universal/filter \
		universal/resize
TRANSFORM_PKG = "data_prep_toolkit_transforms"
endif

ifeq ($(T_SET), lang1)
TRANSFORMS_NAMES = language/doc_quality \
		language/lang_id \
		language/text_encoder \
		language/html2parquet \
		universal/tokenization \
		universal/ededup \
		/universal/doc_id	\
		universal/filter \
		universal/resize
TRANSFORM_PKG = "data_prep_toolkit_transforms_lang1"
endif

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(DPK_TRANSFORMS_VERSION) TOML_VERSION=$(DPK_TRANSFORMS_VERSION) .transforms.set-versions 

test-src:: 
	$(MAKE) src 
	$(MAKE) .transforms.python-venv
	$(MAKE) run-ut
	@# Help: Do any default transform setup before running make src and setting up a test environment

test-with-pypi:
	$(MAKE) src 
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install . 
	$(MAKE) run-ut
	@# Help: Load dependencies from pypi and run all unit tests: final step in verification BEFORE deploying to pypi)


test-wheel:
	-rm -fr venv
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install dist/*.whl
	$(MAKE) run-ut
	@# Help: Load wheel from local folder and run all unit tests



test-latest-patch:
	$(MAKE) clean 
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install $(TRANSFORM_PKG) 
	$(MAKE) run-ut
	@# Help: Load wheel from pypi and run all unit tests: final step in verification AFTER deploying to pypi)



