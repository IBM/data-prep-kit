# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

#TRANSFORM_NAME=doc_quality

include $(REPOROOT)/transforms/.make.transforms

venv::	.transforms.python-venv


TRANSFORMS_NAMES = code/code_quality \
    	code/code2parquet \
    	code/header_cleanser \
		code/code_quality \
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
		universal/tokenization


test::	test-src

clean:: .transforms.clean

image:: .transforms.python-image

test-src:: venv
	source venv/bin/activate;       \
	for T in $(TRANSFORMS_NAMES); do                    \
	    @echo running unit test on: $$T ; \
		$(PYTEST) $(REPOROOT)/transforms/$$T/python/test; \
	done;


clean:: .transforms.clean

image:: .transforms.python-image

setup:: .transforms.setup

build:: build-dist
        
build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist

run-local-sample: .transforms.run-local-sample

run-local-python-sample: .transforms.run-local-python-sample


