# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

include $(REPOROOT)/transforms/.make.transforms


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
	universal/fdedup \
	universal/tokenization \
	universal/ededup \
	universal/profiler \
	universal/doc_id \
	universal/filter \
	universal/resize


venv: 
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install . 


test::	setup venv test-src

clean:: .transforms.clean
	-rm -fr src
	-rm -fr *.egg-info
	-rm -fr dist
	-rm -fr build

image:: .transforms.python-image

test-src:: 
	source venv/bin/activate;       \
	for T in $(TRANSFORMS_NAMES); do                    \
	    echo running unit test on: $$T/ray/test ; \
		$(PYTEST) $(REPOROOT)/transforms/$$T/ray/test; \
	done;

test-with-pypi:
	$(MAKE) clean
	$(MAKE) .defaults.create-venv
	source venv/bin/activate;       \
	$(PYTHON) -m pip install data_prep_toolkit_transforms_ray==0.2.1.dev0 
	$(MAKE) test-src


setup: .transforms.setup 
	$(MAKE) src

src:
	for T in $(TRANSFORMS_NAMES); do                    \
	    echo copy src from  $$T/ray/src ; \
		cp -R $(REPOROOT)/transforms/$$T/ray/src/ src/ ; \
	done;
	-rm -fr *.egg-info
	-rm -fr dist
	-rm -fr build


build:: build-dist
        
publish:: publish-dist

build-dist:: .defaults.build-dist 

publish-dist:: .defaults.publish-dist


