# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../..
# Include the common rules.
# Use "make help" to see them.
include ../../.make.defaults

export PYTHON ?= python
PYTEST=pytest

clean::
	@# Help: Clean up the venv 
	rm -rf venv || true
	rm makeenv || true
	$(MAKE) RULE=$@ .recurse

setup::
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

venv:: 
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

build::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

test::
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse
