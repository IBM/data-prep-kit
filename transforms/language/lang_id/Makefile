REPOROOT=../../..
# Use make help, to see the available rules
include $(REPOROOT)/.make.defaults

setup::
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

clean::
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

build::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse
	
setup::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

venv::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

image:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

set-versions::  
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

publish:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-image:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-src::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

load-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

.PHONY: workflow-venv
workflow-venv:
	$(MAKE) -C kfp_ray workflow-venv

.PHONY: workflow-test
workflow-test:
	$(MAKE) -C kfp_ray workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C kfp_ray workflow-upload

.PHONY: workflow-build
workflow-build:
	$(MAKE) -C  kfp_ray workflow-build