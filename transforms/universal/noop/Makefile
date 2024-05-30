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
venv::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

image:: 
	@# Help: Recursively make $@ in all subdirs 
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
	$(MAKE) -C kfp_ray/v1 workflow-venv

.PHONY: workflow-build
workflow-build:
	$(MAKE) -C kfp_ray/v1 workflow-build

.PHONY: workflow-test
workflow-test:
	$(MAKE) -C $(PIPELINE_PATH) workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C $(PIPELINE_PATH) workflow-upload

.PHONY: workflow-reconcile-requirements
workflow-reconcile-requirements:
	$(MAKE) -C $(PIPELINE_PATH) workflow-reconcile-requirements
