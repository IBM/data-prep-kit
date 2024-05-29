REPOROOT=../../..

KFPv2 ?= 1
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
ifeq ($(KFPv2), 1)
	$(MAKE) -C kfp_ray/v2 workflow-venv
else
	$(MAKE) -C kfp_ray/v1 workflow-venv
endif

.PHONY: workflow-build
workflow-build:
ifeq ($(KFPv2), 1)
	$(MAKE) -C kfp_ray/v2 workflow-build
else
 	$(MAKE) -C kfp_ray/v1 workflow-build
endif
	

.PHONY: workflow-test
workflow-test:
ifeq ($(KFPv2), 1)
	$(MAKE) -C kfp_ray/v2 workflow-build
else
	$(MAKE) -C kfp_ray/v1 workflow-test
endif

.PHONY: workflow-upload
workflow-upload:
ifeq ($(KFPv2), 1)
	$(MAKE) -C kfp_ray/v2 workflow-upload
else
	$(MAKE) -C kfp_ray/v1 workflow-upload
endif

.PHONY: workflow-reconcile-requirements
workflow-reconcile-requirements:
ifeq ($(KFPv2), 1)
	$(MAKE) -C kfp_ray/v2 reconcile-requirements
else
	$(MAKE) -C kfp_ray/v1 reconcile-requirements
endif
