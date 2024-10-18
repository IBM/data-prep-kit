REPOROOT=../../..
# Use make help, to see the available rules
include $(REPOROOT)/.make.defaults
include transform.config

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

set-versions:  
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
	@is_blacklisted=$$(cd $(REPOROOT); bash scripts/check-workflows.sh -show-kfp-black-list | grep $(TRANSFORM_NAME)); \
	if [ -z "$$is_blacklisted"  ]; \
		then \
			echo $(MAKE) -C kfp_ray $@ ; \
		else \
			echo "Skipping KFP workflow: Transform is blacklisted " ; \
	fi

.PHONY: workflow-test
workflow-test:
	@is_blacklisted=$$(cd $(REPOROOT); bash scripts/check-workflows.sh -show-kfp-black-list | grep $(TRANSFORM_NAME)); \
	if [ -z "$$is_blacklisted"  ]; \
		then \
			echo $(MAKE) -C kfp_ray $@ ; \
		else \
			echo "Skipping KFP workflow: Transform is blacklisted " ; \
	fi
	
.PHONY: workflow-upload
workflow-upload:
	@is_blacklisted=$$(cd $(REPOROOT); bash scripts/check-workflows.sh -show-kfp-black-list | grep $(TRANSFORM_NAME)); \
	if [ -z "$$is_blacklisted"  ]; \
		then \
			echo $(MAKE) -C kfp_ray $@ ; \
		else \
			echo "Skipping KFP workflow: Transform is blacklisted " ; \
	fi

.PHONY: workflow-build
workflow-build:
	is_blacklisted=$$(cd $(REPOROOT); bash scripts/check-workflows.sh -show-kfp-black-list | grep $(TRANSFORM_NAME)); \
	if [ -z "$$is_blacklisted"  ]; \
		then \
			echo $(MAKE) -C kfp_ray $@ ; \
		else \
			echo "Skipping KFP workflow: Transform is blacklisted " ; \
	fi

