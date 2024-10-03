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

kind-load-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

docker-load-image::
	@# Help: Recursively make $@ in all subdirs
	$(MAKE) RULE=$@ .recurse

docker-save-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

set-versions:
	@# Help: Recursively $@ in all subdirs
	$(MAKE) RULE=$@ .recurse

.PHONY: workflow-venv
workflow-venv:
	if [ -e kfp_ray ]; then                 \
	    $(MAKE) -C kfp_ray workflow-venv;   \
	fi

.PHONY: workflow-test
workflow-test:
	if [ -e kfp_ray ]; then                 \
	    $(MAKE) -C kfp_ray workflow-test;   \
	fi
	
.PHONY: workflow-upload
workflow-upload:
	if [ -e kfp_ray ]; then                 \
	    $(MAKE) -C kfp_ray workflow-upload; \
	fi

.PHONY: workflow-build
workflow-build:
	if [ -e kfp_ray ]; then                 \
	    $(MAKE) -C  kfp_ray workflow-build; \
	fi

