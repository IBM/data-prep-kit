REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults
include .make.transforms_workflows

setup::
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

clean:: .transforms_workflows.clean
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

workflow-venv::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse	

workflow-build::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-test::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-upload::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

