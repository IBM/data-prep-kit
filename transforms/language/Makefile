REPOROOT=../..
# Use make help to see the avialable rules
include $(REPOROOT)/.make.defaults

clean::
	@# Help: Recursively clean all subdirs 
	$(MAKE) RULE=clean .recurse
build::
	@# Help: Recursively build all subdirs 
	$(MAKE) RULE=build .recurse
venv::
	@# Help: Recursively create venv in all subdirs 
	$(MAKE) RULE=venv .recurse
test:: 
	@# Help: Recursively test in all subdirs 
	@$(MAKE) RULE=test .recurse
setup:: 
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse
image:: 
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-image:: 
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-src::
	@# Help: Recursively test in all subdirs using source from data-processing-lib
	$(MAKE) RULE=$@  .recurse

publish:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse


set-versions:  
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

kind-load-image::
	@# Help: Recursively make $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

docker-load-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

docker-save-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

workflow-venv::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse	

setup::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-test::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-upload::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-build::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse