REPOROOT=../../
# Use make help, to see the available rules
include ${REPOROOT}/.make.defaults

clean::
	@# Help: Recursively make $@ all subdirs 
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

workflow-reconcile-requirements::
	@# Help: Recursively make $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

