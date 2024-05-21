# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../../../..
include $(REPOROOT)/transforms/.make.transforms

# Define the root of the local git clone for the common rules to be able
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
