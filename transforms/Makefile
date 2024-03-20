REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults

setup::
	@# Help: Recursively setup all subdirs 
	$(MAKE) RULE=setup .recurse
clean::
	@# Help: Recursively clean all subdirs 
	$(MAKE) RULE=clean .recurse
build::
	@# Help: Recursively build all subdirs 
	$(MAKE) RULE=build .recurse
venv::
	@# Help: Recursively create venv in subdirs 
	$(MAKE) RULE=venv .recurse
test:: 
	@# Help: Recursively test in all subdirs 
	@$(MAKE) RULE=test .recurse

test-lib-src::
	@# Help: Install the library source into the venv and test 
	$(MAKE) RULE=test-lib-src .recurse

