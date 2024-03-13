REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults

clean::
	@# Help: clean, Recursively clean all subdirs 
	$(MAKE) RULE=clean .recurse
build::
	@# Help: build, Recursively build all subdirs 
	$(MAKE) RULE=build .recurse
venv::
	@# Help: build, Recursively create venv in subdirs 
	$(MAKE) RULE=venv .recurse
test:: 
	@# Help: test, Recursively build test in all subdirs 
	@$(MAKE) RULE=test .recurse

test-lib-src::
	@# Help: test-lib-src, Install the library source into the venv and test 
	$(MAKE) RULE=test-lib-src .recurse

