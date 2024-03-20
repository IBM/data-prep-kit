REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults

clean::
	@# Help: clean, Recursively clean all subdirs 
	$(MAKE) RULE=clean .recurse
setup::

build::
	@# Help: build, Recursively build all subdirs 
	$(MAKE) RULE=build .recurse
test:: 
	@# Help: test, Recursively build test in all subdirs 
	@$(MAKE) RULE=test .recurse

