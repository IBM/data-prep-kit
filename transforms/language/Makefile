REPOROOT=../..
# Use make help to see the avialable rules
include ../../.make.defaults

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

test-lib-src::
	@# Help: Recursively test in all subdirs using source from data-processing-lib
	$(MAKE) RULE=test-lib-src .recurse

