REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults

clean::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

setup::

build::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

test:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

publish:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

set-versions:  
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse
