REPOROOT=../../../
# Use "make help" to see them.
include ../../../.make.defaults

.reconcile-requirement::
	$(MAKE) RULE=.reconcile-requirement .recurse

clean:: 
	@# Help: Recursively $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

setup::

build::
	$(MAKE) RULE=$@ .recurse

venv:: 
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

test:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

upload::
	@# Help: Recursively make $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse
