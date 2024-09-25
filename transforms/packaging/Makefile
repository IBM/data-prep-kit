REPOROOT=../../
# Use make help, to see the available rules
include ../../.make.defaults

setup::

clean::
	# Clean up workflows common virtual environment.
	rm -rf venv || true
	rm -rf *.back || true
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

src::
	@# Help: Recursively setup $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

setup::

build::

build-dist::
	@# Help: Recursively build distributions in all subdirs 
	$(MAKE) RULE=$@ .recurse

publish-dist::
	@# Help: Recursively publish distributions in all subdirs 
	$(MAKE) RULE=$@ .recurse

venv::

image:: 

publish:: 

test-image:: 

test:: 

test-src::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

kind-load-image::

docker-load-image::

docker-save-image::

workflow-venv::

workflow-test::

workflow-build::

workflow-upload::

set-versions::
	@# Help: Recursively make $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse
