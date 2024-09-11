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

setup::

build::

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
