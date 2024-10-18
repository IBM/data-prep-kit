REPOROOT=../
# Use make help, to see the available rules
include ../.make.defaults
include ./transform.config


setup::
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

clean::
	# Clean up workflows common virtual environment.
	rm -rf venv || true
	rm -rf *.back || true
	@# Help: Recursively make $@ all subdirs 
	$(MAKE) RULE=$@ .recurse

setup::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

build::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse
venv::
	@# Help: Recursively make $@ in subdirs 
	$(MAKE) RULE=$@ .recurse

image:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

publish:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-image:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test:: 
	@# Help: Recursively make $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

test-src::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

kind-load-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

docker-load-image::
	@# Help: Recursively make $@ in all subdirs
	$(MAKE) RULE=$@ .recurse

docker-save-image::
	@# Help: Recursively make $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

workflow-venv::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse	

setup::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-test::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-build::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

workflow-upload::
	@# Help: Recursively make $@ in subdirs
	$(MAKE) RULE=$@ .recurse

set-versions::
	@# Help: Recursively make $@ in all subdirs
	make set-pkg-version
	@$(MAKE) RULE=$@ .recurse

set-pkg-version:
	@# Help: Set tag for this package and its dependencies
	cat pyproject.toml | sed -e 				\
		's/^version[ ]*=.*/version = "'${TRANSFORMS_PKG_VERSION}'"/' 	\
		> tt
	mv tt pyproject.toml
	echo $(DPK_VERSION)
	cat requirements.txt |	sed -e \
		's/data-prep-toolkit\([=><~][=]\).*/data-prep-toolkit\1$(DPK_VERSION)/' 	\
	    > tt
	mv tt requirements.txt
	cat requirements-ray.txt |	sed -e	\
		's/data-prep-toolkit\[ray\]\([=><~][=]\).*/data-prep-toolkit\[ray\]\1$(DPK_VERSION)/' 	\
	    > tt
	mv tt requirements-ray.txt


build-pkg-dist:
	@# Help: Build package wheel 
	## Most transforms today don't have a package name.... Need to fix that
	## In the meantime, we will copy everything to a single folder
	-rm -fr src
	mkdir src
	# Copy all the src folders recursively (not clear if they have subfolders)
	for x in $(shell find . | grep '[ray| python]/src$$') ; do \
		echo $$x ; \
		if [ -d "$$x" ]; then \
		   cp -r $$x/* src ; \
		fi \
	done
	# Only needs to build the whl
	$(MAKE) BUILD_WHEEL_EXTRA_ARG=-w .defaults.build-dist
	-rm -fr src

test-pkg-dist:
	@# Help: Setup environment and run unit tests for all transforms.
	-rm -fr venv
	python -m venv venv
	source venv/bin/activate && $(PYTHON) -m pip  install '$(REPOROOT)/data-processing-lib/dist/data_prep_toolkit-$(DPK_VERSION)-py3-none-any.whl[dev,ray]'
	source venv/bin/activate && $(PYTHON) -m pip  install 'dist/data_prep_toolkit_transforms-$(DPK_TRANSFORMS_VERSION)-py3-none-any.whl[all]'
	for T in $(shell find . | grep '[ray| python]/test$$') ; do \
        echo "running unit test on: $$T" ; \
		source venv/bin/activate && $(PYTEST) $$T; \
    done;
	@# Help: Setup environment and run unit tests for all transforms

publish-dist :: .defaults.publish-dist

