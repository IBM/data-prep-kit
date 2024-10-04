# Use make help, to see the available rules
REPOROOT=..
include $(REPOROOT)/.make.defaults

clean::
	@# Help: Clean up the distribution build and the venv
	rm -rf dist venv
	rm -rf src/*egg-info

.check-env::
	@echo "Checks passed"

setup::

set-versions: .check-env
	$(MAKE) TOML_VERSION=$(DPK_LIB_VERSION) .defaults.update-toml

build:: build-dist

#build:: update-toml .defaults.build-dist
build-dist :: .defaults.build-dist

publish:: publish-dist

publish-dist :: .check-env .defaults.publish-dist

venv::	pyproject.toml
	@# Help: Create the virtual environment using pyproject.toml
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm makeenv || true
	$(PYTHON) -m venv venv
	source venv/bin/activate; 	\
	pip install --upgrade pip;	\
	pip install -e .;		\
	pip install pytest pytest-mock pytest-datadir pytest-cov moto==5.0.5 markupsafe==2.0.1

image::
	@# Help: Placeholder does nothing for now.
	@echo "Image building for ray is in the works (comming soon)."

# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped..
# TODO: the following fails.  Why?  source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  .
.PHONY: test
test::   venv
	@# Help: Use the already-built virtual environment to run pytest on the test directory.
	source venv/bin/activate; $(PYTEST);
