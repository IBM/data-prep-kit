# Use make help, to see the available rules
REPOROOT=../..
include $(REPOROOT)/.make.defaults

TAG := "v${DPK_LIB_VERSION}"


clean:: .defaults.clean
	@# Help: Clean up the distribution build and the venv 
	-rm -rf dist > /dev/null 2>&1

.PHONY: .check-env
.check-env::
	@echo "Checks passed"

setup::

set-versions: .check-env
	$(MAKE) TOML_VERSION=$(DPK_LIB_VERSION) .defaults.update-toml
	cat pyproject.toml | sed -e 's/"ray[default]==.*",/"ray[default]==$(RAY)",/' > tt.toml
	mv tt.toml pyproject.toml


build:: build-dist 

build-dist :: .defaults.build-dist 

publish:: publish-dist  

publish-dist :: .check-env .defaults.publish-dist 

venv::	pyproject.toml
	$(MAKE) .defaults.ray-lib-src-venv
	pip install moto==5.0.5 markupsafe==2.0.1


image:: 
	@# Help: Placeholder does nothing for now.
	@echo "Image building for ray is in the works (coming soon)."

# Here we run each test directory of tests and each ray launched test separately, because
# it seems when running multiple ray launch tests in a single pytest run there is some sort of ray.init() duplication.
# pytest-forked was tried, but then we get SIGABRT in pytest when running the s3 tests, some of which are skipped.. 
test::  venv 
	@# Help: Use the already-built virtual environment to run pytest on the test directory. 
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_ray_tests/launch/ray/ray_util_test.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_ray_tests/launch/ray/ray_launcher_test.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_ray_tests/launch/ray/ray_test_noop_launch.py;
	source venv/bin/activate; export PYTHONPATH=../src; cd test; $(PYTEST)  data_processing_ray_tests/launch/ray/ray_multi_launcher_test.py;


