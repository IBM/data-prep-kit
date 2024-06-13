REPOROOT=../../
# Use make help, to see the available rules
include ${REPOROOT}/.make.defaults

.PHONY: workflow-venv
workflow-venv:
	$(MAKE) -C ray/kfp_v1 workflow-venv

.PHONY: workflow-build
workflow-build:
	$(MAKE) -C ray/kfp_v1 workflow-build

.PHONY: workflow-test
workflow-test:
	$(MAKE) -C ray/kfp_v1 workflow-test

.PHONY: workflow-upload
workflow-upload:
	$(MAKE) -C ray/kfp_v1 workflow-upload

.PHONY: set-versions
set-versions:
	$(MAKE) -C ray/kfp_v1 set-versions

clean::

build::

test::

