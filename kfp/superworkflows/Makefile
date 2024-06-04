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

.PHONY: workflow-reconcile-requirements
workflow-reconcile-requirements:
	$(MAKE) -C ray/kfp_v1 workflow-reconcile-requirements

set-versions::  #workflow-reconcile-requirements
	@# Help: Nothing to do here yet.

clean::

build::

test::

