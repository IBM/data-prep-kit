REPOROOT=../../
# Use make help, to see the available rules
include ${REPOROOT}/.make.defaults

KFPv2 ?= 0

.PHONY: publish
publish:

.PHONY: workflow-venv
workflow-venv:
ifeq ($(KFPv2), 1)
	echo "Skipping build as KFPv2 is defined"
else
	$(MAKE) -C ray/kfp_v1 workflow-venv
endif

.PHONY: workflow-build
setup:
ifeq ($(KFPv2), 1)
	echo "Skipping build as KFPv2 is defined"
else
	$(MAKE) -C ray/kfp_v1 setup
endif

.PHONY: workflow-test
workflow-test:
ifeq ($(KFPv2), 1)
	echo "Skipping build as KFPv2 is defined"
else
	$(MAKE) -C ray/kfp_v1 workflow-test
endif

.PHONY: workflow-upload
workflow-upload:
ifeq ($(KFPv2), 1)
	# we want to prevent execution of the rule, when we run `make build` in upper directories and KFPv2==1
	echo "Skipping build as KFPv2 is defined"
else
	$(MAKE) -C ray/kfp_v1 workflow-upload
endif

.PHONY: set-versions
set-versions:
ifeq ($(KFPv2), 1)
	echo "Skipping build as KFPv2 is defined"
else
	$(MAKE) -C ray/kfp_v1 set-versions
endif

clean::

build::

test::

