#################################################################################################################
#
# This is the top level makefile, which is intended to be able to process a common set of rules on all 
# sub-projects underneath this directory.  Currently, the common/standardized set of rules are as follows
# and supported by .make.defaults 
#
# clean: 
# build:
# test:
#
# When finally getting to a makefile that requires a rule implementation, for example to test the build,
# that makefile should override/implement the rule to meet its needs.  Such a rule may continue to recurse
# using "$(MAKE) <rule>-recurse", for example "$(MAKE) test-recurse". 
#
# Each rule is called recursively on sub-directories and if a similar inclusion is done in the sub-Makefiles,
# the rules will be applied/executed recursively in their sub-directories.
#
#################################################################################################################

# Add local or overriding rules here
include Makefile.env

include cluster.mk

.PHONY: setup-kind-cluster
setup-kind-cluster:
	$(MAKE) delete-kind-cluster
	$(MAKE) create-kind-cluster
	$(MAKE) cluster-prepare
	$(MAKE) cluster-prepare-wait
	cd $(KIND_DIR)/hack && ./ingress.sh deploy
	@echo "setup-cluster completed"

# Get some common rules for the whole repo
include .make.defaults

########## ########## ########## ########## ########## ########## ########## ########## 
# Global rules that are generally to be implemented in the sub-directories and can
# be overridden there (the double colon on the rule makes the overridable). 

clean:: 
	@# Help: clean, Recursively clean all subdirs 
	$(MAKE) RULE=clean .recurse

build:: 
	@# Help: build, Recursively build all subdirs 
	$(MAKE) RULE=build .recurse

test::  
	@# Help: test, Recursively test in all subdirs 
	@$(MAKE) RULE=test .recurse


