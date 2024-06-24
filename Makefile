#################################################################################################################
#
# This is the top level makefile, which is intended to be able to process a common set of rules on all 
# sub-projects underneath this directory.  Currently, the common/standardized set of rules are as follows
# and supported by .make.defaults 
#
# setup: 
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

REPOROOT=.

# Get some common rules for the whole repo
include .make.defaults

########## ########## ########## ########## ########## ########## ########## ########## 
# Global rules that are generally to be implemented in the sub-directories and can
# be overridden there (the double colon on the rule makes the overridable). 

clean:: 
	@# Help: Recursively $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

setup::
	@# Help: Recursively $@ in all subdirs
	@$(MAKE) RULE=$@ .recurse

build:: 
	@# Help: Recursively $@ in all subdirs 
	$(MAKE) RULE=$@ .recurse

test::  
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

publish::
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

set-versions:  
	@# Help: Recursively $@ in all subdirs 
	@$(MAKE) RULE=$@ .recurse

#set-release-verions:
#	@# Help: Update all internally used versions to not include the release suffix. 
#	@$(MAKE) DPK_VERSION_SUFFIX= set-versions

#lib-release:
#	@# Help: Set versions to be unsuffixed and publish libraries 
#	@$(MAKE) set-release-versions 
#	@$(MAKE) publish-lib 
	
show-version:
	@echo $(DPK_VERSION)

#publish-lib:
#	@# Help: Publish data-prep-kit $(DPK_LIB_VERSION) and data-prep-kit-kfp $(DPK_LIB_KFP_VERSION) libraries to pypi 
#	@$(MAKE) -C $(DPK_PYTHON_LIB_DIR) build publish
#	@$(MAKE) -C $(DPK_RAY_LIB_DIR) build publish
#	@$(MAKE) -C $(DPK_SPARK_LIB_DIR) build publish
#	@$(MAKE) -C kfp/kfp_support_lib build publish
#	@echo ""
#	@echo "This modified files in the repo. Please be sure to commit/push back to the repository."
#	@echo ""



