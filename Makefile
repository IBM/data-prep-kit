#################################################################################################################
#
# This is the top level makefile, which is intended to be able to process a common set of rules on all 
# sub-projects underneath this directory.  Currently, the common/standardized set of rules are as follows
# and supported by makefile.include
#
# clean: 
# build:
# publish:
#
# When finally getting to a makefile that requires a rule implementation, for example to publish the build,
# that makefile should override/implement the rule to meet its needs.  Such a rule may continue to recurse
# using "$(MAKE) <rule>-recurse", for example "$(MAKE) publish-recurse". 
#
# Each rule is called recursively on sub-directories and if a similar inclusion is done in the sub-Makefiles,
# the rules will be applied/executed recursively in their sub-directories.
#
#################################################################################################################
# Provides support for the recursive evaluation of rules in sub-directories and likely other function as we evolve.
include makefile.recurse

# Add local or overriding rules here

