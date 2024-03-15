# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
include ../../../.make.defaults

# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms

venv::	.transforms.check_python_version
	@$(MAKE) .transforms.check_env	# Put this first so we don't get its help
	@# Help: Install tools required to install fasttext
	$(PYTHON) -m venv venv
	@source venv/bin/activate;					\
	$(PIP) install --upgrade pip;		\
	$(PIP) install setuptools wheel cmake;		\
	$(MAKE) .transforms.venv		\

build:: venv image

test::  .transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-lib-src:: .transforms.test-lib-src

install-lib-src:: .transforms.install-lib-src
