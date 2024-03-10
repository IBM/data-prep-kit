
# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
include ../../makefile.transforms

DOCKER_IMAGE_NAME=noop
DOCKER_IMAGE_VERSION=0.6

# Use default rule inherited from ../../makefile.transforms
.PHONY: clean
clean: .clean

# Use default rule inherited from ../../makefile.transforms
.PHONY: test 
test: .test

# Use default rule inherited from ../../makefile.transforms
.PHONY: image 
image: .image

# Use default rule inherited from ../../makefile.transforms
.PHONY: build 
build: .build

# Use default rule inherited from ../../makefile.transforms
venv: .venv

# Use default rule inherited from ../../makefile.transforms
.PHONY:  install_lib_source
install_lib_source: .install_lib_source


