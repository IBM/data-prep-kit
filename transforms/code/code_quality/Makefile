
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../..
include ../../.make.transforms

TRANSFORM_NAME=cq
DOCKER_IMAGE_VERSION=0.1.0

# Use default rule inherited from makefile.common
clean:: .transforms.clean

# Use default rule inherited from makefile.common
test:: .transforms.test

# Use default rule inherited from makefile.common
image:: .transforms.image

# Use default rule inherited from makefile.common
build:: .transforms.build

# Use default rule inherited from makefile.common
venv:: .transforms.venv

# Use default rule inherited from makefile.common
install-src:: .transforms.install-src

test-src:: .transforms.test-src

test-image:: .transforms.test-image

