# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms_workflows

IGNORE := $(shell bash -c "sed -n /=/p  ${REPOROOT}/transforms/requirements.env | sed 's/=/:=/' | sed 's/^/export /' > makeenv")
include makeenv

DOCKER_IMAGE_NAME=code_quality
DOCKER_IMAGE_VERSION=${CODE_QUALITY}
