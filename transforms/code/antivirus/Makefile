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

DOCKER_IMAGE_NAME=antivirus
DOCKER_IMAGE_VERSION=0.1

venv::	.transforms.venv
	@$(DOCKER) build --target clamav-local -f Dockerfile -t clamav-local:latest .
	@echo "$(DOCKER) run -d --name clamav -p 3310:3310 clamav-local:latest" >> ./venv/bin/activate
	@sed -i -e "s/deactivate () {/deactivate () {\nfor c in \`docker ps -a | grep clamav | cut -f1 -d ' '\`; do $(DOCKER) rm -f -v \$$c; done/" ./venv/bin/activate

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-lib-src:: .transforms.test-lib-src

install-lib-src:: .transforms.install-lib-src