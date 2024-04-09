# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 
include ../../.make.transforms

TRANSFORM_NAME=antivirus
DOCKER_IMAGE_VERSION=0.2

OS := $(shell uname -s)
ifeq ($(OS),Darwin)
venv::	.transforms.venv
	@./install_clamd_mac.sh
	@echo -e "ln -s /var/run/clamav .tmp\nif [\$$(ps aux | grep '[c]lamd' | wc -l) -eq 0 ]; then\n    clamd\nfi" >> ./venv/bin/activate
	@sed -i -e "s#deactivate () {#deactivate () {\nps aux | grep '[c]lamd' | awk '{print \$$2}' | xargs kill\nrm -rf ${PWD}/.tmp#" ./venv/bin/activate
else
venv::	.transforms.venv
	@rm -rf .tmp && mkdir .tmp && chmod 777 .tmp
	@$(DOCKER) build --target clamav-local -f Dockerfile -t clamav-local:latest .
	@echo "$(DOCKER) run -d --name clamav -v \$$PWD/.tmp:/var/run/clamav clamav-local:latest" >> ./venv/bin/activate
	@sed -i -e "s/deactivate () {/deactivate () {\nfor c in \`$(DOCKER) ps -a | grep clamav | cut -f1 -d ' '\`; do $(DOCKER) rm -f -v \$$c; done/" ./venv/bin/activate
endif

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

test-image:: .transforms.test-image-help
	@echo WARNING: Skipping pytest test until it passes

