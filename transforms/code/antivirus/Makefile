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
	@echo -e "if [ ! -e /var/run/clamav/clamd.ctl ]; then\n    clamd --config-file=\$$(brew --prefix)/etc/clamav/clamd.conf\nfi" >> ./venv/bin/activate
	@sed -i '' -e "s#deactivate () {#deactivate () {\nps aux | grep \"[c]lamd --config-file=\$$(brew --prefix)/etc/clamav/clamd.conf\" | awk '{print \$$2}' | xargs kill\nrm -f /var/run/clamav/clamd.ctl#" ./venv/bin/activate
else
venv::	.transforms.venv
	@sudo mkdir -p /var/run/clamav && sudo chmod 777 /var/run/clamav
	@$(DOCKER) build --target clamav-local -f Dockerfile -t clamav-local:latest .
	@echo -e "$(DOCKER) run -d --name clamav -v /var/run/clamav:/var/run/clamav clamav-local:latest" >> ./venv/bin/activate
	@echo -e "retry=0\nuntil python -c \"import clamd; clamd.ClamdUnixSocket().ping()\" > /dev/null 2>&1; do\n    sleep 3\n    retry=\$$((retry+1))\n    if [ \$$retry -gt 20 ]; then break; fi\ndone" >> ./venv/bin/activate
	@sed -i -e "s/deactivate () {/deactivate () {\nfor c in \`$(DOCKER) ps -a | grep clamav | cut -f1 -d ' '\`; do $(DOCKER) rm -f -v \$$c; done/" ./venv/bin/activate
endif

build::	.transforms.build

test::	.transforms.test

clean:: .transforms.clean

image:: .transforms.image

test-src:: .transforms.test-src

test-image:: .transforms.test-image

