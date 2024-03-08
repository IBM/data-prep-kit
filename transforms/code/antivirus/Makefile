
# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
include ../../makefile.transforms

DOCKER_IMAGE_NAME=antivirus
DOCKER_IMAGE_VERSION=0.1

# Use default rule inherited from makefile.common
clean: .clean

# Use default rule inherited from makefile.common
test: .test

# Use default rule inherited from makefile.common
image: .image

# Use default rule inherited from makefile.common
build: .build

# Use default rule inherited from makefile.common
venv: .venv
	@docker build --target clamav-local -f Dockerfile -t clamav-local:latest .
	@echo "docker run -d --name clamav -p 3310:3310 clamav-local:latest" >> ./venv/bin/activate
	@sed -i -e "s/deactivate () {/deactivate () {\nfor c in \`docker ps -a | grep clamav | cut -f1 -d ' '\`; do docker rm -f -v \$$c; done/" ./venv/bin/activate

# Use default rule inherited from makefile.common
install_lib_source: .install_lib_source


