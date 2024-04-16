
# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.
include ../../.make.transforms

TRANSFORM_NAME=doc_quality
DOCKER_IMAGE_VERSION=0.1.0


venv:: .transforms.venv
	@# Help: Create the venv and then install kenlm 
	source venv/bin/activate;pip install https://github.com/kpu/kenlm/archive/master.zip

cc_net:
	@# Help: Clone the cc_net repo with the kenlm model 
	git clone https://github.com/facebookresearch/cc_net

lm_sp: cc_net
	@# Help: Build the model 
	rm -rf lm_sp
	mkdir lm_sp
	cd cc_net/;$(MAKE) lang=en dl_lm
	cp cc_net/data/lm_sp/* lm_sp

	
build::	.transforms.build

test:: lm_sp .transforms.test
	@# Help: Create the model and test the source and image

clean:: .transforms.clean
	rm -rf cc_net
	rm -rf lm_sp

image:: lm_sp .transforms.image
	@# Help: Create the model and build the image.

setup:: .transforms.setup


#test-image:: .transforms.test-image
# The following is temporary to get an inital PR in before the repo split.
test-image:: .transforms.test-image-help

test-src:: lm_sp .transforms.test-src
	@# Help: Create the model and test the source 

#publish:: .transforms.publish
publish:: 
	@echo Skipping publish until we have split the repos
