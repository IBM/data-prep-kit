
# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include the common rules.
# Use "make help" to see them. 
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below.
include ../../.make.transforms

DOCKER_IMAGE_NAME=doc_quality
DOCKER_IMAGE_VERSION=0.1.0


venv::	.transforms.venv
	source venv/bin/activate;pip install https://github.com/kpu/kenlm/archive/master.zip

cc_net:
	git clone https://github.com/facebookresearch/cc_net

model-en: cc_net lm_sp
	cd cc_net/;$(MAKE) lang=en dl_lm
	cp cc_net/data/lm_sp/en* lm_sp

lm_sp:
	mkdir lm_sp
	
build::	.transforms.build

test::	model-en .transforms.test

clean:: .transforms.clean
	rm -rf cc_net
	rm -rf lm_sp

image:: .transforms.image

test-lib-src:: .transforms.test-lib-src

install-lib-src:: .transforms.install-lib-src


