REPOROOT=../../..
# Use make help, to see the available rules
include $(REPOROOT)/transforms/.make.cicd.targets

#
# This is intended to be included across the Makefiles provided within
# a given transform's directory tree,  so must use compatible syntax.
#
################################################################################
# This defines the name of the transform and is used to match against
# expected files and is used to define the transform's image name. 
TRANSFORM_NAME=$(shell basename `pwd`)

################################################################################



run-cli-sample:
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).transform_python \
                --data_local_config "{ 'input_folder' : 'test-data/input', 'output_folder' : 'output'}"  \
                --lang_id_model_credential "ANY CREDENTIAL"	\
				--lang_id_model_kind "fasttext"	\
				--lang_id_model_url "facebook/fasttext-language-identification" \
				--lang_id_content_column_name "text"

run-cli-ray-sample: 
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).ray.transform \
                --run_locally True --data_local_config "{ 'input_folder' : 'test-data/input', 'output_folder' : 'output'}"  \
                --lang_id_model_credential "ANY CREDENTIAL"	\
				--lang_id_model_kind "fasttext"	\
				--lang_id_model_url "facebook/fasttext-language-identification" \
				--lang_id_content_column_name "text"


# CI: touched to trigger this transform test workflow after the
# pull_request_target -> pull_request change (see PR for details).
