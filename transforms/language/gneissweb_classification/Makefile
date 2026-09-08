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

TRANSFORM_PYTHON_SRC="-m dpk_$(TRANSFORM_NAME).runtime"
TRANSFORM_RAY_SRC="-m dpk_$(TRANSFORM_NAME).ray.runtime"


run-cli-sample:
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).runtime \
                --data_local_config "{ 'input_folder' : 'test-data/input', 'output_folder' : 'output'}"  \
				--gcls_model_file_name "['fasttext_medical.bin']"	\
				--gcls_model_url "['ibm-granite/GneissWeb.Med_classifier']"\
				--gcls_output_label_column_name "['label']"	\
				--gcls_content_column_name "text"

run-cli-ray-sample: 
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).ray.runtime \
                --run_locally True --data_local_config "{ 'input_folder' : 'test-data/input', 'output_folder' : 'output'}"  \
				--gcls_model_file_name "['fasttext_medical.bin']"	\
				--gcls_model_url "['ibm-granite/GneissWeb.Med_classifier']"\
				--gcls_output_label_column_name "['label']"	\
				--gcls_content_column_name "text"

run-math-s3:
ifndef OUTPUT_FOLDER
	$(info >>>> OUTPUT_FOLDER is not defined. Must specify a valid output folder in current bucket)
	$(info >>>> EXAMPLE- make OUTPUT_FOLDER=test_$(shell date +%m%d%H) run-math-s3)
	$(error OUTPUT_FOLDER is required)
endif
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).runtime \
		--gcls_n_processes 1 \
		--gcls_model_file_name "[ 'math.bin' ]"	\
		--gcls_model_url "[ 's3://bucket-wikimedia-1/math.bin' ]"\
		--gcls_output_label_column_name "[ 'math_label' ]"	\
		--gcls_output_score_column_name "[ 'math_score' ]"	\
        --gcls_content_column_name "contents" \
        --data_s3_config "{ 'input_folder': 'bucket-wikimedia-1/ededup', 'output_folder': 'bucket-wikimedia-1/$(OUTPUT_FOLDER)' }" \
        --data_files_to_use "[ '.parquet' ]" \
        --data_checkpointing False

run-math-ray-s3:
ifndef OUTPUT_FOLDER
	$(info >>>> OUTPUT_FOLDER is not defined. Must specify a valid output folder in current bucket)
	$(info >>>> EXAMPLE- make OUTPUT_FOLDER=test_$(shell date +%m%d%H) run-math-ray-s3)
	$(error OUTPUT_FOLDER is required)
endif
	#make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).ray.runtime \
        --run_locally True \
		--gcls_model_file_name "[ 'math.bin' ]"	\
		--gcls_model_url "[ 's3://bucket-wikimedia-1/math.bin' ]"\
		--gcls_output_label_column_name "[ 'math_label' ]"	\
		--gcls_output_score_column_name "[ 'math_score' ]"	\
        --gcls_content_column_name "contents" \
        --data_s3_config "{ 'input_folder': 'bucket-wikimedia-1/ededup', 'output_folder': 'bucket-wikimedia-1/$(OUTPUT_FOLDER)' }" \
        --data_files_to_use "[ '.parquet' ]" \
        --data_checkpointing False \
        --runtime_num_workers 5 \
        --runtime_worker_options "{ 'num_cpus': 0.8, 'max_restarts': 0, 'max_task_retries': 0 }"

docker-math-ray-s3:
# Fails on MacOs
ifndef OUTPUT_FOLDER
	$(info >>>> OUTPUT_FOLDER is not defined. Must specify a valid output folder in current bucket)
	$(info >>>> EXAMPLE- make OUTPUT_FOLDER=test_$(shell date +%m%d%H) docker-math-ray-s3)
	$(error OUTPUT_FOLDER is required)
endif
	DOCKER_PLATFORM=linux/arm64 make image-ray
	docker run --env-file .env -dt -v ~/tmp:/tmp --shm-size=8gb --memory=8g gneissweb_classification-ray:latest $(PYTHON) -m dpk_$(TRANSFORM_NAME).ray.runtime \
		--run_locally True \
		--gcls_model_file_name "[ 'math.bin' ]"	\
		--gcls_model_url "[ 's3://bucket-wikimedia-1/math.bin' ]"\
		--gcls_output_label_column_name "[ 'math_label' ]"	\
		--gcls_output_score_column_name "[ 'math_score' ]"	\
		--gcls_content_column_name "contents" \
		--data_s3_config "{ 'input_folder': 'bucket-wikimedia-1/ededup', 'output_folder': 'bucket-wikimedia-1/$(OUTPUT_FOLDER)' }" \
		--data_files_to_use "[ '.parquet' ]" \
		--data_checkpointing False \
		--runtime_num_workers 1 \
		--runtime_worker_options "{ 'num_cpus': 1, 'max_restarts': 0, 'max_task_retries': 0 }"

docker-math-s3:
ifndef OUTPUT_FOLDER
	$(info >>>> OUTPUT_FOLDER is not defined. Must specify a valid output folder in current bucket)
	$(info >>>> EXAMPLE- make OUTPUT_FOLDER=test_$(shell date +%m%d%H) docker-math-s3)
	$(error OUTPUT_FOLDER is required)
endif
	DOCKER_PLATFORM=linux/arm64 make image-python
	docker run --env-file .env -dt gneissweb_classification-python:latest $(PYTHON) -m dpk_$(TRANSFORM_NAME).runtime \
		--gcls_model_file_name "[ 'math.bin' ]"	\
		--gcls_model_url "[ 's3://bucket-wikimedia-1/math.bin' ]"\
		--gcls_output_label_column_name "[ 'math_label' ]"	\
		--gcls_output_score_column_name "[ 'math_score' ]"	\
		--gcls_content_column_name "contents" \
		--data_s3_config "{ 'input_folder': 'bucket-wikimedia-1/ededup', 'output_folder': 'bucket-wikimedia-1/$(OUTPUT_FOLDER)' }" \
		--data_files_to_use "[ '.parquet' ]" \
		--data_checkpointing False


deploy-kuberay:
	$(REPOROOT)/scripts/k8s-setup/apply_secrets.sh
	kind load docker-image quay.io/dataprep1/data-prep-kit/gneissweb_classification-ray:latest --name kind-cluster
	#cd $(REPOROOT)/scripts/k8s-setup/tools && K8S_SETUP_SCRIPTS=../ KUBERAY_APISERVER=1.1.0 KUBERAY_OPERATOR=1.0.0 ./install_kuberay.sh cleanup
	cd $(REPOROOT)/scripts/k8s-setup/tools && K8S_SETUP_SCRIPTS=../ KUBERAY_APISERVER=1.1.0 KUBERAY_OPERATOR=1.0.0 ./install_kuberay.sh deploy

# CI: touched to trigger this transform test workflow after the
# pull_request_target -> pull_request change (see PR for details).
