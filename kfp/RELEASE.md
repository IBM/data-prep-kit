# Release Process

This document describes the release process for the following components:

- `fm_data_processing_kfp` Python package in `kfp_support_lib` directory.

- `kfp-data-processing` docker image built based on the Docker file in `kfp_ray_components` directory.

- kubeflow pipelines in `transform_workflows` directory. For example the one that is generated from `transform_workflows/universal/noop/noop_wf.py` file.

**Note:** The docker image is dependent on the library thus it is required to build a new docker image once a new library version is created.

### 1. Update `requirements.env` file

The [requirements.env](requirements.env) file specifies the target versions for the building components, as well as the desired versions for the dependencies.
All components names in the requirement file must be in uppercase, for example: `KFP_DOCKER_TAGNAME=0.0.8`.

Upon component version update, modify the [`requirements.env`](./requirements.env) file.

### 3. (Optional) Build the library

Run the `make -C kfp_support_lib build` command to build the `fm_data_processing_kfp` library if a new version should be created

### 4. (Optional) Publish the library

Run `make -C kfp_support_lib publish` command to push the library to the TestPyPI repository.

### 5. Build the image

Run `make -C kfp_ray_components build` command to build the `kfp-data-processing` docker image.


### 5. Push the image

Run `make -C kfp_ray_components publish` command to push the `kfp-data-processing` docker image.


To generate new kubeflow pipeline yaml files run `make -C transform_workflows build` command.