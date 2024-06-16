# Release Process

This document describes the release process for the following components:

- `kfp_support_lib` Python packages in `kfp_support_lib` directory.

- `kfp-data-processing` docker image built based on the Docker file in `kfp_ray_components` directory.

- kubeflow pipelines in `transform_workflows` directory. For example the one that is generated from `transform_workflows/universal/noop/noop_wf.py` file.

### 1. Update `.make.versions` file

The [.make.versions](../.make.versions) file specifies the target versions for the building components, as well as the 
desired versions for the dependencies. 
The KFP package build uses the following variables from the file:
- RELEASE_VERSION_SUFFIX - the common suffix for all building components
- DPK_LIB_KFP_VERSION - the version of `kfp_v1_workflow_support`
- DPK_LIB_KFP_VERSION_v2 - the version of `kfp_v2_workflow_support`
- DPK_LIB_KFP_SHARED - the version of `kfp_shared_workflow_support`
- KFP_DOCKER_VERSION - the docker image version of KFP components for KFPv1
- KFP_DOCKER_VERSION_v2 - the docker image version of KFP components for KFPv2

**Note:** The docker images are dependent on the libraries but use the python source code from the repository, so inorder 
to build docker images, the python modules (libraries) do not have to be deployed. 

### 2. Choose the supported KFP version.
The docker images and some `workflow_support` libraries depend on KFP version. In order to build images and libraries for  
KFP v2, run the following command:

```shell
export KFPv2=1
```

### 3. (Optional) Build the library

Run the `make -C shared_workflow_support build` command to build the shared library.
If you need a library for KFPv1
Run `make -C kfp_v1_workflow_support build`
For KFP v2 set the environment variable `KFPv2`, se above, and run `make -C kfp_v2_workflow_support build`

### 4. (Optional) Publish the library

Run `make -C shared_workflow_support publish`, and either `make -C kfp_v1_workflow_support publish` or 
`make -C kfp_v2_workflow_support publish`command to push the libraries to the TestPyPI repository.


### 5. Build the image

Run `make -C kfp_ray_components build` command to build the `kfp-data-processing` docker image, or `kfp-data-processing_v2`,
when `KFPv2==1`

### 5. Push the image

Run `make -C kfp_ray_components publish` command to push the docker image.
