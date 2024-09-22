# KFP components
# KFP components

All data processing pipelines have the same `shape`. They all compute execution parameters, create Ray cluster,
execute Ray job and then delete the cluster. With the exception of computing execution parameters all of the steps,
although receiving different parameters are identical.

To simplify implementation of the data processing KFP pipelines, this directory provides several components.

As defined by [KFP documentation](https://www.kubeflow.org/docs/components/pipelines/v1/sdk/component-development/)
````
A pipeline component is a self-contained set of code that performs one step in a workflow. 
````

The first step in creation of components its implementation. The framework automation includes the following 3 components:

* [Create Ray cluster](src/create_ray_cluster.py) is responsible for creation of the Ray cluster. Its implementation is 
  based on the [RayRemoteJobs class](../kfp_support_lib/src/kfp_support/workflow_support/README.md)
* [execute Ray job](src/execute_ray_job.py) is responsible for submission of the Ray job, watching its execution,
  periodically printing job execution log and completing, once the job is completed. Its implementation is
  based on the [RayRemoteJobs class](../kfp_support_lib/src/kfp_support/workflow_support/README.md)
* [clean up Ray cluster](src/delete_ray_cluster.py) is responsible for deletion of the Ray cluster, thus freeing
  up cluster resources. Its implementation is based on the 
  [RayRemoteJobs class](../kfp_support_lib/src/kfp_support/workflow_support/README.md)

Once the components are implemented we also implement their interfaces as a component specification which defines:

* The component’s inputs and outputs.
* The container image that your component’s code runs in, the command to use to run your component’s code, and the 
command-line arguments to pass to your component’s code99.
* The component’s metadata, such as the name and description.

Components specifications are provided here:

* [Create Ray cluster Component](createRayClusterComponent.yaml)
* [execute Ray job component](executeRayJobComponent.yaml)
* [clean up Ray cluster component](deleteRayClusterComponent.yaml)

## Building the docker image

To build the component docker image first execute the following commands to
set the details of the docker registry as environment variables:

```bash
export DOCKER_SERVER=<> # for example us.icr.io 
export DOCKER_USERNAME=iamapikey
export DOCKER_EMAIL=iamapikey
export DOCKER_PASSWORD=<PASSWORD>
```

Then build the image:

```bash
make build
make publish
```
