### kfp_ray_components

This directory includes the source code, specifications, and Docker image file for the [kubeflow pipelines components](https://www.kubeflow.org/docs/components/pipelines/v1/concepts/component/). 
These components are utilized in Kubeflow pipelines to manage Ray clusters: creating them, executing jobs on them, and deleting them.

## Building the image

To build the component docker image first execute the following commands to
set the details of the docker registry as environment variables:

```bash
export DOCKER_SERVER=us.icr.io
export DOCKER_USERNAME=iamapikey
export DOCKER_EMAIL=iamapikey
export DOCKER_PASSWORD=<PASSWORD>
```

As the Docker image utilizes libraries from Python Artifactory, 
set the Python Artifactory details as environment variables by executing the following commands:

```bash
export ARTIFACTORY_USER=<artifactory-user>
export ARTIFACTORY_API_KEY=<artifactory-key>
```

Next, login to IBM cloud using the following commands:
```bash
ibmcloud login --sso
ibmcloud cr region-set us-south
ibmcloud cr login --client docker
```

Then build the image:

```bash
make build
```
