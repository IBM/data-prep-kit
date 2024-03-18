# Simplest Transform pipeline tutorial

In this example, we implement a pipeline to automate execution of the simple 
[noop transform](../../data-processing-lib/doc/simplest-transform-tutorial.md)
In this tutorial, we will show the following:

* How to write the `noop` transform automation pipeline, leveraging [KFP components](../kfp_ray_components/README.md).
* How to compile a pipeline and deploy it to KFP
* How to execute pipeline and view execution results

## Implementing pipeline

[Overall implementation](../transform_workflows/universal/noop/noop_wf.py) roughly contains 4 major sections:

* Components definition - defintion of the main steps of our pipeline
* Input parameters definition 
* Pipeline wiring - definition of the sequence of invocation (with parameter passing) of participating components
* Additional configuration

You should start by defining imports:

```python
import kfp.compiler as compiler
import kfp.components as comp
import kfp.dsl as dsl
from kfp_support.workflow_support.utils import (
    ONE_HOUR_SEC,
    ONE_WEEK_SEC,
    ComponentUtils,
)
from kubernetes import client as k8s_client
```

### Components definition

Our pipeline includes 4 steps - compute execution parameters, create Ray cluster, submit and watch Ray job, clean up 
Ray cluster. FOr each step we have to define a component that will execute them:

```python
    base_kfp_image = "us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-data-processing:0.0.1-test3"
    # execute parameters
    compute_exec_params_op = comp.func_to_container_op(
        func=ComponentUtils.default_compute_execution_params, base_image=base_kfp_image
    )
    # create Ray cluster
    create_ray_op = comp.load_component_from_file("../../../kfp_ray_components/createRayComponent.yaml")
    # execute job
    execute_ray_jobs_op = comp.load_component_from_file("../../../kfp_ray_components/executeRayJobComponent.yaml")
    # clean up Ray
    cleanup_ray_op = comp.load_component_from_file("../../../kfp_ray_components/cleanupRayComponent.yaml")
```
Note that here we are using components described in this [document](../kfp_ray_components/README.md) for `create_ray_op`, 
`execute_ray_jobs_op` and `cleanup_ray_op`  while `compute_exec_params_op` component is build inline, because it might
differ significantly. For "simple" components we are using [defauld implementation](../kfp_support_lib/src/kfp_support/workflow_support/utils/workflow_utils.py),
while, for example for ededup, we are using a very [specialized one](../transform_workflows/universal/ededup/src/ededup_compute_execution_params.py).

### Input parameters definition

The pipeline defines all of the parameters required for the execution:

```python
    ray_name: str = "noop-kfp-ray",  # name of Ray cluster
    ray_head_options: str = '{"cpu": 1, "memory": 2, "image": "us.icr.io/cil15-shared-registry/preprocessing-pipelines/noop:guftest",\
             "image_pull_secret": "prod-all-icr-io"}',
    ray_worker_options: str = '{"replicas": 2, "max_replicas": 2, "min_replicas": 2, "cpu": 2, "memory": 4, "image_pull_secret": "prod-all-icr-io",\
            "image": "us.icr.io/cil15-shared-registry/preprocessing-pipelines/noop:guftest"}',
    server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    additional_params: str = '{"wait_interval": 2, "wait_cluster_ready_tmout": 400, "wait_cluster_up_tmout": 300, "wait_job_ready_tmout": 400, "wait_print_tmout": 30, "http_retries": 5}',
    noop_sleep_sec: int = 10,
    lh_config: str = "None",
    max_files: int = -1,
    actor_options: str = "{'num_cpus': 0.8}",
    pipeline_id: str = "pipeline_id",
    s3_access_secret: str = "cos-access",
    s3_config: str = "{'input_folder': 'cos-optimal-llm-pile/doc_annotation_test/input/noop_small/', 'output_folder': 'cos-optimal-llm-pile/doc_annotation_test/output_noop_guf/'}",
```

The parameters used here are as follows:

* ray_name - name of the Ray cluster
* ray_head_options - head node options, containing the following:
  * cpu - number of cpus
  * memory - memory
  * image - image to use
  * image_pull_secret - image pull secret
* ray_worker_options - worker node options (we here are using only 1 worker pool), containing the following:
  * replicas - number of replicas to create
  * max_replicas - max number of replicas
  * min_replicas - min number of replicas
  * cpu - number of cpus
  * memory - memory
  * image - image to use
  * image_pull_secret - image pull secret
* server_url - url of the KubeRay API server
* additional_params - additional (support) parameters, containing the following:
  * wait_interval - wait interval for API server, sec
  * wait_cluster_ready_tmout - time to wait for cluster ready, sec
  * wait_cluster_up_tmout - time to wait for cluster up, sec
  * wait_job_ready_tmout - time to wait for job ready, sec
  * wait_print_tmout - time between prints, sec
  * http_retries - httpt retries for API server calls
* lh_config - lake house configuration
* s3_config - s3 configuration
* s3_access_secret - s3 access secret
* max_files - max files to process
* actor_options - actor options - see [here](../../data-processing-lib/doc/launcher-options.md)
* pipeline_id - pipeline id
* noop_sleep_sec - noop sleep time

**Note** that here we are specifying initial values for all parameters that will be propagated to the worklow UI
(see below)

### Pipeline wiring

Now that all components and input parameters are defined, we can implement pipeline wiring defining sequence of 
component execution and parameters submittied to every component. 

```python
    # create clean_up task
    clean_up_task = cleanup_ray_op(ray_name=ray_name, run_id=RUN_ID, server_url=server_url)
    ComponentUtils.add_settings_to_component(clean_up_task, 60)
    # pipeline definition
    with dsl.ExitHandler(clean_up_task):
        # compute execution params
        compute_exec_params = compute_exec_params_op(
            worker_options=ray_worker_options,
            actor_options=actor_options,
        )
        ComponentUtils.add_settings_to_component(compute_exec_params, ONE_HOUR_SEC * 2)
        # start Ray cluster
        ray_cluster = create_ray_op(
            ray_name=ray_name,
            run_id=RUN_ID,
            ray_head_options=ray_head_options,
            ray_worker_options=ray_worker_options,
            server_url=server_url,
            additional_params=additional_params,
        )
        ComponentUtils.add_settings_to_component(ray_cluster, ONE_HOUR_SEC * 2)
        ray_cluster.after(compute_exec_params)
        # Execute job
        execute_job = execute_ray_jobs_op(
            ray_name=ray_name,
            run_id=RUN_ID,
            additional_params=additional_params,
            # note that the parameters below are specific for NOOP transform
            exec_params={
                "s3_config": s3_config,
                "noop_sleep_sec": noop_sleep_sec,
                "lh_config": lh_config,
                "max_files": max_files,
                "num_workers": compute_exec_params.output,
                "worker_options": actor_options,
                "pipeline_id": pipeline_id,
            },
            exec_script_name=EXEC_SCRIPT_NAME,
            server_url=server_url,
        )
```

Here we first create `cleanup_task` and the use it as an 
[exit handler](https://www.kubeflow.org/docs/components/pipelines/v2/pipelines/control-flow/) so that it will be invoked
in case any of the step will fail.
Then we create each individual component passing it required parameters and specify execution sequence, for example
(`ray_cluster.after(compute_exec_params)`).

### Additional configuration

The final thing that we need to do is set some pipeline global configuration:

```python
    # set image pull secrets
    dsl.get_pipeline_conf().set_image_pull_secrets([k8s_client.V1ObjectReference(name="prod-all-icr-io")])
    # Configure the pipeline level to one week (in seconds)
    dsl.get_pipeline_conf().set_timeout(ONE_WEEK_SEC)
```

## Compiling pipeline and deploying it to KFP

To compile pipeline execute this [file](../transform_workflows/universal/noop/noop_wf.py), which produces file `noop_wf.yaml`
in the same directory. Now create kind cluster cluster with all required software installed using the following command: 

````shell
 make setup-kind-cluster
````
**Note** that this command has to run from the project root directory

Once the cluster is up, go to `localhost:8080/kfp/`, which will bring up KFP UI, see below:

![KFP UI](kfp_ui.png)

Click on the `Upload pipeline` link and follow instructions on the screen to upload your file (`noop_wf.yaml`) and
name pipeline noop. Once this is done, you should see something as follows:

![noop pipeline](noop_pipeline.png)

## Executing pipeline and watching execution results

Before we can run the pipeline we need to create required secrets (one for image loading in case of secured 
registry and one for S3 access). As KFP is deployed in `kubeflow` namespace, workflow execution will happen
there as well, which means that secrets have to be created there as well.

Once this is done we can execute the workflow. 

## Clean up cluster

Finally you can delete kind cluster running the following command:

```Shell
make delete-kind-cluster
```