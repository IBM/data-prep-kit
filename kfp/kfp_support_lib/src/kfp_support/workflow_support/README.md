# Workflow Utils

This library provides 3 main classes:
* KFPUtils - helper utilities for KFP implementations
* PipelinesUtils - helper class for pipeline management based on KFP client
* RayRemoteJobs - class supporting Ray remote jobs

## KFPUtils

This class contains a collection of functions useful for KFP pipelines implementation, which include: 
* credentials - get S3 credentials from the environment
* get_namespace - get the name of the kubernetes namespace we are running in
* runtime_name - generates unique runtime name
* dict_to_req - convert dictionary of request parameters to a proper formatted JSON string
* load_from_json - convert json string to dictionary and exit with error if conversion fails

## PipelinesUtils

This class provides some higher level functionality based on the capabilities of the python KFP client, including" 
* get_experiment_by_name obtains KFP experiment object based on its name
* get_pipeline_by_name obtains KFP pipeline object based on its name
* start_pipeline start a pipeline represented by pipeline object in experiment represented by experiment object and a 
dictionary of parameters. It returns kfp run ID
* wait_pipeline_completion - waits for the completion of the pipeline run with the given ID

## RayRemoteJobs

At the moment there is no "standard" approach for KubeRay remote APIs. There are several options available, 
including [codeflareSDK](https://github.com/project-codeflare/codeflare-sdk/tree/1fe04c3022d98bc286454dea2cd1e31709961bd2/src/codeflare_sdk)
[KubeRay Python Apis](https://github.com/ray-project/kuberay/tree/master/clients/python-client) and 
[KubeRay API server APIs](https://github.com/ray-project/kuberay/tree/master/clients/python-apiserver-client) to name a few.
We are using here KubeRay API server APIs, but in order to simplify possible transition to another APIs. this class 
implements 4 high-level methods, that allow to hide the specifics of the particular APIs. This methods are:
* create_ray_cluster - creates Ray cluster.
* delete_ray_cluster - deletes Ray cluster.
* submit_job - submits Ray job to the cluster
* follow_execution - watching job execution to completion, periodically printing out the job log
These basic methods can be used as a foundation of any KFP pipeline implementation

## ComponentUtils

This class provides some methods to simplify building pipelines:
* add_settings_to_component - adds settings to component, including timeout, image_pull_policy and cache strategy
* set_cos_env_vars_to_component - sets environment variables to support S3
* default_compute_execution_params - default implementation of compute execution parameters (based on CPU, GPU and memory requirements)