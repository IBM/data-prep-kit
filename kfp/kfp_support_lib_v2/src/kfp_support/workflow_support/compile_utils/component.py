import kfp.dsl as dsl
from kfp import kubernetes
from typing import Dict

RUN_NAME = "KFP_RUN_NAME"

class ComponentUtils:
    """
    Class containing methods supporting building pipelines
    """

    @staticmethod
    def add_settings_to_component(
            task: dsl.PipelineTask,
            timeout: int,
            image_pull_policy: str = "IfNotPresent",
            cache_strategy: bool = False,
    ) -> None:
        """
        Add settings to kfp task
        :param task: kfp task
        :param timeout: timeout to set to the component in seconds
        :param image_pull_policy: pull policy to set to the component
        :param cache_strategy: cache strategy
        """

        kubernetes.use_field_path_as_env(task, env_name=RUN_NAME,
                                         field_path="metadata.annotations['pipelines.kubeflow.org/run_name']")
        # Set cashing
        task.set_caching_options(enable_caching=cache_strategy)
        # image pull policy
        kubernetes.set_image_pull_policy(task, image_pull_policy)
        # Set the timeout for the task to one day (in seconds)
        kubernetes.set_timeout(task, seconds=timeout)

    @staticmethod
    def set_s3_env_vars_to_component(
            task: dsl.PipelineTask,
            secret: str = '',
            env2key: Dict[str, str] = {'s3-key': 'S3_KEY', 's3-secret': 'S3_SECRET', 's3-endpoint': 'ENDPOINT'},
            prefix: str = None,
    ) -> None:
        """
        Set S3 env variables to KFP component
        :param task: kfp task
        :param secret: secret name with the S3 credentials
        :param env2key: dict with mapping each env variable to a key in the secret
        :param prefix: prefix to add to env name
        """

        if prefix is not None:
            for env_name, _ in env2key.items():
                env2key[prefix + "_" + env_name] = env2key.pop(env_name)
        kubernetes.use_secret_as_env(task=task, secret_name='s3-secret', secret_key_to_env=env2key)

    @staticmethod
    def default_compute_execution_params(
            worker_options: str,  # ray worker configuration
            actor_options: str,  # cpus per actor
    ) -> str:
        """
        This is the most simplistic transform execution parameters computation
        :param worker_options: configuration of ray workers
        :param actor_options: actor request requirements
        :return: number of actors
        """
        import sys

        from data_processing.utils import GB, get_logger
        from kfp_support.workflow_support.runtime_utils import KFPUtils

        logger = get_logger(__name__)

        # convert input
        w_options = KFPUtils.load_from_json(worker_options.replace("'", '"'))
        a_options = KFPUtils.load_from_json(actor_options.replace("'", '"'))
        # Compute available cluster resources
        cluster_cpu = w_options["replicas"] * w_options["cpu"]
        cluster_mem = w_options["replicas"] * w_options["memory"]
        cluster_gpu = w_options["replicas"] * w_options.get("gpu", 0.0)
        logger.info(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_mem}, GPUs {cluster_gpu}")
        # compute number of actors
        n_actors_cpu = int(cluster_cpu * 0.85 / a_options.get("num_cpus", 0.5))
        n_actors_memory = int(cluster_mem * 0.85 / (a_options.get("memory", GB) / GB))
        n_actors = min(n_actors_cpu, n_actors_memory)
        # Check if we need gpu calculations as well
        actor_gpu = a_options.get("num_gpus", 0)
        if actor_gpu > 0:
            n_actors_gpu = int(cluster_gpu / actor_gpu)
            n_actors = min(n_actors, n_actors_gpu)
        logger.info(f"Number of actors - {n_actors}")
        if n_actors < 1:
            logger.warning(
                f"Not enough cpu/gpu/memory to run transform, "
                f"required cpu {a_options.get('num_cpus', .5)}, available {cluster_cpu}, "
                f"required memory {a_options.get('memory', 1)}, available {cluster_mem}, "
                f"required cpu {actor_gpu}, available {cluster_gpu}"
            )
            sys.exit(1)

        return str(n_actors)