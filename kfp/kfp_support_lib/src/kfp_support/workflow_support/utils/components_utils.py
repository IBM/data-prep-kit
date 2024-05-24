# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import kfp.dsl as dsl
from data_processing.utils import get_logger
from kubernetes import client as k8s_client


logger = get_logger(__name__)

ONE_HOUR_SEC = 60 * 60
ONE_DAY_SEC = ONE_HOUR_SEC * 24
ONE_WEEK_SEC = ONE_DAY_SEC * 7


class ComponentUtils:
    """
    Class containing methods supporting building pipelines
    """

    @staticmethod
    def add_settings_to_component(
        component: dsl.ContainerOp,
        timeout: int,
        image_pull_policy: str = "IfNotPresent",
        cache_strategy: str = "P0D",
    ) -> None:
        """
        Add settings to kfp component
        :param component: kfp component
        :param timeout: timeout to set to the component in seconds
        :param image_pull_policy: pull policy to set to the component
        :param cache_strategy: cache strategy
        """
        # Set cashing
        component.execution_options.caching_strategy.max_cache_staleness = cache_strategy
        # image pull policy
        component.container.set_image_pull_policy(image_pull_policy)
        # Set the timeout for the task
        component.set_timeout(timeout)

    @staticmethod
    def set_s3_env_vars_to_component(
        component: dsl.ContainerOp,
        secret: str,
        env2key: dict[str, str] = {
            "S3_KEY": "s3-key",
            "S3_SECRET": "s3-secret",  # pragma: allowlist secret
            "ENDPOINT": "s3-endpoint",
        },
        prefix: str = None,
    ) -> None:
        """
        Set S3 env variables to KFP component
        :param component: kfp component
        :param secret: secret name with the S3 credentials
        :param env2key: dict with mapping each env variable to a key in the secret
        :param prefix: prefix to add to env name
        """
        for env_name, secret_key in env2key.items():
            if prefix is not None:
                env_name = f"{prefix}_{env_name}"
            component = component.add_env_variable(
                k8s_client.V1EnvVar(
                    name=env_name,
                    value_from=k8s_client.V1EnvVarSource(
                        secret_key_ref=k8s_client.V1SecretKeySelector(name=secret, key=secret_key)
                    ),
                )
            )

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
        from kfp_support.workflow_support.utils import KFPUtils

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
