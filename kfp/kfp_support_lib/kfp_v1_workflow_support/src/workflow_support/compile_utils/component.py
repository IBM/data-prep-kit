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

import json
import os

import kfp.dsl as dsl
from data_processing.utils import get_logger
from kubernetes import client as k8s_client
from kubernetes.client import (
    V1Affinity,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1Toleration,
)


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

        def _add_tolerations() -> None:
            """
            Adds Tolerations if specified
            """
            try:
                tolerations = os.getenv("KFP_TOLERATIONS", "")
                if tolerations != "":
                    print(f"Note: Applying Tolerations {tolerations} to kfp and ray pods")

                    # Add Tolerations as env var so it can be used when creating the ray cluster
                    component.add_env_variable(k8s_client.V1EnvVar(name="KFP_TOLERATIONS", value=tolerations))

                    tolerations = json.loads(tolerations)
                    for toleration in tolerations:
                        component.add_toleration(
                            V1Toleration(
                                key=toleration["key"],
                                operator=toleration["operator"],
                                value=toleration["value"],
                                effect=toleration["effect"],
                            )
                        )
            except Exception as e:
                logger.warning(f"Exception while handling tolerations {e}")

        def _add_node_selector() -> None:
            """ "
            Adds mode selector if specified
            """
            try:
                node_selector = os.getenv("KFP_NODE_SELECTOR", "")
                if node_selector != "":
                    print(f"Note: Applying node_selector {node_selector} to kubeflow pipelines pods")
                    node_selector = json.loads(node_selector)
                    component.add_node_selector_constraint(node_selector["label_key"], node_selector["label_value"])
            except Exception as e:
                logger.warning(f"Exception while handling node_selector {e}")

        # Set cashing
        component.execution_options.caching_strategy.max_cache_staleness = cache_strategy
        # image pull policy
        component.container.set_image_pull_policy(image_pull_policy)
        # Set the timeout for the task
        component.set_timeout(timeout)
        # Add tolerations
        _add_tolerations()
        # Add affinity
        _add_node_selector()

    @staticmethod
    def set_s3_env_vars_to_component(
        component: dsl.ContainerOp,
        secret: str,
        env2key: dict[str, str] = {"S3_KEY": "s3-key", "S3_SECRET": "s3-secret", "ENDPOINT": "s3-endpoint"},
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
    def add_cm_volume_to_com_function(component: dsl.ContainerOp, cmName: str, mountPoint: str, optional=False):
        last_folder = os.path.basename(os.path.normpath(mountPoint))
        vol = k8s_client.V1Volume(
            name=last_folder,
            config_map=k8s_client.V1ConfigMapVolumeSource(name=cmName, optional=optional),
        )
        component.add_pvolumes({mountPoint: vol})

    @staticmethod
    def add_secret_volume_to_com_function(
        component: dsl.ContainerOp, secretName: str, mountPoint: str, optional=False
    ):
        last_folder = os.path.basename(os.path.normpath(mountPoint))
        vol = k8s_client.V1Volume(
            name=last_folder,
            secret=k8s_client.V1SecretVolumeSource(secret_name=secretName, optional=optional),
        )
        component.add_pvolumes({mountPoint: vol})
