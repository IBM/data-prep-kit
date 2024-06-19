from typing import Dict

import kfp.dsl as dsl

from kfp import kubernetes


RUN_NAME = "KFP_RUN_NAME"

ONE_HOUR_SEC = 60 * 60
ONE_DAY_SEC = ONE_HOUR_SEC * 24
ONE_WEEK_SEC = ONE_DAY_SEC * 7


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

        kubernetes.use_field_path_as_env(
            task, env_name=RUN_NAME, field_path="metadata.annotations['pipelines.kubeflow.org/run_name']"
        )
        # Set cashing
        task.set_caching_options(enable_caching=cache_strategy)
        # image pull policy
        kubernetes.set_image_pull_policy(task, image_pull_policy)
        # Set the timeout for the task to one day (in seconds)
        kubernetes.set_timeout(task, seconds=timeout)

    @staticmethod
    def set_s3_env_vars_to_component(
        task: dsl.PipelineTask,
        secret: str = "",
        env2key: Dict[str, str] = {"s3-key": "S3_KEY", "s3-secret": "S3_SECRET", "s3-endpoint": "ENDPOINT"},
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
            for secret_key, _ in env2key.items():
                env_name = env2key.pop(secret_key)
                env_name = f"{prefix}_{env_name}"
                env2key[secret_key] = env_name
        # FIXME: see https://github.com/kubeflow/pipelines/issues/10914
        kubernetes.use_secret_as_env(task=task, secret_name="s3-secret", secret_key_to_env=env2key)
